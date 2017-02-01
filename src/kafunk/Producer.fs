namespace Kafunk

open FSharp.Control
open Kafunk
open System
open System.Text

/// A producer message.
type ProducerMessage =
  struct
    /// The message payload.
    val value : Binary.Segment
    /// The optional message key.
    val key : Binary.Segment
    new (value:Binary.Segment, key:Binary.Segment) = 
      { value = value ; key = key }
  end
    with

      /// Creates a producer message.
      static member ofBytes (value:Binary.Segment, ?key) =
        ProducerMessage(value, defaultArg key Binary.empty)

      /// Creates a producer message.
      static member ofBytes (value:byte[], ?key) =
        let keyBuf = defaultArg (key |> Option.map Binary.ofArray) Binary.empty
        ProducerMessage(Binary.ofArray value, keyBuf)

      /// Creates a producer message.
      static member ofString (value:string, ?key:string) =
        let keyBuf = defaultArg (key |> Option.map (Encoding.UTF8.GetBytes >> Binary.ofArray)) Binary.empty
        ProducerMessage(Binary.ofArray (Encoding.UTF8.GetBytes value), keyBuf)

/// A producer response.
type ProducerResult =
  struct
    /// The offsets produced.
    val offsets : (Partition * Offset)[]
    new (os) = { offsets = os }
  end

/// The number of partitions of a topic.
type PartitionCount = int

/// A partition function.
type Partitioner = TopicName * Partition[] * ProducerMessage -> Partition

/// Partition functions.
[<Compile(Module)>]
module Partitioner =

  open System.Threading

  let private ensurePartitions (ps:Partition[]) =
    if isNull ps then nullArg "ps"
    if ps.Length = 0 then invalidArg "ps" "must have partitions"
    
  /// Creates a partition function.
  let create (f:TopicName * Partition[] * ProducerMessage -> Partition) : Partitioner = 
    f

  /// Computes the partition.
  let partition (p:Partitioner) (t:TopicName) (ps:Partition[]) (m:ProducerMessage) =
    p (t,ps,m)

  /// Constantly returns the same partition.
  let konst (p:Partition) : Partitioner =
    create <| konst p

  /// Round-robin partition assignment.
  let roundRobin : Partitioner =
    let mutable i = 0
    create <| fun (_,ps,_) -> 
      ensurePartitions ps
      let i' = Interlocked.Increment &i
      if i' < 0 then
        Interlocked.Exchange (&i, 0) |> ignore
        ps.[0]
      else 
        ps.[i' % ps.Length]

  /// Computes the hash-code of the routing key to get the topic partition.
  let hashKey (h:Binary.Segment -> int) : Partitioner =
    create <| fun (_,ps,pm) -> 
      ensurePartitions ps
      ps.[abs (h pm.key) % ps.Length]

  /// Random partition assignment.
  let rand (seed:int option) : Partitioner =
    let rng = 
      match seed with 
      | Some s -> new Random(s)
      | None -> new Random()
    let inline next ps = lock rng (fun () -> rng.Next (0, ps))
    create <| fun (_,ps,_) ->
      ensurePartitions ps
      let i = next ps.Length
      ps.[i]
  
  /// CRC32 of the message key.
  let crc32Key : Partitioner =
    hashKey (fun key -> int (Crc.crc32 key.Array key.Offset key.Count))


/// Producer configuration.
type ProducerConfig = {

  /// The topic to produce to.
  topic : TopicName

  /// A partition function which given a topic name, cluster topic metadata and the message payload, returns the partition
  /// which the message should be written to.
  partitioner : Partitioner

  /// The acks required.
  /// Default: Local
  requiredAcks : RequiredAcks

  /// The compression method to use.
  /// Default: None
  compression : byte

  /// The maximum time to wait for acknowledgement.
  /// Default: 0
  timeout : Timeout

  /// The per-broker buffer size.
  /// When the buffer reaches capacity, backpressure is exerted on incoming produce requests.
  /// Default: 100
  bufferSize : int

  /// The maximum size of a batch of produce requests.
  /// Default: 100
  batchSize : int

  /// The maximum time to wait for a produce request btch to reach capacity.
  /// Default: 1
  batchLinger : int

} with

  static member internal messageVersion (connVersion:Version) = 
    Versions.produceReqMessage (Versions.byKey connVersion ApiKey.Produce)

  /// Creates a producer configuration.
  static member create (topic:TopicName, partition:Partitioner, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout, ?bufferSize:int, ?batchSize, ?batchLinger) =
    {
      topic = topic
      requiredAcks = defaultArg requiredAcks RequiredAcks.Local
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout 0
      partitioner = partition
      bufferSize = defaultArg bufferSize 100
      batchSize = defaultArg batchSize 100
      batchLinger = defaultArg batchLinger 10
    }



/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
type Producer = private {
  conn : KafkaConn
  config : ProducerConfig
  messageVersion : ApiVersion
  state : Resource<ProducerState>
  produceBatch : (Partition[] -> Partition * ProducerMessage[]) -> Async<ProducerResult>
}

/// Producer state corresponding to the state of a cluster.
and private ProducerState = {
  partitionBrokers : Map<Partition, Chan>
  partitions : Partition[]
  brokerQueues : Map<EndPoint, (((ProducerMessage[] * Partition) * IVar<Result<ProducerResult, ChanError list>>)[] -> Async<unit>)>
}


/// High-level producer API.
[<Compile(Module)>]
module Producer =

  open System.Threading
  open System.Threading.Tasks

  let private Log = Log.create "Kafunk.Producer"

  /// Sends a batch of messages to a broker, and replies to the respective reply channels.
  let private sendBatch (cfg:ProducerConfig) (messageVer) (ch:Chan) (batch:((ProducerMessage[] * Partition) * IVar<Result<ProducerResult, ChanError list>>)[]) = async {
    
    let ms,reps = 
      batch
      |> Seq.map (fun (((ms),p),rep) -> (p,ms),(p,rep))
      |> Seq.toArray
      |> Array.unzip

    let pms = 
      ms
      |> Seq.groupBy fst
      |> Seq.map (fun (p,pms) ->
        let ms = 
          pms 
          |> Seq.collect (fun (_,pms) -> pms |> Seq.map (fun pm -> Message.create pm.value pm.key None))
          |> MessageSet.ofMessages
          |> Compression.compress messageVer cfg.compression
        p,ms)
      |> Seq.toArray

    let req = ProduceRequest.ofMessageSetTopics [| cfg.topic, pms |] cfg.requiredAcks cfg.timeout
    let! res = Chan.send ch (RequestMessage.Produce req) |> Async.map (Result.map ResponseMessage.toProduce)
    match res with
    | Success res ->
      
      let oks,retryErrors,fatalErrors =
        res.topics
        |> Seq.collect (fun (_t,os) ->
          os
          |> Seq.map (fun (p,ec,o) ->
            match ec with
            | ErrorCode.NoError -> Choice1Of3 (p,o)

            // 404
            | ErrorCode.InvalidMessage | ErrorCode.MessageSizeTooLarge
            | ErrorCode.InvalidTopicCode | ErrorCode.InvalidRequiredAcksCode -> Choice3Of3 (p,o,ec)

            // timeout  (retry)
            | ErrorCode.RequestTimedOut | ErrorCode.LeaderNotAvailable -> Choice2Of3 (p,o)

            // topology change (retry)
            | ErrorCode.UnknownTopicOrPartition | ErrorCode.NotLeaderForPartition -> Choice2Of3 (p,o)
              
            // unknown
            | _ -> Choice3Of3 (p,o,ec)))
        |> Seq.partitionChoices3
      
      // TODO: error only by partition
      if fatalErrors.Length > 0 then
        let msg = sprintf "produce_fatal_errors|errors=%A" fatalErrors
        Log.error "%s" msg
        let ex = exn(msg)
        for (_,rep) in reps do
          IVar.error ex rep
      elif retryErrors.Length > 0 then
        Log.error "produce_transient_errors|errors=%A" retryErrors
        let res = Failure [] // TODO: specific error
        for (_,rep) in reps do
          IVar.put res rep
      elif oks.Length > 0 then
        let res = ProducerResult(oks)
        let res = Success res
        for (_,rep) in reps do
          IVar.put res rep
      else
        failwith "invalid state"

      return ()

    | Failure err ->
      let err = Failure err
      for (_,rep) in reps do
        IVar.put err rep 
      return () }

  /// Fetches cluster state and initializes a per-broker produce buffer.
  let private getState (conn:KafkaConn) (cfg:ProducerConfig) (ct:CancellationToken) = async {
    
    Log.info "fetching_topic_metadata|topic=%s" cfg.topic
    let! state = conn.GetMetadataState [|cfg.topic|]

    let partitions = 
      Routes.topicPartitions state.routes |> Map.find cfg.topic

    let! _,brokerByPartition =
      ((Map.empty,Map.empty), AsyncSeq.ofSeq partitions)
      ||> AsyncSeq.foldAsync (fun (brokerByEndPoint,brokerByPartition) p -> async {
        match Routes.tryFindHostForTopic state.routes (cfg.topic,p) with
        | Some ep -> 
          match ConnState.tryFindChanByEndPoint ep state with
          | Some ch ->
            return (Map.add ep ch brokerByEndPoint, Map.add p ch brokerByPartition)
          | None -> 
            match brokerByEndPoint |> Map.tryFind ep with
            | Some ch ->
              return (brokerByEndPoint, Map.add p ch brokerByPartition)
            | None ->
              let! ch = Chan.connect (conn.Config.version,conn.Config.tcpConfig,conn.Config.clientId) ep
              return (Map.add ep ch brokerByEndPoint, Map.add p ch brokerByPartition)
        | None -> 
          return failwith "invalid state" })
    
    let messageVer = ProducerConfig.messageVersion conn.Config.version

    let! ct' = Async.CancellationToken
    let queueCts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct')

    let queue (ch:Chan) =
      let buf = BoundedMb.create cfg.bufferSize
      BoundedMb.take buf
      |> AsyncSeq.replicateInfiniteAsync
      |> AsyncSeq.bufferByCountAndTime cfg.batchSize cfg.batchLinger
      |> AsyncSeq.iterAsync (Array.concat >> sendBatch cfg messageVer ch)
      |> (fun x -> Async.Start (x, queueCts.Token))
      (flip BoundedMb.put buf)
   
    let brokerQueues =
      brokerByPartition
      |> Map.toSeq
      |> Seq.map (fun (_,ch) ->
        let ep = Chan.endpoint ch
        let q = queue ch
        ep,q)
      |> Map.ofSeq
        
    return { partitions = partitions ; partitionBrokers = brokerByPartition ; brokerQueues = brokerQueues } }

  let private produceBatchInternal (state:ProducerState) (createBatch:Partition[] -> Partition * ProducerMessage[]) = async {
    let p,ms = createBatch state.partitions
    let q = 
      let ep = state.partitionBrokers |> Map.find p |> Chan.endpoint
      state.brokerQueues |> Map.find ep
    let rep = IVar.create ()
    do! q [|(ms,p),rep|]
    let! res = IVar.get rep
    match res with
    | Success res ->
      return Success res
    | Failure errs ->
      return Failure (Resource.ResourceErrorAction.RecoverRetry (exn(sprintf "%A" errs))) }

  /// Creates a producer.
  let createAsync (conn:KafkaConn) (config:ProducerConfig) : Async<Producer> = async {
    Log.info "initializing_producer|topic=%s" config.topic
    let messageVersion = Versions.produceReqMessage (Versions.byKey conn.Config.version ApiKey.Produce)
    let! resource = 
      Resource.recoverableRecreate 
        (getState conn config) 
        (fun (s,v,_req,ex) -> async {
          Log.warn "closing_resource|version=%i partitions=[%s] error=%O" v (Printers.partitions s.partitions) ex
          return () })
    let! state = Resource.get resource
    let produceBatch = Resource.injectWithRecovery resource (RetryPolicy.constantMs 500) (produceBatchInternal)
    let p = { state = resource ; config = config ; conn = conn ; messageVersion = messageVersion ; produceBatch = produceBatch }
    Log.info "producer_initialized|topic=%s partitions=[%s]" config.topic (Printers.partitions state.partitions)
    return p }

  /// Creates a producer.
  let create (conn:KafkaConn) (cfg:ProducerConfig) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  /// Produces a message. The message will be sent as part of a batch and the result will correspond to the offsets
  /// produced by the entire batch.
  let produce (p:Producer) (m:ProducerMessage) =
    p.produceBatch (fun ps -> Partitioner.partition p.config.partitioner p.config.topic ps m, [|m|])

  /// Produces a batch of messages using the specified function which creates messages given a set of partitions
  /// currently configured for the topic.
  /// The messages will be sent as part of a batch and the result will correspond to the offsets
  /// produced by the entire batch.
  let produceBatch (p:Producer) =
    p.produceBatch