namespace Kafunk

open FSharp.Control
open Kafunk
open System
open System.Text
open System.Collections.Generic

/// A producer message.
type ProducerMessage =
  struct
    /// The message payload.
    val value : ArraySegment<byte>
    /// The optional message key.
    val key : ArraySegment<byte>
    new (value:ArraySegment<byte>, key:ArraySegment<byte>) = 
      { value = value ; key = key }
  end
    with

      /// Creates a producer message.
      static member ofBytes (value:ArraySegment<byte>, ?key) =
        ProducerMessage(value, defaultArg key Binary.empty)

      /// Creates a producer message.
      static member ofBytes (value:byte[], ?key) =
        let keyBuf = defaultArg (key |> Option.map Binary.ofArray) Binary.empty
        ProducerMessage(Binary.ofArray value, keyBuf)

      /// Creates a producer message.
      static member ofString (value:string, ?key:string) =
        let keyBuf = defaultArg (key |> Option.map (Encoding.UTF8.GetBytes >> Binary.ofArray)) Binary.empty
        ProducerMessage(Binary.ofArray (Encoding.UTF8.GetBytes value), keyBuf)

      /// Returns the size, in bytes, of the message.
      static member internal size (m:ProducerMessage) =
        m.key.Count + m.value.Count

/// A producer response.
type ProducerResult =
  struct
    /// The partition to which the message was written.
    val partition : Partition
    
    /// The offset of the first message produced to the partition.
    val offset : Offset
    
    new (p,o) = { partition = p ; offset = o }
  end

/// The number of partitions of a topic.
type PartitionCount = int

/// A partition function.
type Partitioner = TopicName * PartitionCount * ProducerMessage -> Partition

/// Partition functions.
[<Compile(Module)>]
module Partitioner =

  open System.Threading

  let private ensurePartitions (ps:PartitionCount) =
    if ps = 0 then invalidArg "ps" "must have partitions"
    
  /// Creates a partition function.
  let create (f:TopicName * PartitionCount * ProducerMessage -> Partition) : Partitioner = 
    f

  /// Computes the partition.
  let partition (p:Partitioner) (t:TopicName) (pc:PartitionCount) (m:ProducerMessage) =
    p (t,pc,m)

  /// Constantly returns the same partition.
  let konst (p:Partition) : Partitioner =
    create <| konst p

  /// Round-robin partition assignment.
  let roundRobin : Partitioner =
    let mutable i = 0
    create <| fun (_,pc,_) -> 
      ensurePartitions pc
      let i' = Interlocked.Increment &i
      if i' < 0 then
        Interlocked.Exchange (&i, 0) |> ignore
        0
      else 
        i' % pc

  /// Computes the hash-code of the routing key to get the topic partition.
  let hashKey (h:Binary.Segment -> int) : Partitioner =
    create <| fun (_,pc,pm) -> 
      ensurePartitions pc
      abs (h pm.key) % pc

  /// Random partition assignment.
  let rand (seed:int option) : Partitioner =
    let rng = 
      match seed with 
      | Some s -> new Random(s)
      | None -> new Random()
    let inline next pc = lock rng (fun () -> rng.Next (0, pc))
    create <| fun (_,pc,_) ->
      ensurePartitions pc
      next pc
  
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
  requiredAcks : RequiredAcks

  // The amount of time, in milliseconds, the broker will wait trying to meet the RequiredAcks 
  /// requirement before sending back an error to the client.
  timeout : Timeout

  /// The compression method to use.
  /// Default: None
  compression : byte

  /// The per-broker, in-memory buffer size in bytes.
  /// When the buffer reaches capacity, backpressure is exerted on incoming produce requests.
  bufferSizeBytes : int

  /// The maximum size, in bytes, of a batch of produce requests per broker.
  /// If set to 0, no batching will be performed.
  batchSizeBytes : int

  /// The maximum time, in milliseconds, to wait for a produce request btch to reach capacity.
  /// If set to 0, no batching will be performed.
  batchLingerMs : int

} with

  /// The default required acks = RequiredAcks.AllInSync.
  static member DefaultRequiredAcks = RequiredAcks.AllInSync

  /// The default produce request timeout = 30000.
  static member DefaultTimeoutMs = 30000

  /// The default per-broker, produce request batch size in bytes = 16384.
  static member DefaultBatchSizeBytes = 16384

  /// The default in-memory, per-broker buffer size = 33554432.
  static member DefaultBufferSizeBytes = 33554432

  /// The default per-broker, produce request linger time in ms = 1000.
  static member DefaultBatchLingerMs = 1000

  /// Creates a producer configuration.
  static member create (topic:TopicName, partition:Partitioner, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout, 
                        ?bufferSizeBytes:int, ?batchSizeBytes, ?batchLingerMs) =
    {
      topic = topic
      partitioner = partition
      requiredAcks = defaultArg requiredAcks ProducerConfig.DefaultRequiredAcks
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout ProducerConfig.DefaultTimeoutMs
      bufferSizeBytes = defaultArg bufferSizeBytes ProducerConfig.DefaultBufferSizeBytes
      batchSizeBytes = defaultArg batchSizeBytes ProducerConfig.DefaultBatchSizeBytes
      batchLingerMs = defaultArg batchLingerMs ProducerConfig.DefaultBatchLingerMs
    }

  static member internal messageVersion (connVersion:Version) = 
    Versions.produceReqMessage (Versions.byKey connVersion ApiKey.Produce)


/// Producer state corresponding to the state of a cluster.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type private ProducerState = {

  /// Current set of partitions for the topic.
  partitions : Partition[]

  /// Per-partition queues.
  partitionQueues : Dictionary<Partition, ProducerMessageBatch -> Async<unit>> }

/// A producer-specific error.
and [<NoEquality;NoComparison;AutoSerializable(false)>] ProducerError = {
  partition : Partition
  offset : Offset
  errorCode : ErrorCode
} with
  static member create p o ec = { partition = p ; offset = o ; errorCode = ec }

and [<NoEquality;NoComparison;AutoSerializable(false)>] private ProducerMessageBatch =
  struct
    val partition : Partition
    val messages : ProducerMessage[]
    val rep : IVar<Result<ProducerResult, Choice<ChanError list, ProducerError, unit>>>
    val size : int
    new (p,ms,rep,size) = { partition = p ; messages = ms ; rep = rep ; size = size }
  end


/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type Producer = private {
  conn : KafkaConn
  config : ProducerConfig
  state : Resource<ProducerState>
  
  /// Timeout for the internal produce batch function, which includes the request timeout
  /// and the batch linger time.
  batchTimeout : TimeSpan }

/// High-level producer API.
[<Compile(Module)>]
module Producer =

  open System.Threading

  let private Log = Log.create "Kafunk.Producer"

  let private messageBatchSizeBytes (batch:ProducerMessage seq) =
    let mutable size = 0
    for m in batch do
      size <- size + (ProducerMessage.size m)
    size

  let private batchSizeBytes (batch:ProducerMessageBatch seq) =
    let mutable size = 0
    for b in batch do
      size <- size + b.size
    size

  let private batchCount (batch:ProducerMessageBatch seq) =
    let mutable count = 0
    for b in batch do
      count <- count + b.messages.Length
    count

  let private toMessageSet (messageVer:ApiVersion) (compression) (batch:seq<ProducerMessageBatch>) =
    let ps : Dictionary<Partition, ResizeArray<_>> = Dict.empty
    for b in batch do
      let mutable xs = Unchecked.defaultof<_>
      if not (ps.TryGetValue(b.partition, &xs)) then
        xs <- ResizeArray<_>()
        ps.Add (b.partition, xs)
      for pm in b.messages do
        let m = Message.create pm.value pm.key None
        let ms = Message.Size (messageVer,m)
        xs.Add (MessageSetItem(0L, ms, m))
    let arr = Array.zeroCreate ps.Count
    let mutable i = 0
    for p in ps do
      let ms = MessageSet(p.Value.ToArray())
      let ms = ms |> Compression.compress messageVer compression
      arr.[i] <- ProduceRequestPartitionMessageSet (p.Key, MessageSet.Size (messageVer,ms), ms)
      i <- i + 1
    arr

  /// Sends a batch of messages to a broker, and replies to the respective reply channels.
  let private sendBatch 
    (conn:KafkaConn)
    (cfg:ProducerConfig) 
    (messageVer:int16) 
    (ch:Chan) 
    (batch:ProducerMessageBatch seq) = async {
    //Log.trace "sending_batch|ep=%O batch_count=%i batch_size=%i" (Chan.endpoint ch) (batchCount batch) (batchSizeBytes batch)

    let pms = toMessageSet messageVer cfg.compression batch

    let req = ProduceRequest(cfg.requiredAcks, cfg.timeout, [| ProduceRequestTopicMessageSet (cfg.topic,pms) |])
    let! res = 
      Chan.send ch (RequestMessage.Produce req) 
      |> Async.map (Result.map ResponseMessage.toProduce)
      |> Async.Catch
      |> Async.map (Result.join)
    match res with
    | Success res ->
      
      let oks,transientErrors,fatalErrors =
        res.topics
        |> Seq.collect (fun x ->
          x.partitions
          |> Seq.map (fun y ->
            let o = y.offset
            let ec = y.errorCode
            let p = y.partition
            match ec with
            | ErrorCode.NoError -> Choice1Of3 (p,o)

            // timeout  (retry)
            | ErrorCode.RequestTimedOut | ErrorCode.LeaderNotAvailable | ErrorCode.NotEnoughReplicasCode
            | ErrorCode.NotEnoughReplicasAfterAppendCode -> Choice2Of3 (p,o,ec)

            // topology change (retry)
            | ErrorCode.UnknownTopicOrPartition | ErrorCode.NotLeaderForPartition -> Choice2Of3 (p,o,ec)

            // 404
            | ErrorCode.InvalidMessage | ErrorCode.MessageSizeTooLarge
            | ErrorCode.InvalidTopicCode | ErrorCode.InvalidRequiredAcksCode -> Choice3Of3 (p,o,ec)
            
            // other
            | _ -> Choice3Of3 (p,o,ec)))
        |> Seq.partitionChoices3
      
      if fatalErrors.Length > 0 then
        let msg = sprintf "fatal_errors|ep=%O errors=%A request=%s response=%s" (Chan.endpoint ch) fatalErrors (ProduceRequest.Print req) (ProduceResponse.Print res) 
        Log.error "%s" msg
        let ex = exn(msg)
        let eps = fatalErrors |> Seq.map (fun (p,_,_) -> p,ex) |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition eps
          |> Option.iter (fun ex -> IVar.tryError ex b.rep |> ignore)
      
      if transientErrors.Length > 0 then
        Log.warn "transient_errors|ep=%O errors=%A request=%s response=%s" 
          (Chan.endpoint ch) transientErrors (ProduceRequest.Print req) (ProduceResponse.Print res) 
        let eps = 
          transientErrors 
          |> Seq.map (fun (p,o,ec) -> p, Failure (Choice2Of3 (ProducerError.create p o ec))) 
          |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition eps
          |> Option.iter (fun res -> IVar.tryPut res b.rep |> ignore)

      //let resolve

      if oks.Length > 0 then
        let oks = oks |> Seq.map (fun (p,o) -> p, Success (ProducerResult(p,o))) |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition oks 
          |> Option.iter (fun res -> IVar.tryPut res b.rep |> ignore)
          
    | Failure (Choice1Of2 err) ->
      Log.warn "broker_channel_error|ep=%O error=\"%A\"" (Chan.endpoint ch) err
      // TODO: delegate routing to connection
      let! _ = conn.RemoveBroker ch
      let err = Failure (Choice1Of3 err)
      for b in batch do
        IVar.tryPut err b.rep |> ignore
      return ()

    | Failure (Choice2Of2 ex) ->
      Log.warn "broker_channel_exception|ep=%O error=\"%O\"" (Chan.endpoint ch) ex
      // TODO: delegate routing to connection
      let! _ = conn.RemoveBroker ch
      for b in batch do
        IVar.tryError ex b.rep |> ignore
      return () }

  /// Fetches cluster state and initializes a per-broker produce buffer.
  let private initProducer (conn:KafkaConn) (cfg:ProducerConfig) (ct:CancellationToken) (_prevState:ProducerState option) = async {
    
    Log.info "fetching_producer_metadata|topic=%s" cfg.topic
    let! state = conn.GetMetadataState [|cfg.topic|]

    let partitions = ConnState.topicPartitions state |> Map.find cfg.topic
    Log.info "discovered_topic_partitions|topic=%s partitions=[%s]" cfg.topic (Printers.partitions partitions)

    let brokerByPartition =
      partitions
      |> Seq.map (fun p -> 
        match ConnState.tryFindTopicPartitionBroker (cfg.topic, p) state with
        | Some ch -> 
          if (Chan.task ch).IsCompleted then
            failwith "invalid broker channel!"
          p, ch
        | None -> failwith "invalid state: should have broker after metadata fetch!")
      |> Map.ofSeq
    
    let messageVer = ProducerConfig.messageVersion conn.Config.version

    let! ct' = Async.CancellationToken
    let queueCts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct')

    let startBrokerQueue (ch:Chan) =
      let sendBatch = sendBatch conn cfg messageVer ch
      if cfg.batchSizeBytes = 0 || cfg.batchLingerMs = 0 then Array.singleton >> sendBatch else
      let bufferCond = 
        BoundedMbCond.group Group.intAdd (fun (b:ProducerMessageBatch) -> b.size) (fun size -> size >= cfg.bufferSizeBytes)
      let batchCond =
        BoundedMbCond.group Group.intAdd (fun (b:ProducerMessageBatch) -> b.size) (fun size -> size >= cfg.batchSizeBytes)
      let buffer = BoundedMb.createByCondition bufferCond
      let produceStream = 
        AsyncSeq.replicateInfiniteAsync (BoundedMb.take buffer)
      let sendProcess =
        produceStream
        |> AsyncSeq.toObservable
        |> Observable.bufferByTimeAndCondition (TimeSpan.FromMilliseconds (float cfg.batchLingerMs)) batchCond
        |> AsyncSeq.ofObservableBuffered
        |> AsyncSeq.iterAsync sendBatch
      Log.info "produce_process_starting|topic=%s ep=%O buffer_size=%i batch_size=%i" 
        cfg.topic (Chan.endpoint ch) cfg.bufferSizeBytes cfg.batchSizeBytes
      sendProcess
      |> Async.tryFinally (fun _ ->
        Log.info "produce_process_stopping|topic=%s ep=%O" cfg.topic (Chan.endpoint ch)
        let buffer = BoundedMb.getAll buffer |> Async.RunSynchronously
        for batch in buffer do
          IVar.tryPut (Failure (Choice3Of3 ())) batch.rep |> ignore)
      |> Async.tryWith (fun ex -> async {
        Log.error "producer_broker_queue_exception|ep=%O topic=%s error=\"%O\"" (Chan.endpoint ch) cfg.topic ex })
      |> (fun x -> Async.Start (x, queueCts.Token))
      (flip BoundedMb.put buffer)
       
    let partitionQueues =
      let qs = Dict.ofSeq []
      brokerByPartition
      |> Map.toSeq
      |> Seq.map (fun (p,ch) ->
        let ep = Chan.endpoint ch
        match Dict.tryGet ep qs with
        | Some q -> p,q
        | None ->
          let q = startBrokerQueue ch
          qs.Add (ep,q)
          p,q)
      |> Map.ofSeq

    return { 
      partitions = partitions ; 
      partitionQueues = partitionQueues |> Dict.ofMap } }

  let private getBrokerQueueByPartition (state:ProducerState) (p:Partition) =
    match state.partitionQueues |> Dict.tryGet p with
    | Some q -> q
    | None -> failwithf "unable to find broker for partition=%i" p

  let private produceBatchInternal (p:Producer) (state:ProducerState) (createBatch:PartitionCount -> Partition * ProducerMessage[]) = async {
    let batch =
      let p,ms = createBatch state.partitions.Length
      let rep = IVar.create ()
      ProducerMessageBatch(p,ms,rep,messageBatchSizeBytes ms)
    let q = getBrokerQueueByPartition state batch.partition
    do! q batch
    //let! res = Async.choose (IVar.get batch.rep) (Async.sleep p.batchTimeout |> Async.map (fun _ -> Failure (Choice3Of3 ())))
    let! res = IVar.getWithTimeout p.batchTimeout (fun _ -> Failure (Choice3Of3 ())) batch.rep
    match res with
    | Success res ->
      return Success res
    | Failure err ->
      let errMsg = 
        match err with
        | Choice1Of3 errs -> sprintf "broker_channel_errors|errors=%A" errs
        | Choice2Of3 e -> sprintf "producer_error|p=%i o=%i ec=%i" e.partition e.offset e.errorCode
        | Choice3Of3 _ -> sprintf "batch_timeout|timeout=%O" p.batchTimeout
      return Failure (Resource.ResourceErrorAction.RecoverRetry (exn errMsg)) }

  /// Creates a producer.
  let createAsync (conn:KafkaConn) (config:ProducerConfig) : Async<Producer> = async {
    Log.info "initializing_producer|topic=%s" config.topic
    let batchTimeout = 
      let tcpReqTimeout = conn.Config.tcpConfig.requestTimeout
      let prodReqTimeout = TimeSpan.FromMilliseconds config.timeout
      let batchLinger = TimeSpan.FromMilliseconds config.batchLingerMs
      let slack = TimeSpan.FromMilliseconds 5000 // TODO: configurable?
      [ (tcpReqTimeout + batchLinger + slack) ; (prodReqTimeout + batchLinger + slack) ] |> List.max
    let! resource = 
      Resource.recoverableRecreate 
        (initProducer conn config) 
        (fun (s,v,_,ex) -> async {
          Log.warn "closing_producer|version=%i topic=%s partitions=[%s] error=\"%O\"" 
            v config.topic (Printers.partitions s.partitions) ex
          return () })
    let! state = Resource.get resource
    let p = { state = resource ; config = config ; conn = conn ; batchTimeout = batchTimeout }
    Log.info "producer_initialized|topic=%s partitions=[%s]" config.topic (Printers.partitions state.partitions)
    return p }

  /// Creates a producer.
  let create (conn:KafkaConn) (cfg:ProducerConfig) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  /// Produces a batch of messages using the specified function which creates messages given a set of partitions
  /// currently configured for the topic.
  /// The messages will be sent as part of a batch and the result will correspond to the offset
  /// produced by the entire batch.
  let produceBatch (p:Producer) (createBatch:PartitionCount -> Partition * ProducerMessage[]) =
    Resource.injectWithRecovery p.state p.conn.Config.requestRetryPolicy (produceBatchInternal p) createBatch

  /// Produces a message. 
  /// The message will be sent as part of a batch and the result will correspond to the offset
  /// produced by the entire batch.
  let produce (p:Producer) (m:ProducerMessage) =
    produceBatch p (fun ps -> Partitioner.partition p.config.partitioner p.config.topic ps m, [|m|])

  /// Gets the configuration for the producer.
  let configuration (p:Producer) = 
    p.config