namespace Kafunk

open System.Threading
open System
open System.Text
open System.Collections.Generic
open System.Threading.Tasks
open FSharp.Control
open Kafunk

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

    /// The number of messages in the produced batch for the partition.
    val count : int

    new (p,o,c) = { partition = p ; offset = o ; count = c }
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
  compression : CompressionCodec

  /// The per-broker, in-memory buffer size in bytes.
  /// When the buffer reaches capacity, backpressure is exerted on incoming produce requests.
  bufferSizeBytes : int

  /// The maximum size, in bytes, of a batch of produce requests per broker.
  /// If set to 0, no batching will be performed.
  batchSizeBytes : int

  /// The maximum time, in milliseconds, to wait for a produce request btch to reach capacity.
  /// If set to 0, no batching will be performed.
  batchLingerMs : int

  /// The maximum number of in-flight produce requests per broker connection.
  /// If this value is greater than 1, message ordering cannot be guaranteed.
  maxInFlightRequests : int

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

  /// The default maximum number of in-flight requests per broker connection.
  static member DefaultMaxInFlightRequests = 1

  /// Creates a producer configuration.
  static member create (topic:TopicName, partition:Partitioner, ?requiredAcks:RequiredAcks, ?compression:CompressionCodec,
                        ?timeout:Timeout, ?bufferSizeBytes:int, ?batchSizeBytes, ?batchLingerMs, ?maxInFlightRequests) =
    {
      topic = topic
      partitioner = partition
      requiredAcks = defaultArg requiredAcks ProducerConfig.DefaultRequiredAcks
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout ProducerConfig.DefaultTimeoutMs
      bufferSizeBytes = defaultArg bufferSizeBytes ProducerConfig.DefaultBufferSizeBytes
      batchSizeBytes = defaultArg batchSizeBytes ProducerConfig.DefaultBatchSizeBytes
      batchLingerMs = defaultArg batchLingerMs ProducerConfig.DefaultBatchLingerMs
      maxInFlightRequests = defaultArg maxInFlightRequests ProducerConfig.DefaultMaxInFlightRequests
    }


/// Producer state corresponding to the state of a cluster.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type ProducerState = private {

  /// The current set of routes.
  routes : ProducerRoutes

  /// Per-partition queues.
  partitionQueues : (ProducerMessageBatch -> Async<unit>)[]

  /// The partition function.
  partition : ProducerMessage -> Partition }

/// Routing info for a producer, for a topic.
and ProducerRoutes = {

  /// The set of partitions for the topic.
  partitionCount : int

  /// Brokers allocated to each partition.
  borkerByPartition : Broker[] }

/// A producer-specific error.
and private ProducerError =
  | BrokerChanError of Broker * ChanError list
  | TransientError of Partition * Offset * ErrorCode
  | RecoverableError of Partition * Offset * ErrorCode
  | BatchTimeoutError of ProducerMessageBatch
  with
    override __.ToString () = ProducerError.errorMessage __
    static member errorMessage (x:ProducerError) =
      match x with
      | BrokerChanError (b,errs) -> sprintf "broker_channel_errors|node_id=%i ep=%O errors=[%s]" b.nodeId (Broker.endpoint b) (ChanError.printErrors errs)
      | TransientError (p,o,ec) -> sprintf "transient_producer_error|p=%i o=%i ec=%i" p o ec
      | RecoverableError (p,o,ec) -> sprintf "recoverable_producer_error|p=%i o=%i ec=%i" p o ec
      | BatchTimeoutError batch -> sprintf "batch_timeout_error|partition=%i size=%i message_count=%i" batch.partition batch.size batch.messages.Length

and [<NoEquality;NoComparison;AutoSerializable(false)>] private ProducerMessageBatch =
  struct
    val partition : Partition
    val messages : ProducerMessage[]
    val rep : IVar<Result<ProducerResult, ProducerError>>
    val size : int
    new (p,ms,rep,size) = { partition = p ; messages = ms ; rep = rep ; size = size }
  end

/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
and [<NoEquality;NoComparison;AutoSerializable(false)>] Producer = private {

  /// The connection.
  conn : KafkaConn

  /// The configuration.
  config : ProducerConfig

  /// The state.
  state : Resource<ProducerState>

  /// Timeout for the internal produce batch function, which includes the request timeout
  /// and the batch linger time.
  batchTimeout : TimeSpan }

/// High-level producer API.
[<Compile(Module)>]
module Producer =

  let private Log = Log.create "Kafunk.Producer"

  let private messageBatchSizeBytes (batch:ProducerMessage seq) =
    let mutable size = 0
    for m in batch do
      size <- size + (ProducerMessage.size m)
    size

  let private toMessageSet (messageVer:ApiVersion) (compression) (batch:ProducerMessageBatch[]) (ps:Dictionary<Partition, ResizeArray<_>>) =
    for i = 0 to batch.Length - 1 do
      let b = batch.[i]
      let mutable xs = Unchecked.defaultof<_>
      if not (ps.TryGetValue(b.partition, &xs)) then
        xs <- ResizeArray<_>()
        ps.Add (b.partition, xs)
      for j = 0 to b.messages.Length - 1 do
        let pm = b.messages.[j]
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
  let private sendBatchToBroker
    (conn:KafkaConn)
    (cfg:ProducerConfig)
    (messageVer:int16)
    (b:Broker)
    (batch:ProducerMessageBatch[]) = async {
    //Log.trace "sending_batch|ep=%O batch_size_bytes=%i" (Broker.endpoint b) (batch |> Seq.sumBy (fun b -> b.size))

    let ps = Dictionary<Partition, ResizeArray<_>>()
    let pms = toMessageSet messageVer cfg.compression batch ps

    let req = ProduceRequest(cfg.requiredAcks, cfg.timeout, [| ProduceRequestTopicMessageSet (cfg.topic,pms) |])
    let! res =
      conn.SendToBroker (b, (RequestMessage.Produce req))
      |> Async.map (Result.map ResponseMessage.toProduce)
    match res with
    | Success res ->

      let oks,transientErrors,recoverableErrors,fatalErrors =
        res.topics
        |> Seq.collect (fun x ->
          x.partitions
          |> Seq.map (fun y ->
            let o = y.offset
            let ec = y.errorCode
            let p = y.partition
            match ec with
            | ErrorCode.NoError -> Choice1Of4 (p,o)

            // timeout  (retry)
            | ErrorCode.RequestTimedOut | ErrorCode.LeaderNotAvailable
            | ErrorCode.NotEnoughReplicasCode | ErrorCode.NotEnoughReplicasAfterAppendCode -> Choice2Of4 (p,o,ec)

            // topology change (retry)
            | ErrorCode.UnknownTopicOrPartition | ErrorCode.NotLeaderForPartition -> Choice3Of4 (p,o,ec)

            // 404
            | ErrorCode.InvalidMessage | ErrorCode.MessageSizeTooLarge
            | ErrorCode.InvalidTopicCode | ErrorCode.InvalidRequiredAcksCode -> Choice4Of4 (p,o,ec)

            // other
            | _ -> Choice4Of4 (p,o,ec)))
        |> Seq.partitionChoices4

      if fatalErrors.Length > 0 then
        let msg = sprintf "fatal_errors|ep=%O errors=[%s] request=%s response=%s"
                    (Broker.endpoint b) (Printers.partitionOffsetErrorCodes fatalErrors) (ProduceRequest.Print req) (ProduceResponse.Print res)
        Log.error "%s" msg
        let ex = exn(msg)
        let eps = fatalErrors |> Seq.map (fun (p,_,_) -> p,ex) |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition eps
          |> Option.iter (fun ex -> IVar.tryError ex b.rep |> ignore)

      if recoverableErrors.Length > 0 then
        Log.warn "recoverable_errors|ep=%O errors=[%s] request=%s response=%s"
          (Broker.endpoint b) (Printers.partitionOffsetErrorCodes recoverableErrors) (ProduceRequest.Print req) (ProduceResponse.Print res)
        let eps =
          recoverableErrors
          |> Seq.map (fun (p,o,ec) -> p, Failure (ProducerError.RecoverableError (p,o,ec)))
          |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition eps
          |> Option.iter (fun res -> IVar.tryPut res b.rep |> ignore)

      if transientErrors.Length > 0 then
        Log.warn "transient_errors|ep=%O errors=[%s] request=%s response=%s"
          (Broker.endpoint b) (Printers.partitionOffsetErrorCodes transientErrors) (ProduceRequest.Print req) (ProduceResponse.Print res)
        let eps =
          transientErrors
          |> Seq.map (fun (p,o,ec) -> p, Failure (ProducerError.TransientError (p,o,ec)))
          |> Map.ofSeq
        for b in batch do
          Map.tryFind b.partition eps
          |> Option.iter (fun res -> IVar.tryPut res b.rep |> ignore)

      if oks.Length > 0 then
        let oks' = Dictionary<_,_>()
        for i = 0 to oks.Length - 1 do
          let p,o = oks.[i] in
          let pmc = ps.[p].Count in
          oks'.[p] <- Success (ProducerResult(p,o,pmc))
        for i = 0 to batch.Length - 1 do
          let b = batch.[i]
          let mutable res = Unchecked.defaultof<_>
          if (oks'.TryGetValue(b.partition, &res)) then
            IVar.tryPut res b.rep |> ignore

    | Failure errs ->
      let err = ProducerError.BrokerChanError (b,errs)
      Log.warn "%s" (ProducerError.errorMessage err)
      let err = Failure err
      for b in batch do
        IVar.tryPut err b.rep |> ignore
      return () }

  let private getProducerRoutes (conn:KafkaConn) (topic:TopicName) = async {

    Log.info "fetching_producer_metadata|topic=%s" topic
    let! state = conn.GetMetadataState [|topic|]

    let partitions = 
      ClusterState.topicPartitions state 
      |> Map.tryFind topic
      |> Option.getOrLazy 
        (fun () -> failwithf "invalid_state: cluster metadata doesn't have partition information for topic=%s" topic)
    
    let partitionCount = partitions.Length

    let brokerByPartition =
      Seq.init partitionCount id
      |> Seq.map (fun p ->
        match ClusterState.tryFindTopicPartitionBroker (topic, p) state with
        | Some b -> b
        | None -> failwithf "invalid_state: should have broker after metadata fetch topic=%s partition=%i" topic p)
      |> Seq.toArray

    let partitionsByBroker =
      brokerByPartition
      |> Seq.mapi (fun p b -> p,b)
      |> Seq.groupBy snd
      |> Seq.map (fun (b,xs) -> b, xs |> Seq.map fst |> Seq.sort |> Seq.toArray)
      |> Map.ofSeq

    Log.info "discovered_topic_partitions|topic=%s partitions=[%s] allocs=[%s]"
      topic (Printers.partitionCount partitionCount) (partitionsByBroker |> Map.toSeq |> Seq.map (fun (b,ps) -> sprintf "[n=%i ep=%s ps=[%s]]" b.nodeId (Broker.endpoint b) (Printers.partitions ps)) |> String.concat " ; ")

    return {
      ProducerRoutes.borkerByPartition = brokerByPartition
      partitionCount = partitionCount } }

  /// Fetches cluster state and initializes a per-broker produce queue.
  let private initProducer (conn:KafkaConn) (cfg:ProducerConfig) (_proc:Task<unit>) (_prevState:ResourceEpoch<ProducerState> option) = async {

    let! routes = getProducerRoutes conn cfg.topic
    let partitionCount = routes.partitionCount
    let messageVer = MessageVersions.produceReqMessage (conn.ApiVersion ApiKey.Produce)

    let startBrokerQueue (b:Broker) =      
      let sendBatch = sendBatchToBroker conn cfg messageVer b
      if cfg.batchSizeBytes = 0 || cfg.batchLingerMs = 0 then 
        (Array.singleton >> sendBatch,async.Return()) else
      let add,flush,produceStream =
        if cfg.bufferSizeBytes > 0 then
          let bufferCond =
            BoundedMbCond.group Group.intAdd (fun (b:ProducerMessageBatch) -> b.size) (fun size -> size >= cfg.bufferSizeBytes)
          let boundedBuffer = BoundedMb.createByCondition bufferCond
          let produceStream = AsyncSeq.replicateInfiniteAsync (BoundedMb.take boundedBuffer)
          (flip BoundedMb.put boundedBuffer),(BoundedMb.getAll boundedBuffer),produceStream
        else
          let buf = new Collections.Concurrent.BlockingCollection<_>()
          let produceStream = buf.GetConsumingEnumerable() |> AsyncSeq.ofSeq
          let flush = async {
            let arr = ResizeArray<_>()
            let mutable batch = Unchecked.defaultof<_>
            while buf.TryTake(&batch) do
              arr.Add batch
            return arr.ToArray() }
          (buf.Add >> async.Return, flush, produceStream)
      let proc = async {
        let ep = Broker.endpoint b
        Log.info "produce_process_starting|topic=%s node_id=%i ep=%O buffer_size=%i batch_size=%i batch_linger=%i"
          cfg.topic b.nodeId ep cfg.bufferSizeBytes cfg.batchSizeBytes cfg.batchLingerMs
        let batchCond =
          BoundedMbCond.group Group.intAdd (fun (b:ProducerMessageBatch) -> b.size) (fun size -> size >= cfg.batchSizeBytes)
        let batchedProduceStream =
          produceStream
          |> AsyncSeq.bufferByConditionAndTime batchCond cfg.batchLingerMs
        let sendProcess =
          if cfg.maxInFlightRequests > 1 then        
            batchedProduceStream |> AsyncSeq.iterAsyncParallelThrottled cfg.maxInFlightRequests sendBatch
          else
            batchedProduceStream |> AsyncSeq.iterAsync sendBatch
        return!
          sendProcess
          |> Async.tryFinally (fun _ ->
            Log.info "produce_process_stopping|topic=%s node_id=%i ep=%O" cfg.topic b.nodeId ep
            let buffer = flush |> Async.RunSynchronously
            for batch in buffer do
              IVar.tryPut (Failure (ProducerError.BatchTimeoutError batch)) batch.rep |> ignore)
          |> Async.tryWith (fun ex -> async {
            Log.error "producer_process_exception|ep=%O topic=%s error=\"%O\"" ep cfg.topic ex }) }
      (add,proc)

    let partitionQueues,queueProcs =
      let qs = Dict.ofSeq []
      routes.borkerByPartition
      |> Seq.mapi (fun _ b ->
        match Dict.tryGet b.nodeId qs with
        | Some q -> q,None
        | None ->
          let q,proc = startBrokerQueue b
          qs.Add (b.nodeId,q)
          q,Some proc)
      |> Seq.toArray
      |> Array.unzip

    let queueProcs = queueProcs |> Seq.choose id |> Async.Parallel |> Async.Ignore

    let state = {
      routes = routes
      partitionQueues = partitionQueues
      partition = Partitioner.partition cfg.partitioner cfg.topic partitionCount } 
      
    return state,queueProcs }

  let private enqueueBatchAndWait (p:Producer) (state:ProducerState) (batch:ProducerMessageBatch) = async {
    if state.partitionQueues.Length < batch.partition then 
      return failwithf "unable to find broker queue for partition=%i" batch.partition 
    else
      do! state.partitionQueues.[batch.partition] batch
      return! batch.rep |> IVar.getWithTimeout p.batchTimeout (fun _ -> Failure (ProducerError.BatchTimeoutError batch)) }

  let private splitBySize (maxBatchSize:int) (batch:ProducerMessage ResizeArray) =
    // NB: if the size of an individual message is larger than the max, then this
    // would still add it to the batch and potentially cause the server to return an error
    let batches = ResizeArray<_>()
    let currentBatch = ResizeArray<_>()
    let mutable currentBatchSize = 0
    for i = 0 to batch.Count - 1 do
      let pm = batch.[i]
      let messageSize = ProducerMessage.size pm
      if (currentBatchSize + messageSize) >= maxBatchSize then 
        if currentBatch.Count > 0 then
          batches.Add (currentBatch.ToArray())
          currentBatch.Clear()
        currentBatchSize <- 0
      currentBatch.Add pm
      currentBatchSize <- currentBatchSize + messageSize
    if currentBatch.Count > 0 then
      batches.Add (currentBatch.ToArray())
    batches.ToArray()

  /// Forms batch produce requests which can be sent in parallel, ensuring batches adhere to the maximum batch size.
  let private formBatchProduceRequests (producer:Producer) (state:ProducerState) (batch:ProducerMessage seq) =
    let ps = Array.zeroCreate state.routes.partitionCount
    for pm in batch do
      let p = state.partition pm
      if isNull ps.[p] then        
        ps.[p] <- ResizeArray<_>()
      ps.[p].Add pm
    seq {
      for p = 0 to ps.Length - 1 do
        let partitionBatch = ps.[p]
        if not (isNull partitionBatch) then
          let partitionBatches = splitBySize producer.config.batchSizeBytes partitionBatch
          let req = async {
            let rs = Array.zeroCreate partitionBatches.Length
            for i = 0 to rs.Length - 1 do
              let partitionBatch = partitionBatches.[i]
              let batchSize = messageBatchSizeBytes partitionBatch
              let b = ProducerMessageBatch(p,partitionBatch,(IVar.create()),batchSize)
              let! r = enqueueBatchAndWait producer state b
              rs.[i] <- r
            return rs }
          yield req }

  let private producerErrorToRetryAction (err:ProducerError) =
    let ex = exn(ProducerError.errorMessage err)
    match err with
    | ProducerError.TransientError _ -> Choice1Of2 ex
    | _ -> Choice2Of2 ex

  let private produceBatchedWithState (producer:Producer) (state:ProducerState) (batch:ProducerMessage seq) = async {
    let reqs = formBatchProduceRequests producer state batch
    let! res = Async.Parallel reqs
    let oks,errs = res |> Seq.concat |> Seq.partitionChoices
    if errs.Length > 0 then
      let retries,recovers =
        errs 
        |> Seq.map producerErrorToRetryAction
        |> Seq.partitionChoices
      if recovers.Length > 0 then
        let ex = Exn.ofSeq (Seq.append recovers retries)
        return Failure (Resource.ResourceErrorAction.CloseRetry ex)
      else
        let ex = Exn.ofSeq retries
        return Failure (Resource.ResourceErrorAction.Retry ex)
    else 
      return Success oks }

  let private produceBatchWithState (p:Producer) (state:ProducerState) (createBatch:PartitionCount -> Partition * ProducerMessage[]) = async {
    let batch =
      let p,ms = createBatch state.routes.partitionCount
      ProducerMessageBatch(p,ms,(IVar.create ()),messageBatchSizeBytes ms)
    let! res = enqueueBatchAndWait p state batch
    match res with
    | Success res -> return Success res
    | Failure err ->
      match producerErrorToRetryAction err with
      | Choice1Of2 ex -> return Failure <| Resource.ResourceErrorAction.Retry ex
      | Choice2Of2 ex -> return Failure <| Resource.ResourceErrorAction.CloseRetry ex }

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
      Resource.create
        (config.topic)
        (initProducer conn config)
        (fun (s,ex) -> async {
          match ex with
          | Some ex ->
            Log.warn "closing_producer|version=%i topic=%s partitions=[%s] error=\"%O\""
              s.version config.topic (Printers.partitionCount s.resource.routes.partitionCount) ex
          | None ->
            Log.info "closing_producer|version=%i topic=%s partitions=[%s]"
              s.version config.topic (Printers.partitionCount s.resource.routes.partitionCount)
          return () })
    let! state = Resource.getResource resource
    let p = { state = resource ; config = config ; conn = conn ; batchTimeout = batchTimeout }
    Log.info "producer_initialized|topic=%s partitions=[%s]" config.topic (Printers.partitionCount state.routes.partitionCount)
    return p }

  /// Creates a producer.
  let create (conn:KafkaConn) (cfg:ProducerConfig) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  /// Produces a batch of messages using the specified function which creates
  /// messages given a set of partitions currently configured for the topic.
  /// The messages will be sent as part of a batch and the result will
  /// correspond to the offset produced by the entire batch. The partition
  /// selected here will override the partition function that is configured
  /// in ProducerConfig value passed in when connecting to Kafka.
  let produceBatch (p:Producer) (createBatch:PartitionCount -> Partition * ProducerMessage[]) =
    Resource.injectWithRecovery p.conn.Config.requestRetryPolicy (produceBatchWithState p) p.state createBatch

  /// Produces a batch of messages, by assigning a partition to each message,
  /// grouping messages by partitions and sending batches by partition in
  /// parallel and collecting the results.
  let produceBatched (p:Producer) (batch:ProducerMessage seq) : Async<ProducerResult[]> =
    Resource.injectWithRecovery  p.conn.Config.requestRetryPolicy (produceBatchedWithState p) p.state batch

  /// Produces a message. The message will be sent as part of a batch and the
  /// result will correspond to the offset produced by the entire batch.
  let produce (p:Producer) (m:ProducerMessage) =
    produceBatch p (fun ps -> Partitioner.partition p.config.partitioner p.config.topic ps m, [|m|])

  /// Gets the configuration for the producer.
  let configuration (p:Producer) =
    p.config
