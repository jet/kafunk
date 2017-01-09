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
    create <| fun (_,ps,_) ->
      ensurePartitions ps
      let i = lock rng (fun () -> rng.Next (0, ps.Length))
      ps.[i]
  
  /// CRC32 of the message key.
  let crc32Key : Partitioner =
    hashKey (fun key -> int (Crc.crc32 key.Array key.Offset key.Count))


/// Producer state.
type private ProducerState = {
  partitions : Partition[]
  version : int
}

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

} with

  /// Creates a producer configuration.
  static member create (topic:TopicName, partition:Partitioner, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout) =
    {
      topic = topic
      requiredAcks = defaultArg requiredAcks RequiredAcks.Local
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout 0
      partitioner = partition
    }


/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
type Producer = private {
  conn : KafkaConn
  config : ProducerConfig
  messageVersion : ApiVersion
  state : MVar<ProducerState>
}

/// High-level producer API.
[<Compile(Module)>]
module Producer =

  open System.Threading
  open System.Threading.Tasks

  let private Log = Log.create "Kafunk.Producer"

  let private getState (conn:KafkaConn) (t:TopicName) (oldVersion:int) = async {
    Log.info "fetching_topic_metadata|topic=%s producer_version=%i" t oldVersion
    let! topicPartitions = conn.GetMetadata [| t |]
    let topicPartitions = topicPartitions |> Map.find t
    return { partitions = topicPartitions ; version = oldVersion + 1 } }

  /// Resets producer state if caller state has matching version,
  /// otherwise returns the newer version of producer state.
  let private reset (p:Producer) (callerState:ProducerState) =
    p.state
    |> MVar.updateAsync (fun currentState -> async {
      if callerState.version = currentState.version then
        return! getState p.conn p.config.topic currentState.version 
      else
        return currentState })

  /// Creates a producer.
  let createAsync (conn:KafkaConn) (cfg:ProducerConfig) : Async<Producer> = async {
    Log.info "initializing_producer|topic=%s" cfg.topic
    let messageVersion = Versions.produceReqMessage (Versions.byKey conn.Config.version ApiKey.Produce)
    let p = { state = MVar.create () ; config = cfg ; conn = conn ; messageVersion = messageVersion }
    let! state = p.state |> MVar.putAsync (getState conn cfg.topic 0)
    Log.info "producer_initialized|topic=%s partitions=%A" cfg.topic state.partitions
    return p }

  /// Creates a producer.
  let create (conn:KafkaConn) (cfg:ProducerConfig) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  /// Produces a batch of messages.
  /// Messages are routed based on the configured routing function and
  /// metadata retrieved by the producer.
  let produce (p:Producer) (ms:ProducerMessage[]) = async {

    let conn = p.conn
    let cfg = p.config
    let messageVer = p.messageVersion
    let send = Kafka.produce conn

    // TODO: rediscover partition set on broker rebalance
    let produce (state:ProducerState) (ms:ProducerMessage[]) = async {
      let pms =
        ms
        |> Seq.groupBy (fun pm -> cfg.partitioner (cfg.topic, state.partitions, pm))
        |> Seq.map (fun (p,pms) ->
          let ms = 
            pms 
            |> Seq.map (fun pm -> Message.create pm.value pm.key None) 
            |> MessageSet.ofMessages
            |> Compression.compress messageVer cfg.compression
          p,ms)
        |> Seq.toArray
      let req = ProduceRequest.ofMessageSetTopics [| cfg.topic, pms |] cfg.requiredAcks cfg.timeout
      let! res = send req |> Async.Catch
      match res with
      | Success res ->
        let oks,errors =
          res.topics
          |> Seq.collect (fun (_t,os) ->
            os
            |> Seq.map (fun (p,ec,o) ->
              match ec with
              | ErrorCode.NoError -> Choice1Of2 (p,o)
              | ErrorCode.InvalidMessage -> Choice2Of2 (p,o)
              | _ -> Choice2Of2 (p,o)))
          |> Seq.partitionChoices
        if errors.Length > 0 then
          Log.error "produce_errors|%A" errors
          return failwithf "produce_errors|%A" errors
        return ProducerResult(oks)
      | Failure ex ->
        Log.error "produce_exception|request=%s error=%O" (ProduceRequest.Print req) ex
        return raise ex }

    let! state = MVar.get p.state
    return! produce state ms }