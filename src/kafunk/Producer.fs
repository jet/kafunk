namespace Kafunk

open Kafunk
open Kafunk.Prelude
open Kafunk.Protocol

/// A producer message.
type ProducerMessage =
  struct
    /// The message payload.
    val value : Binary.Segment
    /// The optional message key.
    val key : Binary.Segment
    /// The optional routing key.
    val routeKey : string
    new (value:Binary.Segment, key:Binary.Segment, routeKey:string) =
      { value = value ; key = key ; routeKey = routeKey }
  end
    with

      /// Creates a producer message.
      static member ofBytes (value:Binary.Segment, ?key, ?routeKey) =
        ProducerMessage(value, defaultArg key Binary.empty, defaultArg routeKey null)

      static member ofBytes (value:byte[], ?key, ?routeKey) =
        let keyBuf = defaultArg (key |> Option.map Binary.ofArray) Binary.empty
        ProducerMessage(Binary.ofArray value, keyBuf, defaultArg routeKey null)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Partitioner =

  /// Constantly returns the same partition.
  let konst (p:Partition) : TopicName * Partition[] * ProducerMessage -> Partition =
    konst p

  /// Computes the hash-code of the routing key to get the topic partition.
  /// NB: Object.GetHashCode isn't guaranteed to be stable across runtimes.
  let routeKeyHashCode : TopicName * Partition[] * ProducerMessage -> Partition =
    fun (_,ps,pm) ->
      let hc = if isNull pm.routeKey then 0 else pm.routeKey.GetHashCode()
      ps.[hc % ps.Length]

  /// Computes the hash-code of the routing key to get the topic partition.
  /// NB: Object.GetHashCode isn't guaranteed to be stable across runtimes.
  let roundRobin : TopicName * Partition[] * ProducerMessage -> Partition =
    let i = ref 0
    fun (_,ps,_) -> ps.[System.Threading.Interlocked.Increment i % ps.Length]


type MessageBundle = {
  topic : TopicName
  messages : ProducerMessage[] }

/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
type Producer = P of (MessageBundle[] -> Async<ProduceResponse>)

/// Producer configuration.
type ProducerCfg = {

  /// The topic to produce to.
  topic : TopicName

  /// The acks required.
  requiredAcks : RequiredAcks

  /// The compression method to use.
  compression : byte

  /// The maximum time to wait for acknowledgement.
  timeout : Timeout

  /// A partition function which given a topic name, cluster topic metadata and the message payload, returns the partition
  /// which the message should be written to.
  partition : TopicName * Partition[] * ProducerMessage -> Partition

}
with
  static member create (topic:TopicName, partition, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout) =
    {
      topic = topic
      requiredAcks = defaultArg requiredAcks RequiredAcks.Local
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout 0
      partition = partition
    }

/// High-level producer API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =

  type private ProducerState = {
    partitions : Partition[]
    version : int
  }

  /// Creates a producer given a Kafka connection and producer configuration.
  let createAsync (conn:KafkaConn) (cfg:ProducerCfg) : Async<Producer> = async {

    let producerState : MVar<ProducerState> = MVar.create ()

    let rec init (_prevState:ProducerState option) = async {            
      let state = async {
        let! topicPartitions = conn.GetMetadata [|cfg.topic|]
        let topicPartitions =      
          topicPartitions
          |> Map.find cfg.topic
        return { partitions = topicPartitions ; version = 0 } }
      // TODO: optimistic concurrency to address overlapping recoveries
      let! state = producerState |> MVar.putAsync state
      return state }

    and produce (state:ProducerState) (ms:MessageBundle[]) = async {
      let topicMs =
        ms
        |> Seq.map (fun {topic = tn; messages = pms} ->
          let ms =
            pms
            |> Seq.groupBy (fun pm -> cfg.partition (tn, state.partitions, pm))
            |> Seq.map (fun (p,pms) ->
              let messages = pms |> Seq.map (fun pm -> Message.create pm.value (Some pm.key) None) 
              let ms = Compression.compress cfg.compression messages
              p,ms)
            |> Seq.toArray
          tn,ms)
        |> Seq.toArray
      let req = ProduceRequest.ofMessageSetTopics topicMs cfg.requiredAcks cfg.timeout
      let! res = Kafka.produce conn req
      if res.topics.Length = 0 then
        // TODO: handle errors
        let! state' = init (Some state)
        return! produce state' ms
      else 
        return res }

    let! state = init None
    return P (produce state) }

  let create (conn:KafkaConn) (cfg:ProducerCfg) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  let produce (P(p)) ms =
    p ms

  let produceSingle (p:Producer) (tn:TopicName, pms:ProducerMessage[]) =
    produce p [| {topic = tn; messages = pms} |]