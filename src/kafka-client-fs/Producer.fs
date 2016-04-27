namespace KafkaFs

open KafkaFs
open KafkaFs.Prelude

/// A producer message.
type ProducerMessage =
  struct

    /// The message payload.
    val value : Buffer

    /// The optional message key.
    val key : Buffer

    /// The optional routing key.
    val routeKey : string

    new (value:Buffer, key:Buffer, routeKey:string) =
      { value = value ; key = key ; routeKey = routeKey }
  end
    with

      /// Creates a producer message.
      static member ofBytes (value:Buffer, ?key, ?routeKey) =
        ProducerMessage (value, defaultArg key (Buffer.empty), defaultArg routeKey null)

      static member ofBytes (value:byte[], ?key, ?routeKey) =
        ProducerMessage (Buffer.ofArray value, defaultArg (key |> Option.map (Buffer.ofArray)) (Buffer.empty), defaultArg routeKey null)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Partitioner =

  /// Constantly returns the same partition.
  let konst (p:Partition) : TopicName * TopicMetadata * ProducerMessage -> Partition =
    konst p

  /// Computes the hash-code of the routing key to get the topic partition.
  let routeKeyHash : TopicName * TopicMetadata * ProducerMessage -> Partition =
    fun (_,tmd,pm) ->
      let hc = if isNull pm.routeKey then 0 else pm.routeKey.GetHashCode()
      let p = tmd.partitionMetadata.[hc % tmd.partitionMetadata.Length]
      p.partitionId

/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
/// TODO: ADT
type Producer = (TopicName * ProducerMessage[])[] -> Async<ProduceResponse>


/// Producer configuration.
type ProducerCfg = {

  /// The set of topics to produce to.
  /// Produce requests must be for a topic in this list.
  topics : TopicName[]

  /// The acks required.
  requiredAcks : RequiredAcks

  /// The compression method to use.
  compression : byte

  /// The maximum time to wait for acknowledgement.
  timeout : Timeout

  /// A partition function which given a topic name, cluster topic metadata and the message payload, returns the partition
  /// which the message should be written to.
  partition : TopicName * TopicMetadata * ProducerMessage -> Partition

}
with
  static member create (topics:TopicName[], partition, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout) =
    {
      topics = topics
      requiredAcks = defaultArg requiredAcks RequiredAcks.Local
      compression = defaultArg compression Compression.None
      timeout = defaultArg timeout 0
      partition = partition
    }


/// High-level producer API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =

  /// Creates a producer given a Kafka connection and producer configuration.
  let createAsync (conn:KafkaConn) (cfg:ProducerCfg) : Async<Producer> = async {

    let! metadata = conn.GetMetadata (cfg.topics)

    let metadataByTopic =
      metadata.topicMetadata
      |> Seq.map (fun tmd -> tmd.topicName,tmd)
      |> Map.ofSeq

    let produce (ms:(TopicName * ProducerMessage[])[]) = async {
      let ms =
        ms
        |> Seq.map (fun (tn,pms) ->
          let ms =
            pms
            |> Seq.groupBy (fun pm -> cfg.partition (tn, Map.find tn metadataByTopic, pm))
            |> Seq.map (fun (p,pms) ->
              let ms = pms |> Seq.map (fun pm -> Message.create (pm.value, pm.key)) |> MessageSet.ofMessages
              p,ms)
            |> Seq.toArray
          tn,ms)
        |> Seq.toArray
      let req = ProduceRequest.ofMessageSetTopics (ms, cfg.requiredAcks, cfg.timeout)
      let! res = Kafka.produce conn req
      // TODO: check for errors
      return res }

    return produce }


  let create (conn:KafkaConn) (cfg:ProducerCfg) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  let produce (p:Producer) ms =
    p ms

  let produceSingle (p:Producer) (tn:TopicName, pms:ProducerMessage[]) =
    produce p [|tn,pms|]