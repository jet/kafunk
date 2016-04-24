namespace KafkaFs

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.ExceptionServices

open KafkaFs

/// A producer message.
type ProducerMessage =
  struct

    /// The message payload.
    val value : ArraySeg<byte>
      
    /// The optional message key.
    val key : ArraySeg<byte>

    /// The optional routing key.
    val routeKey : ProducerMessageRouteKey
                 
    new (value:ArraySeg<byte>, key:ArraySeg<byte>, routeKey:string) = 
      { value = value ; key = key ; routeKey = routeKey }
  end      
    with
        
      /// Creates a producer message.
      static member ofBytes (value:ArraySeg<byte>, ?key, ?routeKey) =
        ProducerMessage (value, defaultArg key (ArraySeg<_>()), defaultArg routeKey null)
      
      static member ofBytes (value:byte[], ?key, ?routeKey) =
        ProducerMessage (ArraySeg.ofArray value, defaultArg (key |> Option.map (ArraySeg.ofArray)) (ArraySeg<_>()), defaultArg routeKey null)


/// A key used for routing to a partition.
and ProducerMessageRouteKey = string


/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
type Producer = (TopicName * ProducerMessage[])[] -> Async<ProduceResponse>
  

/// High-level producer API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =
    
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
    partition : TopicName * TopicMetadata * ProducerMessageRouteKey -> Partition

  }

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
            |> Seq.groupBy (fun pm -> cfg.partition (tn, Map.find tn metadataByTopic, pm.routeKey))
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
     

// -------------------------------------------------------------------------------------------------------------------------