namespace Kafunk

open FSharp.Control
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
    new (value:Binary.Segment, key:Binary.Segment) = 
      { value = value ; key = key }
  end
    with

      /// Creates a producer message.
      static member ofBytes (value:Binary.Segment, ?key) =
        ProducerMessage(value, defaultArg key Binary.empty)

      static member ofBytes (value:byte[], ?key) =
        let keyBuf = defaultArg (key |> Option.map Binary.ofArray) Binary.empty
        ProducerMessage(Binary.ofArray value, keyBuf)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Partitioner =

  /// Constantly returns the same partition.
  let konst (p:Partition) : TopicName * Partition[] * ProducerMessage -> Partition =
    konst p

  /// Round-robins across partitions.
  let roundRobin : TopicName * Partition[] * ProducerMessage -> Partition =
    let i = ref 0
    fun (_,ps,_) -> ps.[System.Threading.Interlocked.Increment i % ps.Length]

  /// Computes the hash-code of the routing key to get the topic partition.
  let hashKey (h:Binary.Segment -> int) : TopicName * Partition[] * ProducerMessage -> Partition =
    fun (_,ps,pm) -> ps.[(h pm.key) % ps.Length]


/// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
type Producer = P of (ProducerMessage[] -> Async<ProduceResponse>)

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

  /// When specified, buffers requests by the specified buffer size and buffer timeout to take advantage of batching.
  bufferCountAndTime : (int * int) option

}
with
  static member create (topic:TopicName, partition, ?requiredAcks:RequiredAcks, ?compression:byte, ?timeout:Timeout, ?bufferSize:int, ?bufferTimeoutMs:int) =
    {
      topic = topic
      requiredAcks = defaultArg requiredAcks RequiredAcks.Local
      compression = defaultArg compression CompressionCodec.None
      timeout = defaultArg timeout 0
      partition = partition
      bufferCountAndTime = 
        match bufferSize, bufferTimeoutMs with
        | Some x, Some y -> Some (x,y)
        | _ -> None
    }

/// High-level producer API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Producer =

  open System.Threading
  open System.Threading.Tasks

  type private ProducerState = {
    partitions : Partition[]
    version : int
  }

  let private boundedMbToAsyncSeq (mb:BoundedMb<'a>) : AsyncSeq<'a> =
    AsyncSeq.unfoldAsync (fun () -> async { 
      let! a = BoundedMb.take mb
      return Some (a,()) }) ()

  let private bufferByCountAndTime (partitionKey:'a -> 'k) (count:int) (timeMs:int) (sendBatch:'a[] -> Async<'b[]>) : 'a -> Async<'b> =
    
    let mb = BoundedMb.create count
    let cts = new CancellationTokenSource()

    let sendBufferAndReply (buf:('a * TaskCompletionSource<_>)[]) = async {
      let! res = sendBatch (Array.map fst buf)
      (buf,res) 
      ||> Array.zip
      |> Array.iter (fun ((_,ack),rep) -> ack.SetResult rep)
      return () }

    mb
    |> boundedMbToAsyncSeq
    |> AsyncSeq.groupBy (fst >> partitionKey)
    |> AsyncSeq.iterAsyncParallel (snd >> AsyncSeq.bufferByCountAndTime count timeMs >> AsyncSeq.iterAsync sendBufferAndReply)
    |> (fun x -> Async.Start(x, cts.Token))
           
    let send a = async {
      let rep = TaskCompletionSource<'b>()
      do! mb |> BoundedMb.put (a,rep)
      return! rep.Task |> Async.AwaitTask }

    send
 
  /// Creates a producer given a Kafka connection and producer configuration.
  let createAsync (conn:KafkaConn) (cfg:ProducerCfg) : Async<Producer> = async {

    let producerState : MVar<ProducerState> = MVar.create ()

    let send = Kafka.produce conn

    let sendBatch =

      match cfg.bufferCountAndTime with
      | None -> 

        let sendBatch (partitions:Partition[]) (ms:ProducerMessage[]) =
          let pms =
            ms
            |> Seq.groupBy (fun pm -> cfg.partition (cfg.topic, partitions, pm))
            |> Seq.map (fun (p,pms) ->
              let messages = pms |> Seq.map (fun pm -> Message.create pm.value (Some pm.key) None) 
              let ms = Compression.compress cfg.compression messages
              p,ms)
            |> Seq.toArray
          let req = ProduceRequest.ofMessageSetTopics [| cfg.topic, pms |] cfg.requiredAcks cfg.timeout
          send req

        sendBatch

      | Some (bufferSize,bufferTimeout) -> 
      
        let sendBatch (batch:ProduceRequest[]) = async {
          let r0 = batch.[0]
          let topics = batch |> Array.collect (fun x -> x.topics)
          let req = ProduceRequest(r0.requiredAcks, r0.timeout, topics)
          let! res = send req
          // TODO: refine
          let ress = Array.zeroCreate batch.Length
          for i in 0..ress.Length-1 do
            ress.[i] <- res
          return ress }

        // NB: the partition function expects that all messages are targeting the same partition
        let send = 
          bufferByCountAndTime 
            (fun (pr:ProduceRequest) ->
              let (_,ps) = pr.topics.[0]
              let (p,_,_) = ps.[0]
              p)
            bufferSize
            bufferTimeout
            sendBatch
    
        let sendBatch (partitions:Partition[]) (ms:ProducerMessage[]) = async {
          let pms =
            ms
            |> Seq.groupBy (fun pm -> cfg.partition (cfg.topic, partitions, pm))
            |> Seq.map (fun (p,pms) ->
              let messages = pms |> Seq.map (fun pm -> Message.create pm.value (Some pm.key) None) 
              let ms = Compression.compress cfg.compression messages
              p,ms)
            |> Seq.toArray
          return!
            pms
            |> Seq.map (fun (p,ms) -> ProduceRequest.ofMessageSetTopics [| cfg.topic, [| p,ms |] |] cfg.requiredAcks cfg.timeout)
            |> Seq.map send
            |> Async.Parallel
            |> Async.map Routing.concatProduceResponses }

        sendBatch

    let init (_prevState:ProducerState option) = async {
      let state = async {
        let! topicPartitions = conn.GetMetadata [| cfg.topic |]
        let topicPartitions = topicPartitions |> Map.find cfg.topic
        return { partitions = topicPartitions ; version = 0 } }
      // TODO: optimistic concurrency to address overlapping recoveries
      let! state = producerState |> MVar.putAsync state
      return state }

    let rec produce (state:ProducerState) (ms:ProducerMessage[]) = async {
      let! res = sendBatch state.partitions ms
      if res.topics.Length = 0 then
        // TODO: handle errors
        let! state' = init (Some state)
        return! produce state' ms
      else 
        return res }

    let! state = init None
    return P (produce state) }

  /// Creates a producer.
  let create (conn:KafkaConn) (cfg:ProducerCfg) : Producer =
    createAsync conn cfg |> Async.RunSynchronously

  let produce (P(p)) ms =
    p ms