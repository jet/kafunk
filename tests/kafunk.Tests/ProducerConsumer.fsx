#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topicName = argiDefault 2 "absurd-topic"
let totalMessageCount = argiDefault 3 "10000" |> Int32.Parse
let batchSize = argiDefault 4 "1000" |> Int32.Parse
let consumerCount = argiDefault 5 "1" |> Int32.Parse
let producerThreads = argiDefault 6 "100" |> Int32.Parse

let contigDeltaThreshold = 200000

let testId = Guid.NewGuid().ToString("n")
let consumerGroup = "kafunk-producer-consumer-test-" + testId

let messageKey = "at-least-once-test-" + testId
let messageKeyBytes = messageKey |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray

let chanConfig = 
  ChanConfig.create (
    requestTimeout = TimeSpan.FromSeconds 10.0)

let consuming = new CountdownEvent(consumerCount)
let completed = IVar.create ()
let sw = System.Diagnostics.Stopwatch.StartNew()


//type Value = int

//type State =
//  | Produced of DateTime
//  | Received of DateTime * DateTime

type Ack () =
  
  /// maps values to their state (prepared | ack'd)
  let pending = SortedList<int, int>(Comparer.Default)
  let mutable contig = -1
  let mutable duplicates = 0
  let mutable received = 0
  let mutable sent = 0

  /// Find the longest contiguous sequence starting at the specified index.
  /// Returns the largest contiguous value.
  let rec findContig i prev =
    if i = pending.Count then prev else
    let v = pending.Keys.[i]
    let s = pending.Values.[i]
    if s = 1 then
      findContig (i + 1) v
    else
      prev

  /// Clears the contiguous sequence up to the specified value.
  let pruneUpTo v =
    let i = pending.IndexOfKey v
    if i > 0 then
      for _ in [0..i] do
        pending.RemoveAt 0
    
  member __.Contig = contig
  member __.Duplicates = duplicates
  member __.Sent = sent
  member __.Received = received
  member __.Pending = pending.Count

  /// Marks a sequence of items as pending and awaiting acknowledgement.
  member __.Prepare (vs:int seq) =
    for v in vs do
      pending.Add (v, 0)
      sent <- sent + 1
  
  /// Marks a set of items as acknowledged, keeping track of duplicates.
  /// Also maintains a contiguous sequence of items to verify ordering.
  member __.Ack (vs:int seq) =
    for v in vs do
      let mutable s = Unchecked.defaultof<_>
      if (pending.TryGetValue (v, &s)) then
        if s = 1 then
          // already ack'd
          duplicates <- duplicates + 1
        else
          // first time ack
          pending.[v] <- 1
          received <- received + 1
      else
        let first,last =
          if pending.Count > 0 then pending.Keys.[0], pending.Keys.[pending.Keys.Count - 1]
          else -1,-1
        if first < v then 
          failwithf "ack v=%i is not in pending list and greater than prune watermark first=%i last=%i" v first last

    let wm = contig
    let wm' = findContig 0 wm
    if wm' > wm then
      contig <- wm'
      pruneUpTo wm'



type Report =
  struct
    val received : int
    val duplicates : int
    val produced : int
    val contigCount : int
    new (r,d,p,cc) = 
      { received = r ; duplicates = d ; produced = p ; contigCount = cc }
  end


let printReport (report:Report) =
  let pending = totalMessageCount - report.received
  let lag = report.produced - report.received
  //let offsetStr = report.offsets |> Seq.map (fun kvp -> sprintf "p=%i o=%i" kvp.Key kvp.Value) |> String.concat " ; "
  let contigDelta = report.received - report.contigCount - 1
  Log.info "monitor|produced=%i received=%i lag=%i duplicates=%i pending=%i contig=%i contig_delta=%i running_time_min=%f" 
    report.produced report.received lag report.duplicates pending report.contigCount contigDelta sw.Elapsed.TotalMinutes



// ----------------------------------------------------------------------------------------------------------------------------------

[<Compile(Module)>]
module Reporter =

  type Reporter = 
    private 
    | R of Mb<ReportReq>

  and private ReportReq = 
    | Received of values:(int * (Partition * Offset))[] * messageCount:int
    | Produced of values:int[] * p:Partition * o:Offset
    | Report of AsyncReplyChannel<Report>

  let create () =

    let mb = Mb.Start (fun mb ->
  
      let ack = Ack ()

      mb.Error.Add (fun e -> Log.error "mailbox_error|%O" e ; completed.TrySetException e |> ignore)

      let report () =
        let r = new Report(ack.Received, ack.Duplicates, ack.Sent, ack.Contig) 
        //printReport r
        r

      let rec loop () = async {
        let! req = mb.Receive ()
        match req with
        | Received (values,_messageBatchCount) ->
      
          ack.Ack (values |> Seq.map fst)

          if ack.Received >= totalMessageCount then
            Log.info "received_complete_set|receive_count=%i" ack.Contig
            IVar.tryPut () completed |> ignore

        | Produced (values,p,o) ->
          ack.Prepare values

        | Report rep ->
          rep.Reply (report ())

        return! loop () }

      loop ())

    R mb

  let report (R(mb)) = 
    mb.PostAndAsyncReply (ReportReq.Report)

  let produced (R(mb)) (batch,p,o) = 
    mb.Post (ReportReq.Produced(batch,p,o))

  let consumed (R(mb)) (values,ms:ConsumerMessageSet) = 
    mb.Post (ReportReq.Received (values,ms.messageSet.messages.Length))


let reporter = Reporter.create ()

let monitor = async {
  while not completed.Task.IsCompleted do 
    do! Async.Sleep 5000
    let! report = Reporter.report reporter
    printReport report
    if (report.received - report.contigCount) > contigDeltaThreshold then
      Log.error "contig_delta_surpassed_threshold"
      IVar.tryPut () completed |> ignore }

// ----------------------------------------------------------------------------------------------------------------------------------


let producer = async {
 
  let message (messageNumber:int) =
    let value = Binary.ofArray (Array.zeroCreate 4)
    let _ = Binary.writeInt32 messageNumber value
    ProducerMessage.ofBytes (value, messageKeyBytes)

  let batchCount = totalMessageCount / batchSize

  do! consuming.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore
  do! Async.Sleep 5000 // TODO: consumer coordination

  Log.info "starting_producer_process|batch_count=%i" batchCount

  let connCfg = 
    KafkaConfig.create (
      [KafkaUri.parse host], 
      tcpConfig = chanConfig, 
      version = Versions.V_0_10_1)

  use! conn = Kafka.connAsync connCfg

  let producerCfg =
    ProducerConfig.create (
      topic = topicName, 
      partition = Partitioner.roundRobin,
      requiredAcks = RequiredAcks.AllInSync,
      batchSizeBytes = ProducerConfig.DefaultBatchSizeBytes,
      bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes)

  let! producer = Producer.createAsync conn producerCfg

  let produceProcess =
    Seq.init batchCount id
    |> Seq.map (fun batchNumber -> async {
      let batch = Array.init batchSize (fun j -> (batchNumber * batchSize + j))
      let pms = batch |> Array.map message
      Reporter.produced reporter (batch,0,0L)
      let! res = Producer.produceBatch producer (fun pc -> batchNumber % pc,pms)
      return () })
    |> Async.parallelThrottledIgnore producerThreads

  return! Async.choose (IVar.get completed) produceProcess

  Log.info "producer_done" }



// ----------------------------------------------------------------------------------------------------------------------------------


let consumer = async {

  let partitionOffsets = new ConcurrentDictionary<Partition, Offset> ()

  let handle (_:ConsumerState) (ms:ConsumerMessageSet) = async {
        
    //failwithf "testing ERRORs!"
    //Log.error "testing error!"

    //let firstOffset' = ConsumerMessageSet.firstOffset ms
    
    //let mutable lastOffset = 0L
    //if partitionOffsets.TryGetValue (ms.partition, &lastOffset) then
    //  if firstOffset' > lastOffset + 1L then
    //    failwithf "offset_gap_detected|partition=%i last_offset=%i first_offset_next_batch=%i" ms.partition lastOffset firstOffset'
    
    //partitionOffsets.[ms.partition] <- ConsumerMessageSet.lastOffset ms

    //ms.messageSet.messages
    //|> Seq.pairwise
    //|> Seq.iter (fun (msi1,msi2) -> 
    //  if msi1.offset + 1L <> msi2.offset then
    //    failwithf "non_contiguous_offsets_detected|offset1=%i offset2=%i" msi1.offset msi2.offset
    //  ())

    for msi in ms.messageSet.messages do
      match partitionOffsets.TryGetValue (ms.partition) with
      | true, lastOffset ->
        if (lastOffset + 1L <> msi.offset) then
          failwithf "non_contig_offsets|partition=%i last_offset=%i current_offset=%i" ms.partition lastOffset msi.offset
      | _ -> ()
      partitionOffsets.[ms.partition] <- msi.offset
        
    let values = 
      ms.messageSet.messages
      |> Seq.choose (fun m ->
        let key = Binary.toString m.message.key 
        if key = messageKey then
          let i,_ = Binary.readInt32 m.message.value
          Some (i,(ms.partition,m.offset))
        else
          None)
      |> Seq.toArray

    Reporter.consumed reporter (values,ms) }

  let connCfg = 
    KafkaConfig.create (
      [KafkaUri.parse host], 
      tcpConfig = chanConfig,
      version = Versions.V_0_10_1)
  use! conn = Kafka.connAsync connCfg

  let consumerCfg = 
    ConsumerConfig.create (
      consumerGroup, 
      topic = topicName, 
      autoOffsetReset = AutoOffsetReset.StartFromTime Time.EarliestOffset,
      endOfTopicPollPolicy = RetryPolicy.constantMs 1000)

  let! consumer = Consumer.createAsync conn consumerCfg

  consuming.Signal () |> ignore

  let consumeProcess = 
    Consumer.consumePeriodicCommit consumer (TimeSpan.FromSeconds 5.0) handle 

  return! Async.choose (consumeProcess) (IVar.get completed) }


// ----------------------------------------------------------------------------------------------------------------------------------


Log.info "starting_producer_consumer_test|host=%s topic=%s message_count=%i batch_size=%i consumer_count=%i producer_parallelism=%i" 
  host topicName totalMessageCount batchSize consumerCount producerThreads

let go = 
  Async.Parallel
    [
      yield monitor
      for _ in [1..consumerCount] do
        yield consumer
        Thread.Sleep 100
      yield producer
    ]
  |> Async.Ignore

try
  Async.RunSynchronously (go)
with ex ->
  Log.error "%O" ex

sw.Stop()

let report = Reporter.report reporter |> Async.RunSynchronously
printReport report
