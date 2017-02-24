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

let testId = Guid.NewGuid().ToString("n")
let consumerGroup = "kafunk-producer-consumer-test-" + testId

let messageKey = "at-least-once-test-" + testId
let messageKeyBytes = messageKey |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray

let chanConfig = 
  ChanConfig.create (
    requestTimeout = TimeSpan.FromSeconds 10.0,
    connectRetryPolicy = RetryPolicy.none)

let consuming = new CountdownEvent(consumerCount)
let completed = IVar.create ()
let sw = System.Diagnostics.Stopwatch.StartNew()


type Ack () =
  
  let pending = SortedList<int, int>(Comparer.Default)
  let mutable contig = -1
  let mutable duplicates = 0
  let mutable received = 0
  let mutable sent = 0

  let rec findContig i prev =
    if i = pending.Count then prev else
    let v = pending.Keys.[i]
    let s = pending.Values.[i]
    if s = 1 then
      findContig (i + 1) v
    else
      prev

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

  member __.Prepare (vs:int seq) =
    for v in vs do
      pending.Add (v, 0)
      sent <- sent + 1
  
  member __.Ack (vs:int seq) = 
    for v in vs do
      //if v <= contig then
      //  duplicates <- duplicates + 1
      //else
        let mutable s = Unchecked.defaultof<_>
        if (pending.TryGetValue (v, &s)) then
          if s = 1 then
            duplicates <- duplicates + 1
          else
            pending.[v] <- 1
            received <- received + 1
        else
          failwithf "invalid state=%i" v
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
    val skipped : int
    val contigCount : int
    val lastContigOffset : (Partition * Offset) option
    val offsets : Map<Partition, Offset>
    new (r,d,p,s,cc,lco,os) = 
      { received = r ; duplicates = d ; produced = p ; skipped = s ; contigCount = cc ; lastContigOffset = lco ; offsets = os }
  end


let printReport (report:Report) =
  let pending = totalMessageCount - report.received
  let lag = report.produced - report.received
  let offsetStr = report.offsets |> Seq.map (fun kvp -> sprintf "p=%i o=%i" kvp.Key kvp.Value) |> String.concat " ; "
  let contigDelta = report.received - report.contigCount 
  Log.info "monitor|produced=%i received=%i lag=%i duplicates=%i pending=%i contig=%i contig_delta=%i last_contig_offset=%A offsets=[%s] running_time_min=%f" 
    report.produced report.received lag report.duplicates pending report.contigCount contigDelta report.lastContigOffset offsetStr sw.Elapsed.TotalMinutes



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
      let skipped = ref 0
      let offsets = ref Map.empty

      mb.Error.Add (fun e -> Log.error "mailbox_error|%O" e)

      let report () =
        let r = new Report(ack.Received, ack.Duplicates, ack.Sent, !skipped, ack.Contig, None, !offsets) 
        printReport r
        Log.info "pending=%i" ack.Pending
        r

      let rec loop () = async {
        let! req = mb.Receive ()
        match req with
        | Received (values,messageBatchCount) ->
      
          ack.Ack (values |> Seq.map fst)

          Interlocked.Add(skipped, messageBatchCount - values.Length) |> ignore

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
    if (report.received - report.contigCount) > 1000000 then
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

  let connCfg = KafkaConfig.create ([KafkaUri.parse host], tcpConfig = chanConfig)
  use! conn = Kafka.connAsync connCfg

  let producerCfg =
    ProducerConfig.create (
      topic = topicName, 
      partition = Partitioner.roundRobin,
      requiredAcks = RequiredAcks.Local,
      batchSizeBytes = ProducerConfig.DefaultBatchSizeBytes,
      bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes)

  let! producer = Producer.createAsync conn producerCfg

  let produceProcess =
    Seq.init batchCount id
    |> Seq.map (fun batchNumber -> async {
      try
        let batch = Array.init batchSize (fun j -> (batchNumber * batchSize + j))
        let pms = batch |> Array.map message
        let! res = Producer.produceBatch producer (fun pc -> batchNumber % pc,pms)
        Reporter.produced reporter (batch,res.partition,res.offset)
      with ex ->
        Log.error "produce_error|error=%O" ex })
    |> Async.parallelThrottledIgnore producerThreads

  return! Async.choose (IVar.get completed) produceProcess

  Log.info "producer_done" }



// ----------------------------------------------------------------------------------------------------------------------------------


let consumer = async {

  let handle (_:ConsumerState) (ms:ConsumerMessageSet) = async {
        
    let values = 
      ms.messageSet.messages
      |> Seq.choose (fun (o,_,m) ->
        let key = Binary.toString m.key
        if key = messageKey then
          let i,_ = Binary.readInt32 m.value
          Some (i,(ms.partition,o))
        else
          None)
      |> Seq.toArray

    Reporter.consumed reporter (values,ms) }

  let connCfg = 
    KafkaConfig.create (
      [KafkaUri.parse host], 
      tcpConfig = chanConfig,
      bootstrapConnectRetryPolicy = (RetryPolicy.constantMs 1000 |> RetryPolicy.maxAttempts 2))
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
