#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
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
let batchSize = argiDefault 4 "10" |> Int32.Parse
let consumerCount = argiDefault 5 "2" |> Int32.Parse
let producerThreads = argiDefault 6 "100" |> Int32.Parse

let testId = Guid.NewGuid().ToString("n")
let consumerGroup = "kafunk-producer-consumer-test-" + testId

let messageKey = "at-least-once-test-" + testId
let messageKeyBytes = messageKey |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray
let chanConfig = ChanConfig.create (requestTimeout = TimeSpan.FromSeconds 10.0)

let consuming = new CountdownEvent(consumerCount)





type ReportReq = 
  | Receive of values:int[] * messageCount:int
  | Produce of count:int * offsets:(Partition * Offset)[]
  | Report of AsyncReplyChannel<Report>

and Report =
  struct
    val received : int
    val duplicates : int
    val produced : int
    val skipped : int
    val nonContigCount : int
    val nonContigSample : (int * int)[]
    val contigCount : int
    val offsets : Map<Partition, Offset>
    new (r,d,p,s,nc,ncs,cc,os) = 
      { received = r ; duplicates = d ; produced = p ; skipped = s ; nonContigCount = nc ; nonContigSample = ncs ; contigCount = cc ; offsets = os }
  end

let completed = IVar.create ()

let mb = Mb.Start (fun mb ->
  
  let duplicates = ResizeArray<int>()
  let received = SortedList<int, int>()
  let produced = ref 0
  let skipped = ref 0
  let offsets = ref Map.empty
  
  mb.Error.Add (fun e -> Log.error "mailbox_error|%O" e)

  let report () = 
    let contigCount =
      received.Keys
      |> Seq.pairwise
      |> Seq.where (fun (x,y) -> y = x + 1)
      |> Seq.length
    let nonContig =
      received.Keys
      |> Seq.pairwise
      |> Seq.where (fun (x,y) -> y <> x + 1)
    let nonContigCount =
      nonContig
      |> Seq.length
    let nonContigSample =
      if totalMessageCount = !produced then
        nonContig |> Seq.toArray
      else 
        [||]
    Report(received.Count, duplicates.Count, !produced, !skipped, nonContigCount, nonContigSample, contigCount, !offsets) 

  let rec loop () = async {
    let! req = mb.Receive ()
    match req with
    | Receive (values,messageBatchCount) ->
      
      for v in values do
        if received.ContainsKey v then
          duplicates.Add v
        else
          received.Add (v, v)

      if received.Count >= totalMessageCount then
        Log.info "received_complete_set|receive_count=%i" received.Count
        IVar.put (report ()) completed

      Interlocked.Add(skipped, messageBatchCount - values.Length) |> ignore

    | Produce (count,os) ->
      Interlocked.Add (produced, count) |> ignore
      offsets := (!offsets, os |> Map.ofArray) ||> Map.mergeWith max

    | Report rep ->
      rep.Reply (report ())

    return! loop () }

  loop ())




let producer = async {
 
  let message (messageNumber:int) =
    let value = Binary.ofArray (Array.zeroCreate 4)
    let _ = Binary.writeInt32 messageNumber value
    ProducerMessage.ofBytes (value, messageKeyBytes)

  let messageBatch (batchNumber:int) (ps:Partition[]) = 
    let p = ps.[batchNumber % ps.Length]
    let messages = Array.init batchSize (fun j -> message (batchNumber * batchSize + j))
    p, messages

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
      batchSize = 1000,
      bufferSize = 1000)

  let! producer = Producer.createAsync conn producerCfg

  do!
    Seq.init batchCount id
    |> Seq.map (fun batchNumber -> async {
      try
        let! res = Producer.produceBatch producer (messageBatch batchNumber)
        mb.Post (ReportReq.Produce (batchSize, res.offsets))
      with ex ->
        Log.error "produce_error|error=%O" ex })
    |> Async.ParallelThrottledIgnore producerThreads

  Log.info "producer_done" }


let consumer = async {

  let handle (_:GroupMemberState) (ms:ConsumerMessageSet) = async {
        
    let values = 
      ms.messageSet.messages
      |> Seq.choose (fun (o,ms,m) ->
        let key = Binary.toString m.key
        if key = messageKey then
          let i,_ = Binary.readInt32 m.value
          Some i
        else
          None)
      |> Seq.toArray

    mb.Post (ReportReq.Receive (values,ms.messageSet.messages.Length)) }

  let connCfg = KafkaConfig.create ([KafkaUri.parse host], tcpConfig = chanConfig)
  use! conn = Kafka.connAsync connCfg

  let consumerCfg = 
    ConsumerConfig.create (
      consumerGroup, 
      topic = topicName, 
      initialFetchTime = Time.LatestOffset, 
      outOfRangeAction = ConsumerOffsetOutOfRangeAction.HaltConsumer,
      fetchMaxBytes = 100000,
      endOfTopicPollPolicy = RetryPolicy.constantMs 1000)

  let! consumer = Consumer.createAsync conn consumerCfg

  consuming.Signal () |> ignore

  return!
    Async.choose
      (Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 5.0) handle consumer)
      (IVar.get completed |> Async.Ignore) }

let sw = System.Diagnostics.Stopwatch.StartNew()

let monitor = async {
  while not completed.Task.IsCompleted do 
    do! Async.Sleep 5000
    let! report = mb.PostAndAsyncReply (ReportReq.Report)
    let pending = totalMessageCount - report.received
    let lag = report.produced - report.received
    let offsetStr = report.offsets |> Seq.map (fun kvp -> sprintf "p=%i o=%i" kvp.Key kvp.Value) |> String.concat " ; "
    let contigDelta = report.received - report.contigCount 
    Log.info "monitor|produced=%i received=%i lag=%i duplicates=%i pending=%i contig=%i contig_delta=%i non_contig=%i non_contig_sample=%A offsets=[%s] running_time_min=%f" 
      report.produced report.received lag report.duplicates pending report.contigCount contigDelta report.nonContigCount report.nonContigSample offsetStr sw.Elapsed.TotalMinutes }

Log.info "starting_producer_consumer_test|host=%s topic=%s message_count=%i batch_size=%i consumer_count=%i producer_parallelism=%i" 
  host topicName totalMessageCount batchSize consumerCount producerThreads

try
  Async.Parallel
    [
      yield monitor
      for _ in [1..consumerCount] do
        yield (consumer |> Async.tryWith (fun ex -> async { Log.error "consumer_error|%O" ex }))
        Thread.Sleep 100
      yield (producer |> Async.tryWith (fun ex -> async { Log.error "producer_errror|%O" ex }))
    ]
  |> Async.RunSynchronously
  |> ignore
with ex ->
  Log.error "%O" ex

sw.Stop()

let report = completed |> IVar.get |> Async.RunSynchronously

Log.info "completed_producer_consumer_test|message_count=%i duplicates=%i consumer_count=%i running_time_min=%f" 
  totalMessageCount report.duplicates consumerCount (sw.Elapsed.TotalMinutes)
