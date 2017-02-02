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
let producerThreads = argiDefault 6 "500" |> Int32.Parse

let testId = Guid.NewGuid().ToString("n")
let consumerGroup = "kafunk-producer-consumer-test-" + testId

let messageKey = "at-least-once-test-" + testId
let messageKeyBytes = messageKey |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray
let chanConfig = ChanConfig.create (requestTimeout = TimeSpan.FromSeconds 10.0)

let consuming = new CountdownEvent(consumerCount)


type ReportReq = 
  | Receive of values:int[] * messageCount:int
  | Produce of count:int
  | AwaitCompletion of AsyncReplyChannel<unit>
  | Report of AsyncReplyChannel<Report>

and Report =
  struct
    val received : int
    val duplicates : int
    val produced : int
    val skipped : int
    val nonContig : (int * int)[]
    new (r,d,p,s,nc) = { received = r ; duplicates = d ; produced = p ; skipped = s ; nonContig = nc }
  end

let completed = IVar.create ()

let mb = Mb.Start (fun mb ->
  
  let duplicates = ResizeArray<int>()
  let received = SortedList<int, int>()
  let produced = ref 0
  let skipped = ref 0
  
  mb.Error.Add (fun e -> Log.error "mailbox_error|%O" e)

  let report () = 
    let nonContig =
      received.Keys
      |> Seq.pairwise
      |> Seq.choose (fun (x,y) -> if y <> x + 1 then Some (x,y) else None)
      |> Seq.truncate 100
      |> Seq.toArray
    Report(received.Count, duplicates.Count, !produced, !skipped, nonContig) 

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

    | Produce count ->
      Interlocked.Add (produced, count) |> ignore
    
    | AwaitCompletion rep ->
      let! _ = 
        Async.StartChild (async {
          let! _ = IVar.get completed
          rep.Reply () })
      ()
    
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
      requiredAcks = RequiredAcks.AllInSync,
      batchSize = 1000,
      bufferSize = 1000)

  let! producer = Producer.createAsync conn producerCfg

  do!
    Seq.init batchCount id
    |> Seq.map (fun batchNumber -> async {
      try
        let! res = Producer.produceBatch producer (messageBatch batchNumber)
        //Interlocked.Add (producedCount, batchSize) |> ignore
        mb.Post (ReportReq.Produce batchSize)
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
    Log.info "monitor|produced=%i received=%i skipped=%i duplicates=%i pending=%i non_contig=%A running_time_min=%f" 
      report.produced report.produced report.skipped report.duplicates (totalMessageCount - report.received) report.nonContig sw.Elapsed.TotalMinutes }

Log.info "starting_producer_consumer_test|host=%s topic=%s message_count=%i batch_size=%i consumer_count=%i producer_parallelism=%i" 
  host topicName totalMessageCount batchSize consumerCount producerThreads

try
  Async.Parallel
    [
      yield monitor
      for _ in [1..consumerCount] do
        yield (consumer |> Async.tryWith (fun ex -> async { Log.error "consumer_error|%O" ex }))
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
