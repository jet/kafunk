#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Concurrent

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topicName = argiDefault 2 "absurd-topic"
let messageCount = argiDefault 3 "10000" |> Int32.Parse
let batchSize = argiDefault 4 "10" |> Int32.Parse
let consumerCount = argiDefault 5 "2" |> Int32.Parse
let producerThreads = argiDefault 6 "500" |> Int32.Parse

let testId = Guid.NewGuid().ToString("n")
let consumerGroup = "kafunk-producer-consumer-test-" + testId

let completed = new TaskCompletionSource<unit>()
let consuming = new CountdownEvent(consumerCount)
let Received = ConcurrentDictionary<int, _>()
let Duplicates = ConcurrentBag<int>()
let producedCount = ref 0
let skippedCount = ref 0

let messageKey = "at-least-once-test-" + testId
let messageKeyBytes = messageKey |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray

let producer = async {
 
  let message (messageNumber:int) =
    let value = Binary.ofArray (Array.zeroCreate 4)
    let _ = Binary.writeInt32 messageNumber value
    ProducerMessage.ofBytes (value, messageKeyBytes)

  let messageBatch (batchNumber:int) (ps:Partition[]) = 
    let p = ps.[batchNumber % ps.Length]
    let messages = Array.init batchSize (fun j -> message (batchNumber * batchSize + j))
    p, messages

  let batchCount = messageCount / batchSize

  do! consuming.WaitHandle |> Async.AwaitWaitHandle |> Async.Ignore
  do! Async.Sleep 10000 // TODO: consumer coordination

  Log.info "starting_producer_process|batch_count=%i" batchCount

  use! conn = Kafka.connHostAsync host

  let producerCfg =
    ProducerConfig.create (
      topic = topicName, 
      partition = Partitioner.roundRobin,
      requiredAcks = RequiredAcks.Local,
      batchSize = 10000,
      bufferSize = 1000)

  let! producer = Producer.createAsync conn producerCfg

  do!
    Seq.init batchCount id
    |> Seq.map (fun batchNumber -> async {
      try
        let! res = Producer.produceBatch producer (messageBatch batchNumber)
        Interlocked.Add (producedCount, batchSize) |> ignore
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

    let skipped = ms.messageSet.messages.Length - values.Length
    Interlocked.Add (skippedCount, skipped) |> ignore

//    let unOrdered =
//      values
//      |> Seq.pairwise
//      |> Seq.choose (fun (x,y) -> if x > y then Some (x,y) else None)
//      |> Seq.toArray
//    if unOrdered.Length > 0 then
//      return failwithf "unordered_message_sequence|partition=%i unordered=%A" ms.partition unOrdered

    for i in values do
      if Received.TryAdd (i,i) then
        if Received.Count >= messageCount then
          Log.info "received_complete_set|receive_count=%i" Received.Count
          IVar.put () completed
      else 
        Duplicates.Add i }

  use! conn = Kafka.connHostAsync host

  let consumerCfg = 
    ConsumerConfig.create (
      consumerGroup, 
      topic = topicName, 
      initialFetchTime = Time.LatestOffset, 
      outOfRangeAction = ConsumerOffsetOutOfRangeAction.ResumeConsumerWithFreshInitialFetchTime,
      fetchMaxBytes = 200000,
      endOfTopicPollPolicy = RetryPolicy.constantMs 1000)

  let! consumer = Consumer.createAsync conn consumerCfg

  consuming.Signal () |> ignore

  return!
    Async.choose
      (Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 5.0) handle consumer)
      (IVar.get completed) }

let sw = System.Diagnostics.Stopwatch.StartNew()

let monitor = async {
  while not completed.Task.IsCompleted do 
    do! Async.Sleep 5000
    let dups = Duplicates |> Seq.truncate 5 |> Seq.toArray
    Log.warn "monitor|produce_count=%i receive_count=%i skipped=%i duplicate_count=%i duplicates=%A pending=%i running_time_min=%f" 
      !producedCount Received.Count !skippedCount Duplicates.Count dups (messageCount - Received.Count) sw.Elapsed.TotalMinutes }

Log.info "starting_producer_consumer_test|host=%s topic=%s message_count=%i batch_size=%i consumer_count=%i producer_parallelism=%i" 
  host topicName messageCount batchSize consumerCount producerThreads

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


let nonContiguous =
  Received
  |> Seq.map (fun kvp -> kvp.Key)
  |> Seq.sort
  |> Seq.pairwise
  |> Seq.choose (fun (x,y) -> if y <> x + 1 then Some (x,y) else None)
  |> Seq.toArray

Log.info "completed_producer_consumer_test|message_count=%i duplicates=%i consumer_count=%i non_contiguous=%A running_time_min=%f" 
  messageCount Duplicates.Count consumerCount nonContiguous (sw.Elapsed.TotalMinutes)
