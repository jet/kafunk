#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading.Tasks
open System.Collections.Concurrent

let Log = Log.create __SOURCE_FILE__

let host = "127.0.0.1:9092"
let topicName = "test-topic_1020i"
let consumerCount = 2
let consumerGroup = "kafunk-test-1020a"
let messageCount = 10000000
//let messageSize = 10
let batchSize = 10
let producerThreads = 500

let tcs = new TaskCompletionSource<unit>()
let ReceiveSet = ConcurrentDictionary<int, _>()
let Duplicates = ConcurrentBag<int>()

let producer () = async {
 
  do! Async.SwitchToThreadPool ()

  use conn = Kafka.connHost host

  let producerCfg =
    ProducerConfig.create (
      topicName, 
      Partitioner.roundRobin, 
      requiredAcks=RequiredAcks.Local)

  let! producer = Producer.createAsync conn producerCfg

  let message i = 
    //printfn "creating message %i" i
    let seg = Binary.ofArray (Array.zeroCreate 4)
    let _ = Binary.writeInt32 i seg
    ProducerMessage.ofBytes seg

  let batchCount = messageCount / batchSize

  do!
    Seq.init batchCount id
    |> Seq.map (fun i -> async {
      let messages = 
        Seq.init batchSize (fun j -> message (i * batchSize + j))
        |> Seq.toArray
      let! res = Producer.produce producer messages
      return () })
    |> Async.ParallelIgnore producerThreads

  Log.error "producer_done"
}


let consumer () = async {

  do! Async.SwitchToThreadPool ()

  use conn = Kafka.connHost host

  let consumerCfg = 
    ConsumerConfig.create (
      consumerGroup, 
      [|topicName|], 
      initialFetchTime=Time.EarliestOffset, 
      fetchBufferBytes=100000)

  let handle _tn _p (ms:MessageSet) = async {
    ms.messages
    |> Seq.iter (fun (o,ms,m) -> 
      try
        if not (isNull m.value.Array) && m.value.Count > 0 then
          let (i,_) = Binary.readInt32 m.value
          //Log.trace "received_message=%i" i
          if ReceiveSet.TryAdd (i,i) then
            if ReceiveSet.Count >= messageCount then
              Log.warn "received_complete_set|receive_count=%i" ReceiveSet.Count
              tcs.SetResult()
          else 
            Duplicates.Add i
        else
          Log.warn "empty_message_received|offset=%i message_size=%i" o ms
      with ex ->
        Log.error "error=%O" ex) }

  return!
    Async.choose
      (Consumer.create conn consumerCfg |> Consumer.consumeCommitAfter handle)
      (tcs.Task |> Async.AwaitTask)
}

let sw = System.Diagnostics.Stopwatch.StartNew()

let rec monitor () = async {
  while not tcs.Task.IsCompleted do 
    do! Async.Sleep 5000
    Log.warn "receive_count=%i duplicate_count=%i running_time_min=%f" ReceiveSet.Count Duplicates.Count sw.Elapsed.TotalMinutes }

Async.Parallel
  [
    yield monitor ()
    yield producer ()
    for _ in [1..consumerCount] do
      yield consumer ()
  ]
|> Async.RunSynchronously

sw.Stop()


Log.info "DONE message_count=%i duplicates=%i consumer_count=%i running_time_min=%f" messageCount Duplicates.Count consumerCount (sw.Elapsed.TotalMinutes)
