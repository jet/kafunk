#r "bin/Release/kafunk.dll"
#time "on"

open Kafunk
open System

let Log = Log.create __SOURCE_FILE__

let host = "127.0.0.1:9092"
let topicName = "test-topic_1019"
let consumerCount = 1
let consumerGroup = "kafunk-test-1020a"
let messageCount = 10000000
//let messageSize = 10
//let batchSize = 1
let producerThreads = 10

let tcs = new System.Threading.Tasks.TaskCompletionSource<unit>()
let ReceiveSet = System.Collections.Concurrent.ConcurrentDictionary<int, _>()
let Duplicates = ResizeArray<int>()

let producer () = async {
 
  do! Async.SwitchToThreadPool ()

  use conn = Kafka.connHost host

  let producerCfg =
    ProducerCfg.create (
      topicName, 
      Partitioner.roundRobin, 
      requiredAcks=RequiredAcks.Local)

  let! producer = Producer.createAsync conn producerCfg

  let message i = 
    //printfn "creating message %i" i
    let seg = Binary.ofArray (Array.zeroCreate 4)
    let _ = Binary.writeInt32 i seg
    ProducerMessage.ofBytes seg

  do!
    Seq.init messageCount id
    |> Seq.map (fun i -> async {
      let message = message i
      let req = { topic = topicName ; messages = [| message |] }
      let! res = Producer.produce producer [| req |]
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

  let handle (ms:MessageSet) = async {    
    ms.messages
    |> Seq.iter (fun (_o,_ms,m) -> 
      try
        if not (isNull m.value.Array) && m.value.Count > 0 then
          let (i,_) = Binary.readInt32 m.value
          //Log.trace "received_message=%i" i
          if ReceiveSet.TryAdd (i,i) then
            if ReceiveSet.Count >= messageCount then
              Log.warn "received complete set count=%i" ReceiveSet.Count
              tcs.SetResult()            
          else 
            //Log.warn "duplicate message=%i" i
            Duplicates.Add i

      with ex ->
        Log.error "error=%O" ex)
    Log.info "consuming_message_set|count=%i size=%i first_offset=%i"
      (ms.messages.Length)
      (ms.messages |> Seq.sumBy (fun (_,s,_) -> s))
      (if ms.messages.Length > 0 then ms.messages |> Seq.map (fun (o,_,_) -> o) |> Seq.min else -1L)
    return () }

  return!
    Async.choose
      (Consumer.consume conn consumerCfg |> Consumer.callbackCommitAfter handle)
      (tcs.Task |> Async.AwaitTask)
}


let sw = System.Diagnostics.Stopwatch.StartNew()

Async.Parallel
  [
    yield producer ()
    for _ in [1..consumerCount] do
      yield consumer ()
  ]
//|> Async.withCancellationToken cts.Token
|> Async.RunSynchronously

sw.Stop()


Log.info "DONE message_count=%i duplicates=%i consumer_count=%i running_time_min=%f" messageCount Duplicates.Count consumerCount (sw.Elapsed.TotalMinutes)
