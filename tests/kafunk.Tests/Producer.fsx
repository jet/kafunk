#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
#r "bin/Release/kafunk.Tests.dll"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "1000000" |> Int64.Parse
let batchSize = argiDefault 4 "100" |> Int32.Parse
let messageSize = argiDefault 5 "10" |> Int32.Parse
let parallelism = argiDefault 6 "1" |> Int32.Parse

let volumeMB = (N * int64 messageSize) / int64 1000000
let payload = Array.zeroCreate messageSize
let batchCount = int (N / int64 batchSize)

Log.info "producer_run_starting|host=%s topic=%s messages=%i batch_size=%i batch_count=%i message_size=%i parallelism=%i MB=%i" 
  host topic N batchSize batchCount messageSize parallelism volumeMB

let conn = Kafka.connHost host

let producerCfg =
  ProducerConfig.create (
    topic, 
    Partitioner.roundRobin, 
    requiredAcks = RequiredAcks.AllInSync,
    bufferSize = 1000,
    batchSize = 100,
    batchLinger = 1)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let counter = Metrics.counter Log (1000 * 5)
let timer = Metrics.timer Log (1000 * 5)

let produceBatch = 
  Producer.produceBatch producer
  |> Metrics.throughputAsyncTo counter (fun _ -> batchSize)
  |> Metrics.latencyAsyncTo timer

let cts = new CancellationTokenSource()

Kafunk.Log.Event 
|> Event.filter (fun e -> e.level = LogLevel.Error || e.level = LogLevel.Warn)
|> FlowMonitor.overflowEvent 10 (TimeSpan.FromSeconds 1.0)
|> Observable.add (fun es ->
  cts.Cancel ()
  printfn "ERROR_OVERFLOW|count=%i" es.Length)

let sw = Stopwatch.StartNew()
let mutable completed = 0L

let go = async {

  let! _ = Async.StartChild (async {
    while true do
      do! Async.Sleep (1000 * 5)
      let completed = completed
      let mb = (int64 completed * int64 messageSize) / int64 1000000
      Log.info "completed=%i elapsed_sec=%f MB=%i" completed sw.Elapsed.TotalSeconds mb })

  return!
    Seq.init batchCount id
    |> Seq.map (fun batchNo -> async {
      try
        let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
        let! prodRes = produceBatch (fun ps -> ps.[batchNo % ps.Length], msgs)
        Interlocked.Add(&completed, int64 batchSize) |> ignore
        return ()
      with ex ->
        Log.error "produce_error|%O" ex
        return () })
    |> Async.ParallelThrottledIgnore parallelism }

try 
  Async.RunSynchronously (go, cancellationToken = cts.Token)
with ex ->
  Log.error "%O" ex

sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB

Thread.Sleep 2000