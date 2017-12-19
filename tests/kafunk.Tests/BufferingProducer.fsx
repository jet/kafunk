#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading
open Refs

let Log = Log.create "buffering_producer_test"

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "10000" |> Int64.Parse
let batchSize = argiDefault 4 "10" |> Int32.Parse
let messageSize = argiDefault 5 "10" |> Int32.Parse
let parallelism = argiDefault 6 "1" |> Int32.Parse

let volumeMB = (N * int64 messageSize) / int64 1000000

let payload = 
  let bytes = Array.zeroCreate messageSize
  let rng = Random()
  rng.NextBytes bytes
  bytes

let batchCount = int (N / int64 batchSize)

Log.info "producer_run_starting|host=%s topic=%s messages=%i batch_size=%i batch_count=%i message_size=%i parallelism=%i MB=%i" 
  host topic N batchSize batchCount messageSize parallelism volumeMB

let conn = Kafka.connHost host

let producerCfg =
  ProducerConfig.create (
    topic, 
    Partitioner.roundRobin, 
    requiredAcks = RequiredAcks.Local,
    timeout = ProducerConfig.DefaultTimeoutMs,
    bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes,
    batchSizeBytes = 100000, //ProducerConfig.DefaultBatchSizeBytes,
    //batchSizeBytes = 0,
    batchLingerMs = 100, //ProducerConfig.DefaultBatchLingerMs,
    compression = CompressionCodec.None
    )

let producer = Producer.create conn producerCfg

let bufConfig = { 
  BufferingProducerConfig.bufferType = ProducerBufferType.Blocking
  capacity = 100000
  batchSize = 10000
  batchTimeMs = 1000
  timeIntervalMs = 90 }

let buffer = BufferingProducer.create producer bufConfig

// subscribe to discarding event
buffer
|> BufferingProducer.discarding  
|> Event.add (Log.warn "buffering_producer_discarding_warning|cap=%d, size=%d" bufConfig.capacity)

// subscribe to blocking event
buffer
|> BufferingProducer.blocking
|> Observable.bufferByTime (TimeSpan.FromSeconds 1.0)
|> Observable.filter (fun batch -> batch.Length > 0)
|> Observable.subscribe (fun batch ->
  Log.warn "buffering_producer_blocking_warning|capacity=%d count=%i" bufConfig.capacity batch.Length)
|> ignore

// subscribe to error event
buffer
|> BufferingProducer.errors 
|> Event.add (fun (e,_) -> Log.error "buffering_producer_error|error=%O" e)

let sw = Stopwatch.StartNew()

//let timer = Metrics.timer Log 5000

//let mutable prev = sw.ElapsedMilliseconds

//let countLatency (_:ProducerResult[]) = 
//  let current = sw.ElapsedMilliseconds
//  timer.Record(int(current - prev))
//  prev <- current

//// Subscribe result to show the time interval of writting to Kafka
//buffer
//|> BufferingProducer.results 
//|> Event.add countLatency

// Subscribe result to show the time interval of writting to Kafka
buffer
|> BufferingProducer.results 
|> Observable.bufferByTime (TimeSpan.FromSeconds 1.0)
|> Observable.filter (fun batch -> batch.Length > 0)
|> Observable.choose (fun batch -> 
  let batch = batch |> Array.concat in 
  if batch.Length > 0 then Some batch
  else None)
|> Observable.subscribe (fun batch -> 
  let str =
    batch
    |> Seq.groupBy (fun r -> r.partition)
    |> Seq.map (fun (p,xs) -> 
      let o = xs |> Seq.map (fun r -> r.offset) |> Seq.max
      sprintf "[p=%i o=%i]" p o)
    |> String.concat " ; "
  Log.info "produced_batch|offsets=[%s]" str)
|> ignore

let mutable completed = 0L

let go = async {

  let monitor = async {
    while true do
      do! Async.Sleep (1000 * 5)
      let completed = completed
      let mb = (int64 completed * int64 messageSize) / int64 1000000
      Log.info "completed=%i elapsed_sec=%f MB=%i" completed sw.Elapsed.TotalSeconds mb }

  let! _ = Async.StartChild monitor
  
  do!
    Seq.init batchCount id
    |> Seq.map (fun batchNo -> async {
      do! Async.Sleep 10
      let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
      for m in msgs do
        BufferingProducer.produce buffer m |> ignore      
      Interlocked.Add(&completed, int64 batchSize) |> ignore
    })
    |> Async.parallelThrottledIgnore parallelism

  BufferingProducer.close buffer

  Log.info "awaiting_flush"

  do! BufferingProducer.flushTask buffer |> Async.awaitTaskCancellationAsError

}

try Async.RunSynchronously go
with ex -> Log.error "ERROR|%O" ex

sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB

//Thread.Sleep 60000