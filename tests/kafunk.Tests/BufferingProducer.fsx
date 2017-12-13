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
    requiredAcks = RequiredAcks.AllInSync,
    timeout = ProducerConfig.DefaultTimeoutMs,
    bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes,
    batchSizeBytes = ProducerConfig.DefaultBatchSizeBytes,
    //batchSizeBytes = 0,
    batchLingerMs = ProducerConfig.DefaultBatchLingerMs,
    compression = CompressionCodec.None
    )

let producer = Producer.create conn producerCfg

let bufConfig = { 
  BufferingProducerConfig.bufferType = ProducerBufferType.Blocking
  capacity = 100000
  batchSize = 100
  batchTimeMs = 1000
  timeIntervalMs = 90 }

let buffer = BufferingProducer.create producer bufConfig
let produce = BufferingProducer.produce buffer

// subscribe to discarding event
buffer
|> BufferingProducer.discarding  
|> Event.add (Log.warn "buffering_producer_discarding_warning|cap=%d, size=%d" bufConfig.capacity)

// subscribe to blocking event
buffer
|> BufferingProducer.blocking  
|> Event.add (Log.warn "buffering_producer_blocking_warning|cap=%d, size=%d" bufConfig.capacity)

// subscribe to error event
buffer
|> BufferingProducer.errors 
|> Event.add (fun (e,_) -> Log.error "buffering_producer_error|error=%O" e)

let sw = Stopwatch.StartNew()
let timer = Metrics.timer Log 5000

let mutable prev = sw.ElapsedMilliseconds

let countLatency (_:ProducerResult[]) = 
  let current = sw.ElapsedMilliseconds
  timer.Record(int(current - prev))
  prev <- current

// Subscribe result to show the time interval of writting to Kafka
buffer
|> BufferingProducer.results 
|> Event.add countLatency

let mutable completed = 0L

Seq.init batchCount id
|> Seq.map (fun batchNo -> async {
  //do! Async.Sleep 10
  let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
  let res =
    msgs
    |> Seq.map (fun m -> produce m)
  let (total: int) = Seq.fold (fun s x -> if x then (s + 1) else s) 0 res
  Interlocked.Add(&completed, int64 total) |> ignore
  //Log.info "completed=%i total=%i" completed total 
})
|> Async.parallelThrottledIgnore parallelism
|> Async.RunSynchronously

Log.info "awaiting_flush"

BufferingProducer.close buffer
(BufferingProducer.flushTask buffer).Wait ()

sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB


Thread.Sleep 60000