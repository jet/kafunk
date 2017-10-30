//ignore this, this is for test purpose
#r "system.net.http.dll"
#r "bin/Debug/FSharp.Control.AsyncSeq.dll"
#r "bin/Debug/Kafunk.dll"
#load "../../tests/kafunk.Tests/Refs.fsx"

//test code below
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading
open Refs
open Kafunk.AsyncEx.Async

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
    //maxInFlightRequests = 1
    )

let producer = Producer.create conn producerCfg
let bufConfig = {BufferConfig.buftype = Discarding; capacity = 1000; batchSize = 100; batchTimeMs = 1000; timeIntervalMs = 90}
let buffer = BufferingProducer.create producer bufConfig
let produce = BufferingProducer.produce buffer

// Handle overflow with log
BufferingProducer.subscribeDiscarding buffer (Log.warn "buffering_producer_overflow_warning|cap=%d, size=%d" bufConfig.capacity)

// Handle error with retry
BufferingProducer.subscribeError buffer (Seq.map produce >> ignore)

let sw = Stopwatch.StartNew()
let timer = Metrics.timer Log 5000

let mutable prev = sw.ElapsedMilliseconds

let countLatency (_:ProducerResult[]) = 
  let current = sw.ElapsedMilliseconds
  timer.Record(int(current - prev))
  prev <- current

// Subscribe result to show the time interval of writting to Kafka
BufferingProducer.subsribeProduceResult buffer countLatency

let mutable completed = 0L

let x =   
  Array.init batchCount id
  |> Array.map (fun batchNo ->
    try
      sleep (TimeSpan.FromMilliseconds <| 100.0) |> Async.RunSynchronously
      let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
      let res =
        msgs
        |> Seq.map (fun m -> produce m)
      let (total: int) = Seq.fold (fun s x -> if x then (s + 1) else s) 0 res
      Interlocked.Add(&completed, int64 total) |> ignore
      Log.info "Completed is %O, total is %O" completed total
      res
    with ex ->
      Log.error "produce_error|%O" ex
      raise ex )
sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB


Thread.Sleep 60000