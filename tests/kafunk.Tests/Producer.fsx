#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading

//Log.MinLevel <- LogLevel.Trace
let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "1000000" |> Int64.Parse
let batchSize = argiDefault 4 "100" |> Int32.Parse
let messageSize = argiDefault 5 "10" |> Int32.Parse
let parallelism = argiDefault 6 "1" |> Int32.Parse
let explicitBatch = argiDefault 7 "false" |> Boolean.Parse

let volumeMB = (N * int64 messageSize) / int64 1000000
let payload = Array.zeroCreate messageSize
let batchCount = int (N / int64 batchSize)

Log.info "producer_run_starting|host=%s topic=%s messages=%i batch_size=%i batch_count=%i message_size=%i parallelism=%i MB=%i" 
  host topic N batchSize batchCount messageSize parallelism volumeMB

let connCfg = 
  
  let chanConfig = 
    ChanConfig.create (
      requestTimeout = TimeSpan.FromSeconds 10.0,
      sendBufferSize = ChanConfig.DefaultSendBufferSize,
      connectRetryPolicy = ChanConfig.DefaultConnectRetryPolicy,
      requestRetryPolicy = ChanConfig.DefaultRequestRetryPolicy,
//      connectRetryPolicy = RetryPolicy.none,
//      requestRetryPolicy = RetryPolicy.none
      bufferPool = BufferPool.bufferManager 100000000L 1000000
      //bufferPool = BufferPool.GC
      )

  KafkaConfig.create (
    [KafkaUri.parse host], 
    //[KafkaUri.parse "localhost:9092" ; KafkaUri.parse "localhost:9093" ; KafkaUri.parse "localhost:9094"], 
    tcpConfig = chanConfig,
    //requestRetryPolicy = KafkaConfig.DefaultRequestRetryPolicy,
    requestRetryPolicy = RetryPolicy.constantBoundedMs 1000 10,
    //bootstrapConnectRetryPolicy = KafkaConfig.DefaultBootstrapConnectRetryPolicy)
    bootstrapConnectRetryPolicy = RetryPolicy.constantBoundedMs 1000 3
    )

let conn = Kafka.conn connCfg

let producerCfg =
  ProducerConfig.create (
    topic, 
    Partitioner.roundRobin, 
    requiredAcks = RequiredAcks.AllInSync,
    timeout = ProducerConfig.DefaultTimeoutMs,
    bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes,
    batchSizeBytes = 2000000,
    batchLingerMs = 1000
    )

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let counter = Metrics.counter Log (1000 * 5)
//let timer = Metrics.timer Log (1000 * 5)

let cts = new CancellationTokenSource()

let sw = Stopwatch.StartNew()
let mutable completed = 0L


let go = async {

  let offsets = Collections.Concurrent.ConcurrentDictionary<Partition, Offset>()

  let monitor = async {
    while true do
      do! Async.Sleep (1000 * 5)
      let completed = completed
      let mb = (int64 completed * int64 messageSize) / int64 1000000
      let offsets = 
        (offsets.ToArray())
        |> Seq.map (fun kvp -> kvp.Key, kvp.Value)
        |> Seq.sortBy fst
        |> Seq.map (fun (p,o) -> sprintf "[p=%i o=%i]" p o)
        |> String.concat " ; "
      Log.info "completed=%i elapsed_sec=%f MB=%i offsets=[%s]" completed sw.Elapsed.TotalSeconds mb offsets }

  let! _ = Async.StartChild monitor

//  let threadPoolMonitor = async {
//    while true do
//      do! Async.Sleep (1000 * 5)
//      let maxWorkerThreads = ref 0
//      let maxIoThreads = ref 0
//      let minWorkerThreads = ref 0
//      let minIoThreads = ref 0
//      let availWorkerThreads = ref 0
//      let availIoThreads = ref 0
//      ThreadPool.GetMaxThreads (maxWorkerThreads, maxIoThreads)
//      ThreadPool.GetMinThreads (minWorkerThreads, minIoThreads)
//      ThreadPool.GetAvailableThreads (availWorkerThreads, availIoThreads)
//      Log.info "thread_pool|max_worker=%i max_io=%i min_worker=%i min_io=%i avail_worker=%i avail_io=%i"
//        !maxWorkerThreads !maxIoThreads !minWorkerThreads !minIoThreads !availWorkerThreads !availIoThreads }
//
//  let! _ = Async.StartChild threadPoolMonitor

  if explicitBatch then

    let produceBatch = 
      Producer.produceBatch producer
      |> Metrics.throughputAsyncTo counter (fun _ -> batchSize)
      //|> Metrics.latencyAsyncTo timer

    return!
      Seq.init batchCount id
      |> Seq.map (fun batchNo -> async {
        try
          let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
          let! prodRes = produceBatch (fun pc -> batchNo % pc, msgs)
          Interlocked.Add(&completed, int64 batchSize) |> ignore
          offsets.[prodRes.partition] <- prodRes.offset
          return ()
        with ex ->
          Log.error "produce_error|%O" ex
          return raise ex })
      |> Async.parallelThrottledIgnore parallelism

  else

//    let produce = 
//      Producer.produce producer
//      |> Metrics.throughputAsyncTo counter (fun _ -> 1)
//      //|> Metrics.latencyAsyncTo timer

    let produce = 
      Producer.produceBatched producer
      |> Metrics.throughputAsyncTo counter (fun (_,r) -> batchSize)

    return!
      Seq.init batchCount id
      |> Seq.map (fun batchNo -> async {
        try
          let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
          let! res = produce msgs
//          let! res =
//            msgs
//            |> Seq.map (fun m -> async {
//              let! prodRes = produce m
//              offsets.[prodRes.partition] <- prodRes.offset
//              return () })
//            |> Async.parallelThrottledIgnore batchSize
          Interlocked.Add(&completed, int64 batchSize) |> ignore
          return ()
        with ex ->
          Log.error "produce_error|%O" ex
          return raise ex })
      |> Async.parallelThrottledIgnore parallelism }

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