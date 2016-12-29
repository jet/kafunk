#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "1000000" |> Int32.Parse
let batchSize = argiDefault 4 "500" |> Int32.Parse
let messageSize = argiDefault 5 "5000" |> Int32.Parse
let parallelism = argiDefault 6 "100" |> Int32.Parse

let conn = Kafka.connHost host

let producerCfg =
  ProducerConfig.create (topic, Partitioner.roundRobin, requiredAcks=RequiredAcks.Local)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let payload = Array.zeroCreate messageSize
let batchCount = N / batchSize

Seq.init N id
|> Seq.batch batchSize
|> Seq.map (fun is -> async {
  do! Async.SwitchToThreadPool ()
  try
    let msgs = is |> Array.map (fun i -> ProducerMessage.ofBytes payload)
    let! prodRes = Producer.produce producer msgs
    //Log.info "produced_offsets|batch_size=%i offsets=%A" is.Length prodRes.offsets
    return ()
  with ex ->
    Log.error "%O" ex
    return ()
})
|> Async.ParallelThrottledIgnore parallelism
|> Async.RunSynchronously

Log.info "producer_run_completed|messages=%i batch_size=%i message_size=%i parallelism=%i" N batchSize messageSize parallelism

System.Threading.Thread.Sleep 2000