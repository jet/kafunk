#r "bin/release/net45/confluent.kafka.dll"
#load "Refs.fsx"
#time "on"

open System
open System.Text
open System.Collections.Generic
open System.Diagnostics
open System.Threading
open Kafunk
open Refs
open Confluent.Kafka
open Confluent.Kafka.Serialization

let toDict (xs:seq<'a * 'b>) =
  let d = new Dictionary<_, _>()
  for (k,v) in xs do
    d.Add (k,v)
  d


let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "1000000" |> Int64.Parse
let batchSize = argiDefault 4 "100" |> Int32.Parse
let messageSize = argiDefault 5 "10" |> Int32.Parse
let parallelism = argiDefault 6 "1" |> Int32.Parse

let payload = //"v"
  let bytes = Array.zeroCreate messageSize
  let rng = Random()
  rng.NextBytes bytes
  bytes |> Encoding.UTF8.GetString

Log.info "running_producer_test|host=%s topic=%s count=%i batch_size=%i message_size=%i parallelism=%i"
  host topic N batchSize messageSize parallelism

let counter = Metrics.counter Log (1000 * 5)

let config =
  [ 
      
    "bootstrap.servers", box host 
    "acks", box "all"
    //"batch.size", box 1000000
    "batch.num.messages", box 100000
    "linger.ms", box 1000
    "max.in.flight.requests.per.connection", box 1

  ] |> toDict

let sw = Stopwatch.StartNew()
let mutable completed = 0L

let go = async {

  let offsets = Collections.Concurrent.ConcurrentDictionary<Partition, Kafunk.Protocol.Offset>()

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

  //use producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8))
  use producer = new Producer<string, string>(config, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8))

  producer.OnLog |> Event.add (fun e ->
    Log.info "librdkafka|name=%s facility=%s message=%s" e.Name e.Facility e.Message
  )

  let produce (m:string) = async {
    let! res = producer.ProduceAsync(topic, "k", m, true) |> Async.awaitTaskCancellationAsError
    return res }

  let produce = 
    produce
    |> Metrics.throughputAsyncTo counter (fun (_,r) -> 1)

  return!
    Seq.init (int N) id
    |> Seq.map (fun i -> async {
      let! res = produce payload
      Interlocked.Add(&completed, 1L) |> ignore
      return()
    })
    |> Async.parallelThrottledIgnore parallelism }

Async.RunSynchronously go

sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds
let volumeMB = (N * int64 messageSize) / int64 1000000

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB