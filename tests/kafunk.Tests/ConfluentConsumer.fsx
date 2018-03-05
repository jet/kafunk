#r "bin/release/confluent.kafka.dll"
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
open FSharp.Control

let dict (xs:seq<'a * 'b>) =
  let d = new Dictionary<_, _>()
  for (k,v) in xs do
    d.Add (k,v)
  d

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "existential-group"

Log.info "running_consumer|host=%s topic=%s group=%s" host topic group

let config =
  [ 
      
    "bootstrap.servers", box host 
    //"linger.ms", box 100
    "group.id", box group
    //"max.in.flight.requests.per.connection", box 1
    "default.topic.config", box <| (dict ["auto.offset.reset", box "latest"])
    "enable.auto.commit", box false
    "fetch.message.max.bytes", box 1000

  ] |> dict

let go = async {
  
  //let consumer = new Consumer (config)

  let consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8))
  
  consumer.OnLog 
  |> Event.add (fun m -> Log.info "log|%O" m)
  
  consumer.OnError 
  |> Event.add (fun m -> Log.info "error|%O" m)
  
  consumer.OnConsumeError 
  |> Event.add (fun m -> Log.info "consumer_error|%O" m)
  
  consumer.OnPartitionsAssigned 
  |> Event.add (fun m -> Log.info "partitions_assigned|%O" (m |> Seq.map (fun x -> sprintf "p=%i" x.Partition) |> String.concat ";"))

  consumer.OnPartitionsRevoked 
  |> Event.add (fun m -> Log.info "partitions_revoked|%O" (m |> Seq.map (fun x -> sprintf "p=%i" x.Partition) |> String.concat ";"))

  consumer.OnPartitionEOF 
  |> Event.add (fun m -> Log.info "eof|%O" m.Partition)
 
  consumer.Assign([ TopicPartition(topic, 0) ])
  //consumer.Subscribe (topic)

  let md = consumer.GetMetadata(true)
  Log.info "metadata|%A" md.Topics

  let handle (_:Message) = async {
    Log.info "handing message"
    return () }

  use counter = Metrics.counter Log 5000

  let handle = 
    handle
    |> Metrics.throughputAsyncTo counter (fun _ -> 1)

  while true do
    let mutable m = Unchecked.defaultof<_>
    if consumer.Consume(&m, 1000) then
      Log.info "handing message"
    else
      Log.info "skipped"

  //let! _ = Async.StartChild (async {
  //  while true do
  //    consumer.Poll (1000) })

  //do!
  //  consumer.OnMessage
  //  |> AsyncSeq.ofObservableBuffered
  //  |> AsyncSeq.iterAsync handle

  return ()
}

try Async.RunSynchronously go
with ex -> Log.error "ERROR|%O" ex

printfn "DONE"