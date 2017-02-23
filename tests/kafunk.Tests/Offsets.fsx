#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "guardians-kafka-cluster.qa.jet.com:9092"
let topic = argiDefault 2 "marvel-pi-batman-cart-category-p16"

let conn = Kafka.connHost host

//let offsets = 
//  Offsets.offsets conn topic [] [ Time.EarliestOffset ; Time.LatestOffset ] 1
//  |> Async.RunSynchronously
//
//for kvp in offsets do
//  for (tn,offsets) in kvp.Value.topics do
//    for p in offsets do
//      printfn "time=%i topic=%s p=%i os=%A" kvp.Key tn p.partition p.offsets

let offsets = 
  Offsets.offsetRange conn topic []
  |> Async.RunSynchronously

for kvp in offsets do
  printfn "partition=%i earliest=%i latest=%i" kvp.Key (fst kvp.Value) (snd kvp.Value)
