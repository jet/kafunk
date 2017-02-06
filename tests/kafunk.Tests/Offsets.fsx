#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

let conn = Kafka.connHost host

let offsets = 
  Offsets.offsets conn topic [] [ Time.EarliestOffset ; Time.LatestOffset ] 1
  |> Async.RunSynchronously

for kvp in offsets do
  for (tn,offsets) in kvp.Value.topics do
    for p in offsets do
      printfn "time=%i topic=%s p=%i os=%A" kvp.Key tn p.partition p.offsets