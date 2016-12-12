#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk

let Log = Log.create __SOURCE_FILE__

let host = ""
let topic = ""

let conn = Kafka.connHost host

let offsets = 
  Kafka.Composite.offsets conn topic [ Time.EarliestOffset ; Time.LatestOffset ] 1
  |> Async.RunSynchronously

for kvp in offsets do
  for (tn,offsets) in kvp.Value.topics do
    for p in offsets do
      printfn "time=%i topic=%s partition=%i offsets=%A" kvp.Key tn p.partition p.offsets