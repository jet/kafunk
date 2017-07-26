#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let host = "localhost"
let cfg = 
  KafkaConfig.create (
    [KafkaUri.parse host], 
    version=Versions.V_0_10_1)
let conn = Kafka.conn cfg

let versions = Kafka.apiVersions conn |> Async.RunSynchronously

for (apiKey,minv,maxv) in versions.apiVersions do
  printfn "key=%i min=%i max=%i" (int apiKey) minv maxv