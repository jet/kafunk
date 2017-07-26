#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open Refs

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

let connCfg = KafkaConfig.create ([KafkaUri.parse host], version=Versions.V_0_10_1)
let conn = Kafka.conn connCfg

let offsetRange =
  Offsets.offsetRange conn topic []
  |> Async.RunSynchronously

printfn "offset response topics=%A" offsetRange

for kvp in offsetRange do
  let p = kvp.Key
  let e,l = kvp.Value
  printfn "p=%i earliest=%i latest=%i" p e l

let ps =
  offsetRange 
  |> Seq.choose (fun kvp -> 
    let p = kvp.Key
    let e,l = kvp.Value
    if l - e > 0L then Some (p,e,1000000)
    else None)
  |> Seq.truncate 10
  |> Seq.toArray

let fetchReq = FetchRequest(-1, 0, 0, [| topic, ps |])

let fetchRes = 
  Kafka.fetch conn fetchReq
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i hwm=%i message_set_size=%i messages=%i" tn p ec hmo mss ms.messages.Length
    for x in ms.messages do
      let o,ms,m = x.offset, x.messageSize, x.message
      printfn "message offset=%i size=%i timestamp=%O key=%s" 
                o ms (DateTime.FromUnixMilliseconds m.timestamp) (m.key |> Binary.toString)

