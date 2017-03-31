#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System

let host = "localhost:9092"
let topic = "test-topic"
let conn = Kafka.connHost host

let offsets =
  Offsets.offsets conn topic [] [Time.EarliestOffset] 1
  |> Async.RunSynchronously

let offsetRes = Map.find Time.EarliestOffset offsets

printfn "offset response topics=%i" offsetRes.topics.Length
for (tn,ps) in offsetRes.topics do
  for p in ps do
    for o in p.offsets do
      printfn "topic=%s partition=%i offset=%i" tn p.partition o

let (tn,ps) = offsetRes.topics.[0]

let fetchReq =
  FetchRequest(-1, 0, 0, [| tn, [| ps.[0].partition, ps.[0].offsets.[0], 20000 |] |])

let fetchRes = 
  Kafka.fetch conn fetchReq
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i hwm=%i message_set_size=%i messages=%i" tn p ec hmo mss ms.messages.Length
    for x in ms.messages do
      let o,ms,m = x.offset, x.messageSize, x.message
      printfn "message offset=%i size=%i message=%s" o ms (m.value |> Binary.toString)

