#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open Refs

Log.MinLevel <- LogLevel.Trace
let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

let connCfg = 
  KafkaConfig.create (
    [KafkaUri.parse host], 
    version=Versions.V_0_10_1, 
    //autoApiVersions=true,
    //version=Versions.V_0_9_0, 
    clientId = "leo"
  )
let conn = Kafka.conn connCfg

//let offsetRange =
//  Offsets.offsetRange conn topic []
//  |> Async.RunSynchronously

//printfn "offset response topics=%A" offsetRange

//for kvp in offsetRange do
//  let p = kvp.Key
//  let e,l = kvp.Value
//  printfn "p=%i earliest=%i latest=%i" p e l

//let ps =
//  offsetRange 
//  |> Seq.choose (fun kvp -> 
//    let p = kvp.Key
//    let e,l = kvp.Value
//    if l - e > 0L then Some (p,e,0L,1000000)
//    else None)
//  |> Seq.truncate 10
//  |> Seq.toArray

//let fetchReq = FetchRequest(-1, 0, 0, [| topic, ps |], 0, 0y)

//let ps = [| (0,0L,0L,10000) ; (1,0L,0L,10000); (2,0L,0L,10000) ; (3,0L,0L,10000) |]
//let ps = [| (0,0L,-1L,10000000) ; (3,0L,-1L,10000000) |]
let ps = [| (3,0L,0L,10000) |]
//let ps = [| (3,0L,0L,100000) ; (0,0L,0L,100000) |]
//let ps = [| (0,25000L,0L,10000) ; (3,0L,0L,10000) |]
let fetchReq = FetchRequest(-1, 100, 0, [| topic, ps |], 10000000, 0y)

let fetchRes = 
  Kafka.fetch conn fetchReq
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,_,logStartOffset,_,mss,ms) in pmds do
    printfn "topic=%s p=%i ec=%i hwm=%i mss=%i mc=%i" tn p ec hmo mss ms.messages.Length
    for x in ms.messages do
      let o,ms,m = x.offset, x.messageSize, x.message
      //printfn "p=%i o=%i size=%i timestamp=%O key=%s value=%s" p o ms (DateTime.FromUnixMilliseconds m.timestamp) (m.key |> Binary.toString) (m.value |> Binary.toString)
      ()

