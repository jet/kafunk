#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open Refs

//Log.MinLevel <- LogLevel.Trace
let Log = Log.create __SOURCE_FILE__
let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

let tcpConfig = 
  ChanConfig.create (
    requestTimeout = TimeSpan.FromSeconds 60.0,
    receiveBufferSize = 8192 * 50,
    sendBufferSize = 8192 * 50,
    connectRetryPolicy = ChanConfig.DefaultConnectRetryPolicy,
    requestRetryPolicy = ChanConfig.DefaultRequestRetryPolicy)

let connCfg = 
  KafkaConfig.create (
    [KafkaUri.parse host], 
    tcpConfig = tcpConfig,
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
//let ps = [| (3,0L,0L,10000) |]
//let ps = [| (3,0L,0L,100000) ; (0,0L,0L,100000) |]
//let ps = [| (0,25000L,0L,10000) ; (3,0L,0L,10000) |]
//let fetchReq = FetchRequest(-1, 100, 0, [| topic, ps |], 10000000, 0y)

//let fetchRes = 
//  Kafka.fetch conn fetchReq
//  |> Async.RunSynchronously

//for (tn,pmds) in fetchRes.topics do
//  for p in pmds do    
//    let ec = p.errorCode
//    let hmo = p.highWatermarkOffset
//    let mss = p.messageSetSize
//    let ms = p.messageSet
//    let p = p.partition
//    printfn "topic=%s p=%i ec=%i hwm=%i mss=%i mc=%i" tn p ec hmo mss ms.messages.Length
//    for x in ms.messages do
//      let o,ms,m = x.offset, x.messageSize, x.message
//      //printfn "p=%i o=%i size=%i timestamp=%O key=%s value=%s" p o ms (DateTime.FromUnixMilliseconds m.timestamp) (m.key |> Binary.toString) (m.value |> Binary.toString)
//      ()

let go = async {

  let! offsetRange = Offsets.offsetRange conn topic []

  let maxBytesTotal = 10000000
  let maxBytesPartition = 10000000

  let rec fetch p o = async {    
    let ps = [| (p,o,0L,maxBytesPartition) |]
    let req = FetchRequest(-1, 100, 1, [| topic, ps |], maxBytesTotal, 0y)
    let! res = Kafka.fetch conn req
    let (_,ps) = res.topics.[0]
    let p = ps.[0]
    let count = p.messageSet.messages.Length
    if count = 0 then return (None,0) else
    let lastOffset = p.messageSet.messages.[count - 1].offset
    let nextOffset = lastOffset + 1L
    return (Some nextOffset,count) }

  use counter = Metrics.counter Log 5000

  let fetch = 
    fetch
    |> Metrics.throughputAsync2To counter (fun (_,_,(_,count)) -> count)  

  let rec fetchProc p o = async {
    let! (nextOffset,count) = fetch p o
    match nextOffset with
    | None -> return ()
    | Some next -> return! fetchProc p next }

  return!
    offsetRange
    |> Map.toSeq
    |> Seq.map (fun (p,(o,_)) -> fetchProc p o)
    |> Async.Parallel
    |> Async.Ignore }

try Async.RunSynchronously go
with ex -> Log.error "%O" ex