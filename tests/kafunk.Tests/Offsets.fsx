#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

//let connCfg = KafkaConfig.create ([KafkaUri.parse host], version=Versions.V_0_10_1)
//let conn = Kafka.conn connCfg

let go = async {

  let rebalance = asyncSeq { 
    yield [| 0,0L ; 1,0L |]
    do! Async.Sleep 10000
    Log.info "rebalanced"
    yield [| 0,10L ; 1,10L ; 2,0L |] }
  
  let commit (ps:(Partition * Offset)[]) = async {
    Log.info "committing|%A" ps }
  
  let interval = System.TimeSpan.FromSeconds 1.0

  let! commitQueue,commitTask = Offsets.periodicOffsetCommitter interval rebalance commit

  let! _ = Async.StartChild commitTask

  let p0 = ref 1L
  let p1 = ref 1L

  while true do
    do! Async.Sleep 5000
    let offsets = [| (0,!p0) ; (1,!p1) |]
    Log.info "queueing|%A" offsets
    commitQueue offsets
    p0 := !p0 + 1L
    p1 := !p1 + 1L

  return ()
}

Async.RunSynchronously go


//let offsets = 
//  Offsets.offsets conn topic [] [ Time.EarliestOffset ; Time.LatestOffset ] 1
//  |> Async.RunSynchronously
//
//for kvp in offsets do
//  for (tn,offsets) in kvp.Value.topics do
//    for p in offsets do
//      printfn "time=%i topic=%s p=%i os=%A" kvp.Key tn p.partition p.offsets

//let offsets = 
//  Offsets.offsetRange conn topic []
//  |> Async.RunSynchronously

//for kvp in offsets do
//  let e,l = kvp.Value
//  let c = l - e
//  printfn "partition=%i earliest=%i latest=%i count=%i" kvp.Key e l c
