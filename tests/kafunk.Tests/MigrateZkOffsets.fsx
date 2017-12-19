#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost:9092"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "absurd-group"

let go = async {
  
  let connOld = 
    let connCfgOld = KafkaConfig.create ([KafkaUri.parse host], version = Versions.V_0_8_2)
    Kafka.conn connCfgOld
  
  let! metadata = 
    Kafka.metadata connOld (MetadataRequest([|topic|]))

  let partitions = 
    metadata.topicMetadata
    |> Seq.collect (fun tmd -> tmd.partitionMetadata |> Seq.map (fun pmd -> pmd.partitionId))
    |> Seq.toArray

  let! zkOffsets = 
    Kafka.offsetFetch connOld (OffsetFetchRequest(group, [| topic, partitions |]))

  let zkOffsets =
    zkOffsets.topics
    |> Seq.collect (fun (t,ps) -> 
      if t = topic then ps |> Seq.map (fun (p,o,_,_) -> p,o)
      else Seq.empty)
    |> Seq.toArray
      
  Log.info "migrating_offsets|topic=%s offsets=[%s]"
    topic
    (zkOffsets |> Seq.map (fun (p,o) -> sprintf "[p=%i o=%i]" p o) |> String.concat " ; ")

  let conn = Kafka.connHost host

  let! offsetCommitRes =
    Kafka.offsetCommit conn (OffsetCommitRequest(group, -1, "", -1L, [| topic, zkOffsets |> Array.map (fun (p,o) -> p,o,0L,"") |]))

  Log.info "migrated_offsets|topic=%s offsets=[%s]"
    topic
    (zkOffsets |> Seq.map (fun (p,o) -> sprintf "[p=%i o=%i]" p o) |> String.concat " ; ")
  
  return () }

Async.RunSynchronously go
