#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let host = argiDefault 2 "localhost"

let conn = Kafka.connHost host

let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic_name=%s topic_error_code=%i ps=[%s]" 
            t.topicName t.topicErrorCode
            (t.partitionMetadata 
              |> Seq.sortBy (fun pmd -> pmd.partitionId) 
              |> Seq.map (fun pmd -> sprintf "(p=%i leader=%i)"  pmd.partitionId pmd.leader) 
              |> String.concat ";")
