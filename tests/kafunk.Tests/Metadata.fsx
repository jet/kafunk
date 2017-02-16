#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk

let conn = Kafka.connHost "localhost"

let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  for p in t.partitionMetadata do
    printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId
