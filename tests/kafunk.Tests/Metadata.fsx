﻿#r "bin/Debug/kafunk.dll"
#time "on"

open Kafunk

// Replace this with an initial broker you wish to use.
let conn = Kafka.connHostAndPort "127.0.0.1" 9092

let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  for p in t.partitionMetadata do
    printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId
