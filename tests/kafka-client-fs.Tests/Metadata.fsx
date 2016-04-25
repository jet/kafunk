#r "bin/release/KafkaFs.dll"
#time "on"

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open KafkaFs

let Log = Log.create __SOURCE_FILE__

let conn = Kafka.connHost "localhost"

let metadata = Kafka.metadata conn (MetadataRequest([||])) |> Async.RunSynchronously

metadata.brokers
|> Seq.iter (fun b ->
  Log.info "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId 
)

metadata.topicMetadata
|> Seq.iter (fun t ->
  Log.info "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  t.partitionMetadata
  |> Seq.iter (fun p ->
    Log.info "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId
  )
)
