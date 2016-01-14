#r "bin/release/KafkaFs.dll"

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


//Log.To.console "*" NLog.LogLevel.Trace
let Log = Log.create __SOURCE_FILE__

let conn = Kafka.connHost "localhost"



let metadata = Kafka.metadata conn (MetadataRequest([||])) |> Async.RunSynchronously

metadata.brokers
|> Seq.iter (fun b ->
  Log.info "broker|host=%s port=%i nodeId=%i" b.host b.nodeId b.port
)

metadata.topicMetadata
|> Seq.iter (fun t ->
  Log.info "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  t.partitionMetadata
  |> Seq.iter (fun p ->
    Log.info "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId
  )
)




//let req = ProduceRequest.ofMessageSet ("test", 0, MessageSet.ofMessage (Message.ofBytes ("hello world"B)))
//
//let res = Kafka.produce conn req |> Async.RunSynchronously
//
//res.topics
//|> Array.iter (fun (tn,offsets) ->
//  Log.info "topic_name=%s" tn
//  offsets
//  |> Array.iter (fun (p,ec,offset) ->
//    Log.info "partition=%i error_code=%i offset=%i" p ec offset 
//  )
//)



//
//let req = FetchRequest.ofTopicPartition ("test", 0, 0L)
//let res = Kafka.fetch conn req |> Async.RunSynchronously
//
//res.topics
//|> Array.iter (fun (tn,ps) ->
//  Log.info "topic_name=%s" tn
//  ps
//  |> Array.iter (fun (p,ec,hwo,mss,ms) ->
//    Log.info "partition=%i error_code=%i high_watermark_offset=%i message_set_size=%i" p ec hwo mss
//    ms.messages
//    |> Array.iter (fun (offset,ms,m) ->
//      Log.info "offset=%i message_size=%i message=%s" offset ms (Message.valueString m)
//    )
//  )
//)
//
//(*
//2015-12-30 22:45:28.4020|INFO|Kafka.fsx|topic_name=test
//2015-12-30 22:45:28.4020|INFO|Kafka.fsx|partition=0 error_code=0 offset=7
//*)






let req = GroupCoordinatorRequest("test-group")
let res = Kafka.groupCoordinator conn req |> Async.RunSynchronously

Log.info "id=%i host=%s port=%i error=%i" res.coordinatorId res.coordinatorHost res.coordinatorPort res.errorCode





let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, [|"test"|], ArraySeg<_>())
let assignmentStrategy : AssignmentStrategy = "range" //roundrobin
let groupProtocols = GroupProtocols([| assignmentStrategy, (toArraySeg consumerProtocolMeta) |])
let joinGroupReq = JoinGroupRequest("test-group", 10000, "", ProtocolType.consumer, groupProtocols)
let joinGroupRes = Kafka.joinGroup conn joinGroupReq |> Async.RunSynchronously

Log.info "error=%i generation_id=%i leader_id=%s member_id=%s" joinGroupRes.errorCode joinGroupRes.generationId joinGroupRes.leaderId joinGroupRes.memberId
joinGroupRes.members.members
|> Seq.iter (fun (mid,metadata) ->
  Log.info "members|member_id=%s" mid
)




let hbReq = HeartbeatRequest("test-group", joinGroupRes.generationId, joinGroupRes.memberId)
let hbRes = Kafka.heartbeat conn hbReq |> Async.RunSynchronously
Log.info "heartbeat error_code=%i" hbRes.errorCode





let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([|"test",[|0|]|]))
let syncReq = SyncGroupRequest("test-group", joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([| joinGroupRes.memberId, (toArraySeg assignment) |]))
let syncRes = Kafka.syncGroup conn syncReq |> Async.RunSynchronously
let (memberAssignment:ConsumerGroupMemberAssignment,_) = read syncRes.memberAssignment

Log.info "error_code=%i version=%i" syncRes.errorCode memberAssignment.version
memberAssignment.partitionAssignment.assignments
|> Seq.iter (fun (tn,ps) ->
  ps
  |> Seq.iter (fun p ->
    Log.info "topic_name=%s partition=%i" tn p
  )
)



let fetchOffset (topic:TopicName, partition:Partition) = async {
  let req = OffsetFetchRequest("test-group", [| topic, [| partition |] |])
  let! res = Kafka.offsetFetch conn req
  let topic,ps = res.topics.[0]
  let (p,offset,metadata,ec) = ps.[0]

  return offset,ec }


let offsetRes,ec = fetchOffset ("test",0) |> Async.RunSynchronously
Log.info "offset=%i error_code=%i" offsetRes ec








(*

TODO

- error handling
- fault tolerance (retry, reconnect)
- consumer groups
- request routing (topic * partition, group coordinator)
- low alloc codec
- benchmark tcp framer
- comppression
- docs
- protocol nuances: compressed fetch responses, no-response requests, etc
- c# adapter
- OSS: internalize marvel, externalize AsyncSeq, project scaffold
- SSL connections

*)