#r "bin/Debug/kafunk.dll"

open Kafunk
open Kafunk.Protocol

//Log.To.console "*" NLog.LogLevel.Trace
let log = Log.create __SOURCE_FILE__

// Replace this with an initial broker you wish to use.
let conn = Kafka.connHostAndPort "127.0.0.1" 9092

module MetadataDemo =

  let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously

  let logBroker (b:Broker) =
    log.info "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

  let logPartition (t:TopicMetadata) (p:PartitionMetadata) =
    log.info "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId

  let logTopic (t:TopicMetadata) =
    log.info "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode

  metadata.brokers
  |> Seq.iter logBroker

  metadata.topicMetadata
  |> Seq.iter (fun topic ->
    logTopic topic
    topic.partitionMetadata
    |> Seq.iter (logPartition topic))

module ProducerDemo =
  let req = ProduceRequest.ofMessageSet "topicA" 0 (MessageSet.ofMessage (Message.ofBytes "hello world"B (Some "key"B))) None None
  let res = Kafka.produce conn req |> Async.RunSynchronously

  res.topics
  |> Array.iter (fun (tn, offsets) ->
    log.info "topic_name=%s" tn
    offsets
    |> Array.iter (fun (p, ec, offset) ->
      log.info "partition=%i error_code=%i offset=%i" p ec offset))

module ConsumerDemo =

  let req = FetchRequest.ofTopicPartition "test1" 0 0L 3600 0 100024
  let res = Kafka.fetch conn req |> Async.RunSynchronously

  res.topics
  |> Array.iter (fun (tn, ps) ->
    log.info "topic_name=%s" tn
    ps
    |> Array.iter (fun (p, ec, hwo, mss, ms) ->
      log.info "partition=%i error_code=%i high_watermark_offset=%i message_set_size=%i" p ec hwo mss
      ms.messages
      |> Array.iter (fun (offset, ms, m) ->
        log.info "offset=%i message_size=%i message=%s" offset ms (Message.valueString m))))

module ConsumerGroupDemo =

  let req = GroupCoordinatorRequest("test-group")
  let res = Kafka.groupCoordinator conn req |> Async.RunSynchronously

  log.info "id=%i host=%s port=%i error=%i" res.coordinatorId res.coordinatorHost res.coordinatorPort res.errorCode

  let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, [|"test"|], Buffer.empty)
  let assignmentStrategy : AssignmentStrategy = "range" //roundrobin
  let groupProtocols = GroupProtocols([| assignmentStrategy, (toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta) |])
  let joinGroupReq = JoinGroup.Request("test-group", 10000, "", ProtocolType.consumer, groupProtocols)
  let joinGroupRes = Kafka.joinGroup conn joinGroupReq |> Async.RunSynchronously

  log.info "error=%i generation_id=%i leader_id=%s member_id=%s" joinGroupRes.errorCode joinGroupRes.generationId joinGroupRes.leaderId joinGroupRes.memberId
  joinGroupRes.members.members
  |> Seq.iter (fun (mid, metadata) -> log.info "members|member_id=%s" mid)

  let hbReq = HeartbeatRequest("test-group", joinGroupRes.generationId, joinGroupRes.memberId)
  let hbRes = Kafka.heartbeat conn hbReq |> Async.RunSynchronously
  log.info "heartbeat error_code=%i" hbRes.errorCode

  let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([|"test", [|0|]|]))
  let syncReq = SyncGroupRequest("test-group", joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([| joinGroupRes.memberId, (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment) |]))
  let syncRes = Kafka.syncGroup conn syncReq |> Async.RunSynchronously
  let (memberAssignment, _) = ConsumerGroupMemberAssignment.read syncRes.memberAssignment

  log.info "error_code=%i version=%i" syncRes.errorCode memberAssignment.version
  memberAssignment.partitionAssignment.assignments
  |> Seq.iter (fun (tn, ps) ->
    ps
    |> Seq.iter (fun p ->
      log.info "topic_name=%s partition=%i" tn p))

  let fetchOffset (topic:TopicName, partition:Partition) = async {
    let req = OffsetFetchRequest("test-group", [| topic, [|partition|] |])
    let! res = Kafka.offsetFetch conn req
    let topic, ps = res.topics.[0]
    let (p, offset, metadata, ec) = ps.[0]
    return offset, ec }

  let offsetRes, ec = fetchOffset ("test",0) |> Async.RunSynchronously
  log.info "offset=%i error_code=%i" offsetRes ec
