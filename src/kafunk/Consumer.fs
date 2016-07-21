namespace Kafunk

open System

open Kafunk
open Kafunk.Protocol


/// High-level consumer API.
module Consumer =

  type ConsumerConfig = {
    groupId : GroupId
    topics : TopicName[]
    sessionTimeout : SessionTimeout
    heartbeatFrequency : int32
    autoOffsetReset : AutoOffsetReset
    fetchMinBytes : MinBytes
    fetchMaxWaitMs : MaxWaitTime
    metadataFetchTimeoutMs : int32
    totalBufferMemory : int32
    fetchBuffer : MaxBytes
    clientId : string
    socketReceiveBuffer : int32
    reconnectBackoffMs : int32
    offsetRetentionTime : int64
  }
    with
      static member create (groupId:GroupId, topics:TopicName[]) =
        {
          groupId = groupId
          topics = topics
          sessionTimeout = 10000
          heartbeatFrequency = 4
          autoOffsetReset = AutoOffsetReset.Anything
          fetchMinBytes = 0
          fetchMaxWaitMs = 0
          metadataFetchTimeoutMs = 0
          totalBufferMemory = 10000
          fetchBuffer = 1000
          clientId = Guid.NewGuid().ToString("N")
          socketReceiveBuffer = 1000
          reconnectBackoffMs = 0
          offsetRetentionTime = 0L
        }

  and AutoOffsetReset =
    | Smallest
    | Largest
    | Disable
    | Anything

(*

# Consumer Group Transitions

## Consumer

- Coordinator heartbeat timeout -> close channel, then group coordinator re-discovery.

## Coordinator

- Invalid generation heartbeat -> invalid generation heartbeat response

*)

  /// Possible responses of a consumer group protocol.
  /// These may be received by several concurrent processes: heartbeating, fetching, committing.
  type ConsumerGroupResponse =

    | GroopCoordResponse // start protocol, or retry
    | JoinResponse //
    | SyncResponse
    | FetchResponse

    // TODO: Choice<OK, Error> ?
    // TODO: on error, running consumers must be stopped (by closing stream).
    // Cleanup:
    // - stop heartbeating
    // - close fetched streams
    // -

    /// GroupLoadInProgressCode	14	Yes	The broker returns this error code for
    /// an offset fetch request if it is still loading offsets (after a leader
    /// change for that offsets topic partition), or in response to group
    /// membership requests (such as heartbeats) when group metadata is being
    /// loaded by the coordinator.
    | GroupLoadInProgress

    /// GroupCoordinatorNotAvailableCode	15	Yes	The broker returns this error
    /// code for group coordinator requests, offset commits, and most group
    /// management requests if the offsets topic has not yet been created, or
    /// if the group coordinator is not active.
    | GroupCoordinatorNotAvailable

    /// IllegalGenerationCode	22	 	Returned from group membership requests
    /// (such as heartbeats) when the generation id provided in the request is
    /// not the current generation.
    | IllegalGeneration

    /// InconsistentGroupProtocolCode	23	 	Returned in join group when the
    /// member provides a protocol type or set of protocols which is not
    /// compatible with the current group.
    | InconsistentGroupProtocol

    | InvalidGroupId

    /// UnknownMemberIdCode	25	 	Returned from group requests (offset
    /// commits/fetches, heartbeats, etc) when the memberId is not in the
    /// current generation.
    | UnknownMemberId

    /// InvalidSessionTimeoutCode	26	 	Return in join group when the requested
    /// session timeout is outside of the allowed range on the broker
    | InvalidSessionTimeout

    /// RebalanceInProgressCode	27	 	Returned in heartbeat requests when the
    /// coordinator has begun rebalancing the group. This indicates to the
    /// client that it should rejoin the group.
    | RebalanceInProgress

    | SessionTimeout

    | MetadataChanged

    /// 16	Yes	The broker returns this error code if it receives an offset
    /// fetch or commit request for a group that it is not a coordinator for.
    | NotCoordinatorForGroup

  /// Given a consumer configuration, initiates the consumer group protocol.
  /// Returns an async sequence of states where each state corresponds to a
  /// generation in the group protocol. The state contains streams for the
  /// topics specified in the configuration. Whenever there is a change in
  /// the consumer group, or a failure, the protocol restarts and returns a
  /// new generation once successful. If there are failures surpassing
  /// configured thresholds, the resulting sequence throws an exception.
  let consume (conn:KafkaConn) (cfg:ConsumerConfig) : AsyncSeq<_> = async {

    // domain-specific api

    let _groopCoord =
      Kafka.groupCoordinator conn (GroupCoordinatorRequest(cfg.groupId))
      |> Async.map (fun res ->
        match res.errorCode with
        | ErrorCode.NoError -> ConsumerGroupResponse.GroopCoordResponse
        | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerGroupResponse.GroupCoordinatorNotAvailable
        | ErrorCode.InvalidGroupIdCode -> ConsumerGroupResponse.InvalidGroupId
        | _ -> failwith "")

    // sent to group coordinator
    let _joinGroup2 =
      let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, Binary.empty)
      let assignmentStrategy : AssignmentStrategy = "range" // roundrobin
      let groupProtocols =
        GroupProtocols(
          [| assignmentStrategy, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta |])
      let joinGroupReq = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, "" (* memberId *), ProtocolType.consumer, groupProtocols)
      Kafka.joinGroup conn joinGroupReq
      |> Async.map (fun res ->
        match res.errorCode with
        | ErrorCode.NoError ->
          //if res.members.members.Length > 0 then
          ConsumerGroupResponse.JoinResponse
        | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerGroupResponse.GroupCoordinatorNotAvailable
        | ErrorCode.InconsistentGroupProtocolCode -> ConsumerGroupResponse.InconsistentGroupProtocol
        | ErrorCode.InvalidSessionTimeoutCode -> ConsumerGroupResponse.InvalidSessionTimeout
        | _ -> failwith "")

    // heartbeats: must be sent to group coordinator
    let rec hb (generationId,memberId) = async {
      let req = HeartbeatRequest(cfg.groupId, generationId, memberId)
      let! res = Kafka.heartbeat conn req
      match res.errorCode with
      | ErrorCode.NoError ->
        do! Async.Sleep (cfg.sessionTimeout / cfg.heartbeatFrequency)
        return! hb (generationId,memberId)
      | ErrorCode.IllegalGenerationCode ->
        return ConsumerGroupResponse.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode ->
        return ConsumerGroupResponse.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode ->
        return ConsumerGroupResponse.RebalanceInProgress
      | _ ->
        return ConsumerGroupResponse.SessionTimeout }

    // sent to group coordinator
    let _leaderSyncGroup (generationId,memberId,_members) = async {
      let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([||]))
      let members = [| "" (*memberId*), (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment) |]
      let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment(members))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.NoError -> return ConsumerGroupResponse.SyncResponse
      | ErrorCode.IllegalGenerationCode -> return ConsumerGroupResponse.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode -> return ConsumerGroupResponse.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode -> return ConsumerGroupResponse.RebalanceInProgress
      | _ ->
        return ConsumerGroupResponse.SessionTimeout }

    // sent to group coordinator
    let _followerSyncGroup (generationId,memberId) = async {
      let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([||]))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.NoError -> return ConsumerGroupResponse.SyncResponse
      | ErrorCode.IllegalGenerationCode -> return ConsumerGroupResponse.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode -> return ConsumerGroupResponse.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode -> return ConsumerGroupResponse.RebalanceInProgress
      | _ ->
        return ConsumerGroupResponse.SessionTimeout }

    // sent to group coordinator
    let commitOffset (generationId,memberId) (topic:TopicName, partition:Partition, offset:Offset) = async {
      let req =
        OffsetCommitRequest(
         cfg.groupId, generationId, memberId, cfg.offsetRetentionTime, [| topic, [|partition, offset, null|] |])
      do! Kafka.offsetCommit conn req |> Async.Ignore
      // TODO: check error
      return () }

    let _fetchOffset (topic:TopicName, partition:Partition) = async {
      let req = OffsetFetchRequest(cfg.groupId, [| topic, [| partition |] |])
      let! res = Kafka.offsetFetch conn req
      let _topic,ps = res.topics.[0]
      let (_p,offset,_metadata,_ec) = ps.[0]
      return offset }

    // fetch sent to broker in metadata or coordinator?
    let stream (generationId,memberId) (topic:TopicName, partition:Partition) : AsyncSeq<MessageSet * Async<unit>> =
      let rec go (offset:FetchOffset) = asyncSeq {
        let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [|partition, offset, cfg.fetchBuffer|] |])
        // TODO: wait for state change (kill) signal
        let! res = Kafka.fetch conn req
        // TODO: check error
        let topic,partitions = res.topics.[0]
        let partition,_ec,_hmo,_mss,ms = partitions.[0]
        let nextOffset = MessageSet.nextOffset ms
        let commit = commitOffset (generationId,memberId) (topic, partition, offset)
        let ms = Compression.decompress ms
        yield ms,commit
        yield! go nextOffset }
      // TODO: fetch offset
      go (0L)

    // TODO: period and watch for changes?
    do! Kafka.metadata conn (Metadata.Request(cfg.topics)) |> Async.Ignore

    let rec go () : AsyncSeq<_> = async {
      // start of session
      let! _groupCoord = Kafka.groupCoordinator conn (GroupCoordinatorRequest(cfg.groupId))
      // TODO: send offset commit/fetch requests to groop coord
      let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, Binary.empty)
      let assignmentStrategy : AssignmentStrategy = "range" //roundrobin
      let groupProtocols =
        GroupProtocols(
          [| assignmentStrategy, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta |])
      let memberId : MemberId = "" // assigned by coordinator
      let joinGroupReq = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, memberId, ProtocolType.consumer, groupProtocols)
      let! joinGroupRes = Kafka.joinGroup conn joinGroupReq
      // TODO: or failure
      let generationId = joinGroupRes.generationId
      let memberId = joinGroupRes.memberId
      // is leader?
      if (joinGroupRes.leaderId = joinGroupRes.memberId) then
        // determine assignments
        // send sync request
        let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([||]))
        let groupAssignment =
          GroupAssignment(
            [| "" (*memberId*), toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment |])
        let syncReq = SyncGroupRequest(cfg.groupId, generationId, memberId, groupAssignment)
        let! _syncRes = Kafka.syncGroup conn syncReq
        // TODO: get metadata?
        return failwith ""
      else
        let syncReq = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([||]))
        let! syncRes = Kafka.syncGroup conn syncReq
        let (memberAssignment:ConsumerGroupMemberAssignment,_) = ConsumerGroupMemberAssignment.read syncRes.memberAssignment
        // the partitions assigned to this member
        let topicPartitions = memberAssignment.partitionAssignment.assignments
        // the topic,partition,stream combinations assigned to this member
        let topicStreams =
          topicPartitions
          |> Array.collect (fun (t,ps) -> ps |> Array.map (fun p -> t,p))
          |> Array.map (fun (t,p) -> t,p, stream (generationId,memberId) (t,p))
        // return and wait for errors, which will stop all child streams
        // and return a new state
        return Cons ( (generationId,memberId,topicStreams), go ())
      }
    return! go ()
  }