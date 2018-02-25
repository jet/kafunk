namespace Kafunk

open FSharp.Control
open System
open System.Threading
open Kafunk

/// The consumer group protocol.
module ConsumerGroup =

  let private Log = Log.create "Kafunk.ConsumerGroup"

  /// A consumer groups assignment stratgey.
  type AssignmentStrategy = {
            
    /// Given the configured topic name, returns metadata for the consumer group protocol.
    metadata : GroupMember -> TopicName[] -> Async<ConsumerGroupProtocolMetadata>
      
    /// Assigns members to partitions given a set of available topic partitions and members.
    assign : GroupMember ->
             (GroupMemberState option) -> 
             (TopicName * Partition[])[] -> 
             (MemberId * ConsumerGroupProtocolMetadata)[] ->
             Async<(MemberId * ConsumerGroupMemberAssignment)[]>
  }

  /// Decodes member state from the MemberAssignment field in SyncGroupResponse.
  let decodeMemberAssignments (memberAssignment:MemberAssignment) =
      
    let assignment,_ = ConsumerGroupMemberAssignment.read memberAssignment
          
    let assignments = 
      assignment.partitionAssignment.assignments
      |> Seq.groupBy fst
      |> Seq.map (fun (t,ps) -> t, ps |> Seq.collect snd |> Seq.toArray)
      |> Seq.toArray

    assignments

  /// Decodes member state from the MemberAssignment field in SyncGroupResponse.
  let internal decodeMemberAssignment (memberAssignment:MemberAssignment) =
    // TODO: consider support for multi-topic subscriptions
    let t,ps = (decodeMemberAssignments memberAssignment).[0]
    t,ps
  
  let private toArraySeg size write x =
    let size = size x
    let buf = Binary.zeros size
    buf |> write x |> ignore
    buf

  /// Creates an instances of the consumer groups protocol given the specified assignment strategies.
  let create (strategies:(AssignmentStrategyName * AssignmentStrategy)[]) (topicSubscription:TopicName[]) =
      
    let strategies = Map.ofArray strategies

    let protocols (gm:GroupMember) (_:GroupMemberState option) = async {
      return!
        strategies
        |> Seq.map (fun kvp -> async {
          let startegyName = kvp.Key
          let strategy = kvp.Value
          let! metadata = strategy.metadata gm topicSubscription
          return startegyName, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write metadata })
        |> Async.Parallel }

    let assign 
      (gm:GroupMember) (prevState:GroupMemberState option) (startegyName:AssignmentStrategyName) 
      (members:(MemberId * MemberMetadata)[]) : Async<(MemberId * ProtocolMetadata)[]> = async {

      let members = 
        members
        |> Array.map (fun (memberId,meta) ->
          let meta,_ = ConsumerGroupProtocolMetadata.read meta
          memberId,meta)
        
      let topics = 
        members 
        |> Seq.collect (fun (_,meta) -> meta.subscription) 
        |> Seq.distinct
        |> Seq.toArray

      let! topicPartitions = gm.conn.GetMetadata topics
        
      let topicPartitions = 
        topicPartitions
        |> Map.onlyKeys topics
        |> Map.toArray

      let assignmentStrategy = Map.find startegyName strategies

      let! memberAssignments = assignmentStrategy.assign gm prevState topicPartitions members

      let memberAssignments =
        memberAssignments
        |> Array.map (fun (memberId,assignment) ->
          memberId, (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment))
        
      return memberAssignments }
        
    { protocolType = ProtocolType.consumer ; protocols = protocols ; assign = assign }

  /// Built-in assignment stratgies.
  module AssignmentStratgies =

    let internal RangeAssign (ps:Partition[]) (ms:MemberId[]) =
      let asmts =
        ms
        |> Array.map (fun m -> m, ResizeArray<_>()) 
      let f = float ms.Length / float ps.Length
      for i in [0..ps.Length - 1] do
        let j = int (floor (float i * f))
        (snd asmts.[j]).Add ps.[i]
      asmts
      |> Array.map (fun (m,ps) -> m, ps.ToArray())

    /// The range consumer group assignment startegy.
    let Range =
      
      let metadata (_:GroupMember) (topicSubscription:TopicName[]) = async {
        let version = 0s
        let userData = Binary.empty
        return ConsumerGroupProtocolMetadata(version, topicSubscription, userData) }

      let assign 
        (gm:GroupMember) (_:GroupMemberState option) (topicPartitions:(TopicName * Partition[])[]) (members:(MemberId * ConsumerGroupProtocolMetadata)[]) = 
      
        let memberAssignments =
          
          let membersByTopic =
            members
            |> Seq.collect (fun (mid,meta) -> meta.subscription |> Seq.map (fun t -> t,mid))
            |> Seq.groupBy fst
            |> Seq.map (fun (t,xs) -> t, xs |> Seq.map snd |> Seq.toArray)
            |> Map.ofSeq
          
          let members = Map.ofSeq members
          
          (Map.ofArray topicPartitions, membersByTopic)
          ||> Map.mergeChoice (fun t -> function
            | Choice2Of3 _ | Choice3Of3 _ -> failwith "invalid state"
            | Choice1Of3 (ps,memberIds) ->
              RangeAssign ps memberIds
              |> Array.map (fun (mid,ps) -> mid,t,ps))
          |> Map.toSeq
          |> Seq.collect snd
          |> Seq.groupBy (fun (mid,_,_) -> mid)
          |> Seq.map (fun (mid,xs) -> mid, Map.find mid members, xs |> Seq.map (fun (_,t,ps) -> t,ps) |> Seq.toArray)
          |> Seq.toArray

        let memberAssignmentsStr =
          memberAssignments
          |> Seq.map (fun (memberId,meta,topicPartitions) -> 
            let str = 
              topicPartitions 
              |> Seq.map (fun (t,ps) -> sprintf "[topic=%s partitions=%s]" t (Printers.partitions ps))
              |> String.concat " ; "
            sprintf "[member_id=%s user_data=%s assignments=%s]" memberId (Binary.toString meta.userData) str)
          |> String.concat " ; "
        Log.trace "leader_determined_member_assignments|conn_id=%s group_id=%s %s" 
          gm.conn.Config.connId gm.config.groupId memberAssignmentsStr

        let memberAssignments =
          memberAssignments
          |> Array.map (fun (memberId,meta,tps) ->
            let userData = meta.userData
            let version = meta.version
            let assignment = ConsumerGroupMemberAssignment(version, PartitionAssignment(tps), userData)
            memberId, assignment)

        async.Return memberAssignments

      { assign = assign ; metadata = metadata }


/// Kafka consumer configuration.
type ConsumerConfig = {
  
  /// The consumer group id shared by consumers in the group.
  groupId : GroupId
  
  /// The topic to consume.
  topic : TopicName
  
  /// The session timeout period, in milliseconds, such that if no heartbeats are received within the
  /// period, a consumer is ejected from the consumer group.
  sessionTimeout : SessionTimeout
  
  /// The time during which a consumer must rejoin a group after a rebalance.
  /// If the consumer doesn't rejoin within this time, it will be ejected.
  /// Supported in v0.10.1.
  rebalanceTimeout : RebalanceTimeout

  /// The number of times to send heartbeats within a session timeout period.
  heartbeatFrequency : int32
  
  /// The minimum bytes to buffer server side for a fetch request.
  /// 0 to return immediately.
  fetchMinBytes : MinBytes

  /// The maximum time to wait for a fetch request to return sufficient data.
  fetchMaxWaitMs : MaxWaitTime
  
  /// The maximum bytes to return as part of a partition for a fetch request.
  fetchMaxBytes : MaxBytes
  
  /// Offset retention time.
  offsetRetentionTime : RetentionTime
  
  /// The poll policy to employ when the end of the topic is reached.
  endOfTopicPollPolicy : RetryPolicy
  
  /// The action to take when the consumer doesn't have offsets at the group coordinator, or
  /// if out of range offsets are requested.
  autoOffsetReset : AutoOffsetReset

  /// The size of the per-partition fetch buffer in terms of message set count.
  /// When at capacity, fetching stops until the buffer is drained.
  fetchBufferSize : int

  /// The consumer group assignment strategies to use.
  /// The group coordinator ensures that all members support the same strategy.
  /// When multiple stratgies are supported by all members, the first one in the list is selected.
  assignmentStrategies : (AssignmentStrategyName * ConsumerGroup.AssignmentStrategy)[]

  /// Specifies whether CRC of incoming messages is verified.
  checkCrc : bool

} with
    
    /// Gets the default session timeout = 30000.
    static member DefaultSessionTimeout = 30000

    /// Gets the default rebalance timeout = 10000.
    static member DefaultRebalanceTimeout = 10000

    /// Gets the default heartbeat frequency = 3.
    static member DefaultHeartbeatFrequency = 3

    /// Gets the default fetch min bytes = 1.
    static member DefaultFetchMinBytes = 1
    
    /// Gets the default fetch max wait = 500.
    static member DefaultFetchMaxWait = 500

    /// Gets the default fetch max bytes = 1048576.
    static member DefaultFetchMaxBytes = 1048576

    /// Gets the default offset retention time = -1.
    static member DefaultOffsetRetentionTime = -1L

    /// Gets the default end of topic poll policy = RetryPolicy.constantMs 10000.
    static member DefaultEndOfTopicPollPolicy = RetryPolicy.constantMs 10000

    /// Gets the default offset reset action = AutoOffsetResetAction.StartFromPreviousCommittedOffsets.
    static member DefaultAutoOffsetReset = AutoOffsetReset.TryStartFromCommittedOffsets

    /// Gets the default fetch buffer size = 1.
    static member DefaultFetchBufferSize = 1

    /// Gets the default fetch buffer size = [| "range", ConsumerGroup.AssignmentStratgies.Range |].
    static member DefaultAssignmentStrategies = [| "range", ConsumerGroup.AssignmentStratgies.Range |]

    /// Gets the default value for check crc = true.
    static member DefaultCheckCrc = true

    /// Creates a consumer configuration.
    static member create 
      (groupId:GroupId, topic:TopicName, ?fetchMaxBytes, ?sessionTimeout, ?rebalanceTimeout,
          ?heartbeatFrequency, ?offsetRetentionTime, ?fetchMinBytes, ?fetchMaxWaitMs, ?endOfTopicPollPolicy, ?autoOffsetReset, 
          ?fetchBufferSize, ?assignmentStrategies, ?checkCrc) =
      {
        groupId = groupId
        topic = topic
        sessionTimeout = defaultArg sessionTimeout ConsumerConfig.DefaultSessionTimeout
        rebalanceTimeout = defaultArg rebalanceTimeout ConsumerConfig.DefaultRebalanceTimeout
        heartbeatFrequency = defaultArg heartbeatFrequency ConsumerConfig.DefaultHeartbeatFrequency
        fetchMinBytes = defaultArg fetchMinBytes ConsumerConfig.DefaultFetchMinBytes
        fetchMaxWaitMs = defaultArg fetchMaxWaitMs ConsumerConfig.DefaultFetchMaxWait
        fetchMaxBytes = defaultArg fetchMaxBytes ConsumerConfig.DefaultFetchMaxBytes
        offsetRetentionTime = defaultArg offsetRetentionTime ConsumerConfig.DefaultOffsetRetentionTime
        endOfTopicPollPolicy = defaultArg endOfTopicPollPolicy ConsumerConfig.DefaultEndOfTopicPollPolicy
        autoOffsetReset = defaultArg autoOffsetReset ConsumerConfig.DefaultAutoOffsetReset
        fetchBufferSize = defaultArg fetchBufferSize ConsumerConfig.DefaultFetchBufferSize
        assignmentStrategies =
          match assignmentStrategies with
          | None -> ConsumerConfig.DefaultAssignmentStrategies
          | Some xs -> xs
        checkCrc = defaultArg checkCrc ConsumerConfig.DefaultCheckCrc
      }

/// The action to take when the consumer attempts to fetch an offset which is out of range or
/// if the group coordinator does not have offsets for the assigned partitions.
and AutoOffsetReset =

  /// tries to start from the previously committed offsets, if available and within range, otherwises stops with an exception.
  | TryStartFromCommittedOffsets

  /// Stop the consumer, raising an exception.
  | Stop
  
  /// Starts from a specified Time value such as Time.EarliestOffset or Time.EarliestOffset.
  | StartFromTime of Time

  
/// State corresponding to a single generation of the consumer group protocol.
type ConsumerState = {
  
  /// The consumer group generation.
  generationId : GenerationId

  /// The consumer's member id.
  memberId : MemberId

  /// The leader of the generation.
  leaderId : LeaderId

  /// The members of the consumer group.
  /// Available only to the leader.
  members : (MemberId * ProtocolMetadata)[]

  /// The assignment strategy selected by the group coordinator.
  assignmentStrategy : AssignmentStrategyName

  /// The selected protocol.
  protocolName : ProtocolName

  /// The partitions assigned to this consumer.
  assignments : Partition[]

  /// Cancelled when the generation is closed.
  closed : CancellationToken

}

/// A consumer.
type Consumer = private {
  conn : KafkaConn
  config : ConsumerConfig
  groupMember : GroupMember
}

/// A set of messages for a consumer from an individual topic-partition.
type ConsumerMessageSet =
  struct
    
    /// The topic.
    val topic : TopicName
    
    /// The partition.
    val partition : Partition
    
    /// The message set.
    val messageSet : MessageSet
    
    /// The last offset in the topic-partition.
    val highWatermarkOffset : HighwaterMarkOffset
    
    new (t,p,ms,hwmo) = { topic = t ; partition = p ; messageSet = ms ; highWatermarkOffset = hwmo }

  end
  with 
    
    /// Returns the first offset in the message set.
    static member firstOffset (ms:ConsumerMessageSet) =
      MessageSet.firstOffset ms.messageSet
    
    /// Returns the last offset in the message set.
    static member lastOffset (ms:ConsumerMessageSet) =
      MessageSet.lastOffset ms.messageSet

    /// Returns the timestamp of the first message.
    static member firstTimestamp (ms:ConsumerMessageSet) =
      MessageSet.firstTimestamp ms.messageSet
    
    /// Returns the offset to commit when this message set has been consumed.
    static member commitOffset (ms:ConsumerMessageSet) =
      MessageSet.lastOffset ms.messageSet + 1L

    /// Returns the partition-offset pair to commit when this message set has been consumed.
    static member commitPartitionOffsets (ms:ConsumerMessageSet) =
      [| ms.partition, ConsumerMessageSet.commitOffset ms |]

    /// Returns the lag = high watermark offset - last offset - 1
    static member lag (ms:ConsumerMessageSet) =
      ms.highWatermarkOffset - (ConsumerMessageSet.lastOffset ms) - 1L

    /// Returns the sum of the sizes of all messages in the message set.
    static member size (ms:ConsumerMessageSet) =
      ms.messageSet.messages |> Seq.sumBy (fun x -> x.messageSize)

    /// Returns an array of all messages in the message set.
    static member messages (ms:ConsumerMessageSet) =
      ms.messageSet.messages |> Array.map (fun x -> x.message)

/// High-level consumer API.
[<Compile(Module)>]
module Consumer =

  let private Log = Log.create "Kafunk.Consumer"

  /// Gets the configuration for the consumer.
  let configuration (c:Consumer) = 
    c.config

  /// Explicitly commits offsets to a consumer group.
  /// Note that consumers only fetch these offsets when first joining a group or when rejoining.
  let commitOffsets (c:Consumer) (offsets:(Partition * Offset)[]) = async {
    //Log.trace "committing_offsets|group_id=%s topic=%s offsets=%s" c.config.groupId c.config.topic (Printers.partitionOffsetPairs offsets)
    let! state = Group.stateInternal c.groupMember
    let conn = c.conn
    let cfg = c.config
    let topic = cfg.topic
    return!
      Group.tryAsync
        (state)
        (ignore)
        (async {
          let req = 
            OffsetCommitRequest(
              cfg.groupId, state.state.generationId, state.state.memberId, cfg.offsetRetentionTime, [| topic, offsets |> Array.map (fun (p,o) -> p,o,0L,"") |])
          let! res = Kafka.offsetCommit conn req |> Async.Catch
          match res with
          | Success res ->
            if res.topics.Length = 0 then
              Log.error "offset_committ_failed|group_id=%s member_id=%s generation_id=%i topic=%s offsets=%s" 
                cfg.groupId state.state.memberId state.state.generationId topic (Printers.partitionOffsetPairs offsets)
              return failwith "offset commit failed!"
            let errors =
              res.topics
              |> Seq.collect (fun (_,ps) ->
                ps
                |> Seq.choose (fun (p,ec) ->
                  match ec with
                  | ErrorCode.NoError -> None
                  | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode 
                  | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode -> Some (p,ec)
                  | _ -> failwithf "unsupported commit offset error_code=%i" ec))
            if not (Seq.isEmpty errors) then
              Log.warn "commit_offset_errors|group_id=%s topic=%s errors=%s" 
                cfg.groupId topic (Printers.partitionErrorCodePairs errors)
              do! Group.leaveAndRejoin c.groupMember state (Seq.nth 0 errors |> snd)
              return ()
            else
              Log.info "committed_offsets|conn_id=%s group_id=%s topic=%s offsets=%s" 
                c.conn.Config.connId c.config.groupId topic (Printers.partitionOffsetPairs offsets)
              return ()
          | Failure ex ->
            Log.warn "commit_offset_exception|conn_id=%s group_id=%s generation_id=%i error=%O" 
              c.conn.Config.connId cfg.groupId state.state.generationId ex
            do! Group.leaveInternal c.groupMember state
            //return raise ex }) }
            return () }) }

  /// Explicitly commits offsets to a consumer group, to a specific offset time.
  /// Note that consumers only fetch these offsets when first joining a group or when rejoining.
  let commitOffsetsToTime (c:Consumer) (time:Time) = async {
    let! offsets = Offsets.offsets c.conn c.config.topic [] [time] 1
    let offsetRes = Map.find time offsets
    let os =
      offsetRes.topics
      |> Seq.collect (fun (_t,ps) -> ps |> Seq.map (fun p -> p.partition, p.offsets.[0]))
      |> Seq.toArray
    return! commitOffsets c os }

  /// Returns the committed partition-offset pairs for the specified topic partitions in a consumer group.
  /// Passing an empty array returns offset information for all topics and partitions.
  /// Passing a topic and an empty array of partitions returns all partitions for that topic.
  let fetchOffsets (conn:KafkaConn) (groupId:GroupId) (topics:(TopicName * Partition[])[]) : Async<(TopicName * (Partition * Offset)[])[]> = async {
    let! topics =
      topics 
      |> Array.map(fun (t, ps) -> async {
        let! partitions =
          match ps |> Array.isEmpty with
          | true ->
            async {
              let! metaData = conn.GetMetadata [|t|]
              return metaData.Item t }
          | false -> 
            async { return ps }
        return t,partitions })
      |> Async.Parallel
    let req = OffsetFetchRequest(groupId, topics)
    let! res = Kafka.offsetFetch conn req
    let oks,errors =
      res.topics
      |> Seq.collect (fun (t,ps) ->
        ps
        |> Seq.map (fun (p,o,_md,ec) ->
          match ec with
          | ErrorCode.NoError ->
            Choice1Of2 (t,p,o)
          | _ ->
            Choice2Of2 (t,p,o,ec)))
        |> Seq.partitionChoices
    if errors.Length > 0 then
      Log.error "fetch_offset_errors|errors=%A" errors
      return failwithf "fetch_offset_errors|errors=%A" errors
    let oks = 
      oks 
      |> Seq.groupBy (fun (t,_,_) -> t)
      |> Seq.map (fun (t,xs) -> t, xs |> Seq.map (fun (_,p,o) -> p,o) |> Seq.toArray)
      |> Seq.toArray
    return oks }

  /// Fetches the starting offset for the specified topic * partitions.
  /// If consumer managed offsets are not available, returns topic-wide offsets.
  let private fetchOffsetsFallback (c:Consumer) (topic:TopicName) (partitions:Partition[]) : Async<(Partition * Offset)[]> = async {
    let conn = c.conn
    let cfg = c.config
    let req = OffsetFetchRequest(cfg.groupId, [| topic, partitions |])
    let! res = Kafka.offsetFetch conn req |> Async.Catch
    match res with
    | Success res ->
      
      let oks,missing,errors =
        res.topics
        |> Seq.collect (fun (t,ps) ->
          ps
          |> Seq.map (fun (p,o,_md,ec) ->
            match ec with
            | ErrorCode.NoError ->
              if o = -1L then Choice2Of3 (p,o)
              else Choice1Of3 (p,o)
            | _ ->
              Choice3Of3 (t,p,o,ec)))
          |> Seq.partitionChoices3

      if errors.Length > 0 then
        Log.error "fetch_offset_errors|errors=%A" errors
        return failwithf "fetch_offset_errors|errors=%A" errors
      
      if missing.Length > 0 then
        Log.warn "offsets_not_available_at_group_coordinator|group_id=%s topic=%s missing_offset_partitions=[%s]" 
          cfg.groupId topic (Printers.partitions (missing |> Array.map fst))
        match cfg.autoOffsetReset with
        | AutoOffsetReset.Stop | AutoOffsetReset.TryStartFromCommittedOffsets ->
          return 
            failwithf "offsets_not_available_at_group_coordinator|group_id=%s topic=%s missing_offset_partitions=[%s]" 
              cfg.groupId topic (Printers.partitions (missing |> Array.map fst))

        | AutoOffsetReset.StartFromTime t ->
          let offsetReq = OffsetRequest(-1, [| topic, missing |> Array.map (fun (p,_) -> p, t, 1) |])
          let! offsetRes = Kafka.offset conn offsetReq
          // TODO: error check
          let oks' = 
            offsetRes.topics
            |> Seq.collect (fun (_tn,ps) -> ps)
            |> Seq.map (fun p -> p.partition, p.offsets.[0])
            |> Seq.distinct
            |> Seq.toArray
          return Array.append oks oks'
      else
        return oks

    | Failure ex ->
      Log.error "fetch_offset_exception|error=%O" ex
      return raise ex }
  
  /// Creates a participant in the consumer groups protocol and joins the group.
  let createAsync (conn:KafkaConn) (cfg:ConsumerConfig) = async {
    let groupProtocol = ConsumerGroup.create cfg.assignmentStrategies [|cfg.topic|]
    let config = 
      { GroupConfig.groupId = cfg.groupId
        heartbeatFrequency = cfg.heartbeatFrequency
        sessionTimeout = cfg.sessionTimeout
        rebalanceTimeout = cfg.rebalanceTimeout
        protocol = groupProtocol }
    let! gm = Group.createJoin conn config
    let consumer = { conn = conn ; config = cfg ; groupMember = gm }
    return consumer }

  /// Creates a consumer.
  let create (conn:KafkaConn) (cfg:ConsumerConfig) =
    createAsync conn cfg |> Async.RunSynchronously

  let private consumerStateFromGroupMemberState (state:GroupMemberState) = 
    let _,assignments = ConsumerGroup.decodeMemberAssignment state.memberAssignment
    { ConsumerState.assignments = assignments
      memberId = state.memberId
      leaderId = state.leaderId
      members = state.members
      generationId = state.generationId
      assignmentStrategy = state.protocolName
      closed = state.closed
      protocolName = state.protocolName }

  /// Returns the current consumer state.
  let state (c:Consumer) : Async<ConsumerState> = async {
    let! state = Group.stateInternal c.groupMember
    return consumerStateFromGroupMemberState state.state }

  /// Returns the stream of consumer states as of the invocation, including the current state.
  let states (c:Consumer) : AsyncSeq<ConsumerState> =
    Group.states c.groupMember |> AsyncSeq.map (consumerStateFromGroupMemberState)

  let private combineFetchResponses 
    (r1:(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option) 
    (r2:(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option) =
    match r1,r2 with
    | Some (oks1,ends1), Some (oks2,ends2) -> Some (Array.append oks1 oks2, Array.append ends1 ends2)
    | Some _, _ -> r1
    | _, Some _ -> r2
    | _ -> None
  
  let private processPartitionFetchResponse 
    (cfg: ConsumerConfig)
    (messageVer: ApiVersion)
    (topic: TopicName)
    (partition,errorCode,highWatermark,_,_,_,messageSetSize,ms) =
    match errorCode with
    | ErrorCode.NoError ->
      if messageSetSize = 0 then Choice2Of4 (partition,highWatermark)
      else 
        let ms = Compression.decompress messageVer ms
        if cfg.checkCrc then
          MessageSet.CheckCrc (messageVer, ms)
        Choice1Of4 (ConsumerMessageSet(topic, partition, ms, highWatermark))
    | ErrorCode.OffsetOutOfRange -> 
      Choice3Of4 (partition,highWatermark)
    | ErrorCode.NotLeaderForPartition | ErrorCode.UnknownTopicOrPartition | ErrorCode.ReplicaNotAvailable ->
          
      Choice4Of4 (partition)
    | _ -> 
      failwithf "unsupported fetch error_code=%i" errorCode

  /// Fetches the specified offsets.
  /// Returns a set of message sets and an end of topic list.
  /// Returns None if the generation closed.
  let rec private tryFetch 
    (c:Consumer) 
    (state:GroupMemberStateWrapper) 
    (offsets:(Partition * Offset)[]) : Async<(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option> = 
    let cfg = c.config
    let topic = cfg.topic
    let fetch = Kafka.fetch c.conn |> AsyncFunc.catch
//    let fetch (_:FetchRequest) : Async<Result<FetchResponse, exn>> = async {
//      return Failure (exn "testing escalation!") }
    let messageVer = MessageVersions.fetchResMessage (c.conn.ApiVersion ApiKey.Fetch)
    Group.tryAsync
      (state)
      (fun _ -> None)
      (async {
        let req = 
          let os = [| topic, offsets |> Array.map (fun (p,o) -> p,o,0L,cfg.fetchMaxBytes) |]
          FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, os, 0, 0y)
        let! res = fetch req
        match res with
        | Success res ->
              
          let oks,ends,outOfRange,staleMetadata =
            res.topics
            |> Seq.collect (fun (t,ps) -> ps |> Seq.map (processPartitionFetchResponse cfg messageVer t))
            |> Seq.partitionChoices4

          let oksAndEnds = Some (oks,ends)
              
          if staleMetadata.Length > 0 then
            let staleOffsets = 
              let offsets = Map.ofArray offsets
              staleMetadata
              |> Seq.map (fun p -> p, Map.find p offsets)
              |> Seq.toArray
            Log.warn "fetch_response_indicated_stale_metadata|stale_offsets=%s" (Printers.partitionOffsetPairs staleOffsets)
            let! _ = c.conn.GetMetadataState ([|topic|])
            // TODO: only fetch stale and combine
            return! tryFetch c state offsets else
              
          if outOfRange.Length > 0 then
            let outOfRange = 
              outOfRange 
              |> Array.map (fun (p,_hwm) -> 
                let o = offsets |> Array.pick (fun (p',o) -> if p = p' then Some o else None)
                p,o)
            let! oksAndEnds' = offsetsOutOfRange c state outOfRange
            return combineFetchResponses oksAndEnds oksAndEnds'
          else
            return oksAndEnds

        | Failure ex ->
          Log.warn "fetch_exception|generation_id=%i topic=%s partition_offsets=%s error=%O" 
            state.state.generationId topic (Printers.partitionOffsetPairs offsets) ex
          //do! Group.leaveInternal c.groupMember state
          return raise ex })

  /// Handles out of range offsets.
  and private offsetsOutOfRange (c:Consumer) (state) (attemptedOffsets:(Partition * Offset)[]) = async {
    let cfg = c.config
    let topic = c.config.topic
    let partitions = attemptedOffsets |> Array.map fst |> set
    let! topicOffsetRange = 
      Offsets.offsetRange c.conn topic partitions
    let offsetInfoStr =
      topicOffsetRange
      |> Map.toSeq
      |> Seq.map (fun (p,(e,l)) -> sprintf "[p=%i earliest=%i latest=%i]" p e l)
      |> String.concat " ; "
    Log.warn "offsets_out_of_range|topic=%s offsets=%s topic_offsets=[%s]" topic (Printers.partitionOffsetPairs attemptedOffsets) offsetInfoStr
    match cfg.autoOffsetReset with
    | AutoOffsetReset.Stop ->
      Log.error "stopping_consumer|topic=%s offsets=%s" topic (Printers.partitionOffsetPairs attemptedOffsets)
      do! Group.leaveInternal c.groupMember state
      return failwithf "offsets_out_of_range|topic=%s offsets=%s topic_offsets=[%s]" topic (Printers.partitionOffsetPairs attemptedOffsets) offsetInfoStr
    | AutoOffsetReset.TryStartFromCommittedOffsets ->
      let! committedOffsets = fetchOffsets c.conn c.config.groupId [| cfg.topic, attemptedOffsets |> Array.map fst |]
      let committedOffsets = committedOffsets |> Array.pick (fun (t,os) -> if t = cfg.topic then Some os else None)
      let resolvedOffsets =
        (committedOffsets |> Map.ofArray, topicOffsetRange)
        ||> Map.mergeChoice (fun _ -> function
          | Choice2Of3 _ | Choice3Of3 _ -> -1L
          | Choice1Of3 (c,(e,l)) ->
            if c >= e && c <= l then c
            else -1L)
        |> Map.toArray
      let missingOffsets = resolvedOffsets |> Array.filter (fun (_,o) -> o = -1L)
      if missingOffsets.Length = 0 then
        Log.info "resuming_fetch_from_committed_offsets|topic=%s committed_offsets=%s" cfg.topic (Printers.partitionOffsetPairs resolvedOffsets)
        return! tryFetch c state committedOffsets
      else
        Log.warn "stopping_consumer|topic=%s committed_offsets=%s" cfg.topic (Printers.partitionOffsetPairs resolvedOffsets)
        do! Group.leaveInternal c.groupMember state
        return failwithf "stopping_consumer|topic=%s committed_offsets=%s" cfg.topic (Printers.partitionOffsetPairs resolvedOffsets)
    | AutoOffsetReset.StartFromTime t ->
      let resetOffsets =
        (attemptedOffsets |> Map.ofArray, topicOffsetRange)
        ||> Map.mergeChoice (fun _ -> function
          | Choice2Of3 _ | Choice3Of3 _ -> failwith "invalid state: expected matching partitions!"
          | Choice1Of3 (a,(e,l)) -> 
            if a > l then l
            elif a < e then
              match t with
              | Time.EarliestOffset -> e
              | Time.LatestOffset -> l
              | _ -> failwith "invalid state: expected valid start time"
            else
              a)
        |> Map.toArray
      Log.info "resuming_fetch_from_reset_offsets|topic=%s reset_offsets=%s" topic (Printers.partitionOffsetPairs resetOffsets)
      return! tryFetch c state resetOffsets }

  /// multiplexed stream of all fetch responses for this consumer
  let private fetchStream (c:Consumer) state initOffsets =
    let cfg = c.config
    let topic = cfg.topic
    let initRetryQueue = RetryQueue.create cfg.endOfTopicPollPolicy fst
    (initOffsets, initRetryQueue)
    |> AsyncSeq.unfoldAsync
        (fun (offsets:(Partition * Offset)[], retryQueue:RetryQueue<_, _>) -> async {
          let! offsets = async {
            if offsets.Length = 0 then
              let! dueRetries = RetryQueue.dueNowAwait retryQueue
              return dueRetries |> Seq.toArray
            else
              let dueRetries = RetryQueue.dueNow retryQueue |> Seq.toArray
              if dueRetries.Length > 0 then 
                return Array.append offsets dueRetries
              else 
                return offsets }
          let! res = tryFetch c state offsets
          match res with
          | None ->
            return None
          | Some (mss,ends) ->
                
            let retryQueue = 
              RetryQueue.retryRemoveAll 
                retryQueue 
                ends 
                (mss |> Seq.map (fun ms -> ms.partition))
                                
            let nextOffsets = 
              mss
              |> Array.map (fun mb -> mb.partition, MessageSet.nextOffset mb.messageSet mb.highWatermarkOffset)

            if ends.Length > 0 then
              let msg =
                ends
                |> Seq.map (fun (p,hwmo) -> sprintf "[partition=%i high_watermark_offset=%i]" p hwmo)
                |> String.concat " ; "
              Log.trace "end_of_topic_partition_reached|group_id=%s generation_id=%i member_id=%s topic=%s %s" 
                cfg.groupId state.state.generationId state.state.memberId topic msg

            return Some (mss, (nextOffsets,retryQueue)) })

  /// Consumes fetchStream and dispatches to per-partition buffers.
  let private fetchProcess 
    (c:Consumer) 
    (state:GroupMemberStateWrapper)
    (initOffsets:(Partition * Offset)[])
    (partitionBuffers:Map<Partition, BoundedMb<ConsumerMessageSet>>) = async {
    let assignment = Array.map fst initOffsets
    let cfg = c.config
    let topic = cfg.topic
    //use! _cnc = Async.OnCancel (fun () -> 
    //  Log.info "cancelling_fetch_process|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i"
    //    cfg.groupId state.state.generationId state.state.memberId topic (assignment.Length))
    Log.info "starting_fetch_process|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i" 
      cfg.groupId state.state.generationId state.state.memberId topic (assignment.Length)
    return!
      fetchStream c state initOffsets
      |> AsyncSeq.iterAsync (fun mss -> async {
        let! _ =
          mss
          |> Seq.map (fun ms -> async {
            let buf = partitionBuffers |> Map.find ms.partition
            do! buf |> BoundedMb.put ms })
          |> Async.Parallel
        return () }) }
   
  let private consumeTopic (c: Consumer) (state: GroupMemberStateWrapper) (topic: TopicName) (assignment: Partition[]) = async {
    let cfg = c.config
    let! ct = Async.CancellationToken
    let fetchProcessCancellation = CancellationTokenSource.CreateLinkedTokenSource (ct, state.state.closed)
    // initialize per-partition messageset buffers
    let partitionBuffers =
      assignment
      |> Seq.map (fun p -> p, BoundedMb.create cfg.fetchBufferSize)
      |> Map.ofSeq

    let! initOffsets = fetchOffsetsFallback c topic assignment
    Log.info "fetched_initial_offsets|conn_id=%s group_id=%s member_id=%s topic=%s offsets=%s" 
      c.conn.Config.connId cfg.groupId state.state.memberId topic (Printers.partitionOffsetPairs initOffsets)
      
    let fetchProcess =
      Async.tryFinnallyWithAsync
        (fetchProcess c state initOffsets partitionBuffers)
        (async {
          Log.info "fetch_process_stopping|group_id=%s generation_id=%i member_id=%s topic=%s" 
            cfg.groupId state.state.generationId state.state.memberId topic
          return () })
        (fun ex -> async {
          Log.error "fetch_process_errored|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i error=%O" 
            cfg.groupId state.state.generationId state.state.memberId topic (assignment.Length) ex
          do! Group.leaveInternal c.groupMember state
          return () })

    Async.Start (fetchProcess, fetchProcessCancellation.Token)
    
    let partitionStreams =
      partitionBuffers
      |> Map.toSeq
      |> Seq.map (fun (p,buf) -> 
        let tryTake = 
          BoundedMb.take buf
          |> Async.cancelWithToken fetchProcessCancellation.Token
        p, AsyncSeq.replicateUntilNoneAsync tryTake)
      |> Seq.toArray

    return partitionStreams 
  }

  /// Initiates consumption of a single generation of the consumer group protocol.
  let private consumeGeneration (c:Consumer) (state:GroupMemberStateWrapper) = 
    let groupId = c.config.groupId

    let topic,assignment = state.state.memberAssignment |> ConsumerGroup.decodeMemberAssignment
    Log.info "consumer_group_assignment_received|conn_id=%s group_id=%s topic=%s partitions=[%s]"
      c.conn.Config.connId groupId topic (Printers.partitions assignment)
      
    if assignment.Length = 0 then
      Log.warn "no_partitions_assigned|conn_id=%s group_id=%s member_id=%s topic=%s" 
        c.conn.Config.connId groupId state.state.memberId topic
      async.Return [||]
    else
      consumeTopic c state topic assignment

  /// Returns an async sequence corresponding to generations, where each generation
  /// is paired with the set of assigned fetch streams.
  let generations (c:Consumer) =
    Group.generationsInternal c.groupMember
    |> AsyncSeq.mapAsyncParallel (fun state -> async {
      let consumerState = consumerStateFromGroupMemberState state.state
      let! partitionStreams = consumeGeneration c state      
      return consumerState,partitionStreams })

  /// Returns a stream of message sets across all partitions assigned to the consumer.
  let stream (c:Consumer) =
    generations c
    |> AsyncSeq.collect (fun (s,ps) -> AsyncSeq.mergeAll (ps |> Seq.map snd |> Seq.toList) |> AsyncSeq.map (fun ms -> s,ms))

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The handler accepts the topic, partition, message set and an async computation which commits offsets corresponding to the message set.
  let consume 
    (c:Consumer)
    (handler:ConsumerState -> ConsumerMessageSet -> Async<unit>) : Async<unit> =
      generations c
      |> AsyncSeq.iterAsync (fun (consumerState,partitionStreams) ->
        partitionStreams
        |> Seq.map (fun (_,stream) -> stream |> AsyncSeq.iterAsync (handler consumerState))
        |> Async.Parallel
        |> Async.Ignore
        |> Async.cancelTokenWith consumerState.closed id)

  /// Creates a period offset committer, which commits at the specified interval (even if no new offsets are enqueued).
  /// Commits the current offsets assigned to the consumer upon creation.
  /// Returns a pair consisting of the commit queue and a process which commits offsets and reacts to rebalancing.
  let periodicOffsetCommitter (c:Consumer) (commitInterval:TimeSpan) = async {
    let rebalanced = 
      states c
      |> AsyncSeq.mapAsync (fun s -> async {
        let! currentOffsets = fetchOffsets c.conn c.config.groupId [| c.config.topic, s.assignments |]
        return
          currentOffsets 
          |> Seq.choose (fun (t,os) -> if t = c.config.topic then Some os else None)
          |> Seq.concat
          //|> Seq.where (fun (_,o) -> o <> -1L)
          |> Seq.toArray })
    return! PeriodicCommitQueue.create commitInterval rebalanced (commitOffsets c) }

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The offsets will be enqueued to be committed after the handler completes, and the commits will be invoked at
  /// the specified interval.
  let consumePeriodicCommit 
    (c:Consumer)
    (commitInterval:TimeSpan)
    (handler:ConsumerState -> ConsumerMessageSet -> Async<unit>) : Async<unit> = async {
      let! commitQueue = periodicOffsetCommitter c commitInterval
      let handler s ms = async {
        do! handler s ms
        PeriodicCommitQueue.enqueue commitQueue (ConsumerMessageSet.commitPartitionOffsets ms) }
      do! Async.choose (consume c handler) (PeriodicCommitQueue.proccess commitQueue) }

  /// Starts consumption from the start offset in the given range.
  /// Will stop consuming for each partition once it reaches the max boundary offset.
  /// Will stop and return the messages WITHIN the range if it overconsumes.
  let streamRange (c:Consumer) (offsetRange:Map<Partition, Offset * Offset>) : Async<ConsumerMessageSet[]> = async {
    let targetPartitions = 
      offsetRange |> Map.toSeq |> Seq.map fst |> set
    do! commitOffsets c (offsetRange |> Map.toSeq |> Seq.map (fun (p,(e,_)) -> p,e) |> Seq.toArray)
    return!
      (Map.empty, stream c)
      ||> AsyncSeq.threadStateAsync (fun observedOffsets (_,ms) -> async {
        let lastOs = ConsumerMessageSet.lastOffset ms
        let observedOffsets' = observedOffsets |> Map.add ms.partition lastOs
        return ((ms,observedOffsets'), observedOffsets') })
      |> AsyncSeq.takeWhileInclusive (fun (_,observedOffsets) ->        
        let reachedPartitions =
          (offsetRange,observedOffsets)
          ||> Map.mergeChoice (fun p -> function
            | Choice1Of3 ((_,targetOffset),observedOffset) -> 
              if observedOffset >= (targetOffset - 1L) then Some p 
              else None
            | _ -> None)
          |> Map.toSeq
          |> Seq.choose snd
          |> set
        not (Set.isSuperset reachedPartitions targetPartitions))
      |> AsyncSeq.map fst
      |> AsyncSeq.toArrayAsync }

  /// Closes the consumer and leaves the consumer group.
  /// This causes all underlying streams to complete.
  let close (c:Consumer) = async {
    return! Group.leave c.groupMember }

