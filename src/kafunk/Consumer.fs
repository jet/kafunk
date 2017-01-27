namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open Kafunk

/// The consumer group protocol.
module ConsumerGroup =

  let private Log = Log.create "Kafunk.ConsumerGroup"

  /// A consumer groups assignment stratgey.
  type AssignmentStrategy = {
            
    /// Given the configured topic name, returns metadata for the consumer group protocol.
    metadata : TopicName -> Async<ConsumerGroupProtocolMetadata>
      
    /// Assigns members to partitions given a set of available topic partitions and members.
    assign : (TopicName * Partition[])[] -> (MemberId * ConsumerGroupProtocolMetadata)[] -> (GroupMemberState option) -> Async<(MemberId * ConsumerGroupMemberAssignment)[]>

  }

  /// Decodes member state from the MemberAssignment field in SyncGroupResponse.
  let internal decodeMemberAssignment (memberAssignment:MemberAssignment) =
      
    let assignment,_ = ConsumerGroupMemberAssignment.read memberAssignment
    
    Log.info "decoded_sync_group_response|version=%i member_assignment=[%s]"
      assignment.version
      (String.concat ", " (assignment.partitionAssignment.assignments |> Seq.map (fun (tn,ps) -> sprintf "topic=%s partitions=%s" tn (Printers.partitions ps)))) 
      
    if assignment.partitionAssignment.assignments.Length = 0 then
      failwith "no partitions assigned!"

    let assignments = 
      assignment.partitionAssignment.assignments
      |> Seq.collect (fun (_,ps) -> ps)
      |> Seq.toArray

    assignments

  /// Creates an instances of the consumer groups protocol given the specified assignment strategies.
  let create (strategies:(AssignmentStrategyName * AssignmentStrategy)[]) (topic:TopicName) =
      
    let strategies = strategies |> Map.ofArray

    let assign (conn:KafkaConn) (prevState:GroupMemberState option) (startegyName:AssignmentStrategyName) (members:(MemberId * MemberMetadata)[]) : Async<(MemberId * ProtocolMetadata)[]> = async {
        
      let! topicPartitions = conn.GetMetadata [|topic|]
        
      let topicPartitions = 
        let ps = topicPartitions |> Map.find topic
        [|topic,ps|]

      let assignmentStrategy = Map.find startegyName strategies

      let members = 
        members
        |> Array.map (fun (memberId,meta) ->
          let meta,_ = ConsumerGroupProtocolMetadata.read meta
          memberId,meta)

      let! memberAssignments = assignmentStrategy.assign topicPartitions members prevState

      let memberAssignments =
        memberAssignments
        |> Array.map (fun (memberId,assignment) ->
          memberId, (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment))
        
      return memberAssignments }

    let protocols = async {
      return!
        strategies
        |> Seq.map (fun kvp -> async {
          let startegyName = kvp.Key
          let strategy = kvp.Value
          let! metadata = strategy.metadata topic
          return startegyName, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write metadata })
        |> Async.Parallel }
        
    { protocolType = ProtocolType.consumer ; protocols = protocols ; assign = assign }

  /// Built-in assignment stratgies.
  module AssignmentStratgies =

    /// The range consumer group assignment startegy.
    let Range =
      
      let metadata topic = async {
        let version = 0s
        let userData = Binary.empty
        return ConsumerGroupProtocolMetadata(version, [|topic|], userData) }

      let assign (topicPartitions:(TopicName * Partition[])[]) (members:(MemberId * ConsumerGroupProtocolMetadata)[]) _ = 
      
        let memberAssignments =
          
          let topicPartitions =
            topicPartitions
            |> Seq.collect (fun (t,ps) -> 
              ps |> Seq.map (fun p -> t,p))
            |> Seq.toArray
            |> Array.groupInto members.Length

          (members,topicPartitions)
          ||> Array.zip 
          |> Array.map (fun ((memberId,meta),ps) ->
            let assignment = 
              ps 
              |> Seq.groupBy fst 
              |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
              |> Seq.toArray
            memberId, meta, assignment)

        let memberAssignmentsStr =
          memberAssignments
          |> Seq.map (fun (memberId,meta,topicPartitions) -> 
            let str = 
              topicPartitions 
              |> Seq.map (fun (t,ps) -> sprintf "[topic=%s partitions=%s]" t (String.concat "," (ps |> Seq.map (string))))
              |> String.concat " ; "
            sprintf "[member_id=%s user_data=%s assignments=%s]" memberId (Binary.toString meta.userData) str)
          |> String.concat " ; "
        Log.info "leader_determined_member_assignments|%s" memberAssignmentsStr

        let memberAssignments =
          memberAssignments
          |> Array.map (fun (memberId,meta,ps) ->
            let userData = meta.userData
            let version = meta.version
            let assignment = ConsumerGroupMemberAssignment(version, PartitionAssignment(ps), userData)
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
  /// Default: 20000
  sessionTimeout : SessionTimeout
  
  /// The time during which a consumer must rejoin a group after a rebalance.
  /// If the consumer doesn't rejoin within this time, it will be ejected.
  /// Supported in v0.10.1.
  /// Default: 20000
  rebalanceTimeout : RebalanceTimeout

  /// The number of times to send heartbeats within a session timeout period.
  /// Default: 10
  heartbeatFrequency : int32
  
  /// The minimum bytes to buffer server side for a fetch request.
  /// 0 to return immediately.
  /// Default: 0
  fetchMinBytes : MinBytes

  /// The maximum time to wait for a fetch request to return sufficient data.
  /// Default: 0
  fetchMaxWaitMs : MaxWaitTime
  
  /// The maximum bytes to return as part of a partition for a fetch request.
  /// Default: 100000
  fetchMaxBytes : MaxBytes
  
  /// Offset retention time.
  /// Default: -1L
  offsetRetentionTime : RetentionTime

  /// The time of offsets to fetch if no offsets are stored (usually for a new group).
  /// Default: Time.EarliestOffset
  initialFetchTime : Time
  
  /// The poll policy to employ when the end of the topic is reached.
  /// Default: RetryPolicy.constantMs 10000
  endOfTopicPollPolicy : RetryPolicy
  
  /// The action to take when a consumer attempts to fetch an out of range offset.
  /// Default: HaltConsumer
  outOfRangeAction : ConsumerOffsetOutOfRangeAction

  /// The size of the per-partition fetch buffer in terms of message set count.
  /// When at capacity, fetching stops until the buffer is drained.
  /// Default: 1
  fetchBufferSize : int

  /// The consumer group assignment strategies to use.
  /// The group coordinator ensures that all members support the same strategy.
  /// When multiple stratgies are supported by all members, the first one in the list is selected.
  /// Default: [ "range", ConsumerGroupProtocol.AssignmentStratgies.Range ]
  assignmentStrategies : (AssignmentStrategyName * ConsumerGroup.AssignmentStrategy)[]

} with
    
    /// Creates a consumer configuration.
    static member create 
      (groupId:GroupId, topic:TopicName, ?initialFetchTime, ?fetchMaxBytes, ?sessionTimeout, ?rebalanceTimeout,
          ?heartbeatFrequency, ?offsetRetentionTime, ?fetchMinBytes, ?fetchMaxWaitMs, ?endOfTopicPollPolicy, ?outOfRangeAction, ?fetchBufferSize,
          ?assignmentStrategies) =
      {
        groupId = groupId
        topic = topic
        sessionTimeout = defaultArg sessionTimeout 20000
        rebalanceTimeout = defaultArg rebalanceTimeout 20000
        heartbeatFrequency = defaultArg heartbeatFrequency 10
        fetchMinBytes = defaultArg fetchMinBytes 0
        fetchMaxWaitMs = defaultArg fetchMaxWaitMs 0
        fetchMaxBytes = defaultArg fetchMaxBytes 100000
        offsetRetentionTime = defaultArg offsetRetentionTime -1L
        initialFetchTime = defaultArg initialFetchTime Time.EarliestOffset
        endOfTopicPollPolicy = defaultArg endOfTopicPollPolicy (RetryPolicy.constantMs 10000)
        outOfRangeAction = defaultArg outOfRangeAction ConsumerOffsetOutOfRangeAction.HaltConsumer
        fetchBufferSize = defaultArg fetchBufferSize 1
        assignmentStrategies =
          match assignmentStrategies with
          | None -> [| "range", ConsumerGroup.AssignmentStratgies.Range |]
          | Some xs -> xs
      }

/// The action to take when the consumer attempts to fetch an offset which is out of range.
/// This typically happens if the consumer is outpaced by the message cleanup process.
/// The default action is to halt consumption.
and ConsumerOffsetOutOfRangeAction =

  /// Halt the consumer, raising an exception.
  | HaltConsumer
  
  /// Halt the consumption of only the out of range partition.
  | HaltPartition

  /// Request a fresh set of offsets and resume consumption from the time
  /// configured as the initial fetch time for the consumer (earliest, or latest).
  | ResumeConsumerWithFreshInitialFetchTime


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

  /// The partitions assigned to this consumer.
  assignments : Partition[]

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
      ms.messageSet.messages |> Seq.sumBy (fun (_,s,_) -> s)

    /// Returns an array of all messages in the message set.
    static member messages (ms:ConsumerMessageSet) =
      ms.messageSet.messages |> Array.map (fun (_,_,m) -> m)


/// High-level consumer API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Consumer =

  let private Log = Log.create "Kafunk.Consumer"

  /// Explicitly commits offsets to a consumer group.
  /// Note that consumers only fetch these offsets when first joining a group or when rejoining.
  let commitOffsets (c:Consumer) (offsets:(Partition * Offset)[]) = async {
    Log.info "committing_offsets|group_id=%s topic=%s offsets=%s" c.config.groupId c.config.topic (Printers.partitionOffsetPairs offsets)
    let! state = Group.stateInternal c.groupMember
    let conn = c.conn
    let cfg = c.config
    let topic = cfg.topic
    return!
        Group.tryAsync
          (state)
          (ignore)
          (async {
            let req = OffsetCommitRequest(cfg.groupId, state.state.generationId, state.state.memberId, cfg.offsetRetentionTime, [| topic, offsets |> Array.map (fun (p,o) -> p,o,"") |])
            let! res = Kafka.offsetCommit conn req |> Async.Catch
            match res with
            | Success res ->
              if res.topics.Length = 0 then
                Log.error "offset_committ_failed|group_id=%s member_id=%s generation_id=%i topic=%s offsets=%s" cfg.groupId state.state.memberId state.state.generationId topic (Printers.partitionOffsetPairs offsets)
                return failwith "offset commit failed!"
              let errors =
                res.topics
                |> Seq.collect (fun (_,ps) ->
                  ps
                  |> Seq.choose (fun (p,ec) ->
                    match ec with
                    | ErrorCode.NoError -> None
                    | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode -> Some (p,ec)
                    | _ -> failwithf "unsupported commit offset error_code=%i" ec))
              if not (Seq.isEmpty errors) then
                do! Group.closeGeneration c.groupMember state
                return ()
              else
                return ()
            | Failure ex ->
              Log.warn "commit_offset_exception|generation_id=%i error=%O" state.state.generationId ex
              do! Group.leaveInternal c.groupMember state
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
    let req = OffsetFetchRequest(groupId, topics)
    let! res = Kafka.offsetFetch conn req
    let oks,errors =
      res.topics
      |> Seq.collect (fun (t,ps) ->
        ps
        |> Seq.map (fun (p,o,_md,ec) ->
          match ec with
          | ErrorCode.NoError ->
            if o = -1L then Choice1Of2 (t,p,o)
            else Choice1Of2 (t,p,o)
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
        Log.info "offsets_not_available_at_group_coordinator|group_id=%s topic=%s missing_offset_partitions=%s" cfg.groupId topic (Printers.partitions (missing |> Array.map fst))
        let offsetReq = OffsetRequest(-1, [| topic, missing |> Array.map (fun (p,_) -> p, cfg.initialFetchTime, 1) |])
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
    let groupProtocol = ConsumerGroup.create cfg.assignmentStrategies cfg.topic
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

  /// Returns an async sequence corresponding to generations, where each generation
  /// is paired with the set of assigned fetch streams.
  let generations (consumer:Consumer) =

    let cfg = consumer.config
    let topic = cfg.topic
    let fetch = Kafka.fetch consumer.conn |> AsyncFunc.catch
    let messageVer = Versions.fetchResMessage (Versions.byKey consumer.conn.Config.version ApiKey.Fetch)
    
    /// Initiates consumption of a single generation of the consumer group protocol.
    let consume (state:GroupMemberStateWrapper) = async {
      
      let assignedPartitions = state.state.memberAssignment |> ConsumerGroup.decodeMemberAssignment

      let! ct = Async.CancellationToken
      let fetchProcessCancellationToken = CancellationTokenSource.CreateLinkedTokenSource (ct, state.state.closed)

      // initialize per-partition messageset buffers
      let partitionBuffers =
        assignedPartitions
        |> Seq.map (fun p -> p, BoundedMb.create cfg.fetchBufferSize)
        |> Map.ofSeq

      let! initOffsets = fetchOffsetsFallback consumer cfg.topic assignedPartitions
      Log.info "fetched_initial_offsets|group_id=%s member_id=%s topic=%s offsets=%s" cfg.groupId state.state.memberId cfg.topic (Printers.partitionOffsetPairs initOffsets)

      let combineFetchResponses (r1:(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option) (r2:(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option) =
        match r1,r2 with
        | Some (oks1,ends1), Some (oks2,ends2) -> Some (Array.append oks1 oks2, Array.append ends1 ends2)
        | Some _, _ -> r1
        | _, Some _ -> r2
        | _ -> None

      /// Fetches the specified offsets.
      /// Returns a set of message sets and an end of topic list.
      /// Returns None if the generation closed.
      let rec tryFetch (offsets:(Partition * Offset)[]) : Async<(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option> = 
        Group.tryAsync
          (state)
          (fun _ -> None)
          (async {
            let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| cfg.topic, offsets |> Array.map (fun (p,o) -> p,o,cfg.fetchMaxBytes) |])
            let! res = fetch req
            match res with
            | Success res ->
              
              let oks,ends,outOfRange,staleMetadata =
                res.topics
                |> Seq.collect (fun (t,ps) ->
                  ps 
                  |> Seq.map (fun (p,ec,hwmo,mss,ms) -> 
                    match ec with
                    | ErrorCode.NoError ->
                      if mss = 0 then Choice2Of4 (p,hwmo)
                      else 
                        let ms = Compression.decompress messageVer ms
                        Choice1Of4 (ConsumerMessageSet(t, p, ms, hwmo))
                    | ErrorCode.OffsetOutOfRange -> 
                      Choice3Of4 (p,hwmo)
                    | ErrorCode.NotLeaderForPartition | ErrorCode.UnknownTopicOrPartition | ErrorCode.ReplicaNotAvailable ->
                      
                      Choice4Of4 (p)
                    | _ -> 
                      failwithf "unsupported fetch error_code=%i" ec))
                |> Seq.partitionChoices4

              let oksAndEnds = Some (oks,ends)

              if staleMetadata.Length > 0 then
                let staleOffsets = 
                  let offsets = Map.ofArray offsets
                  staleMetadata
                  |> Seq.map (fun p -> p, Map.find p offsets)
                  |> Seq.toArray
                Log.warn "fetch_response_indicated_stale_metadata|stale_offsets=%s" (Printers.partitionOffsetPairs staleOffsets)
                let! _ = consumer.conn.GetMetadata ([|cfg.topic|])
                // TODO: only fetch stale and combine
                return! tryFetch offsets else
              
              if outOfRange.Length > 0 then
                let outOfRange = 
                  outOfRange 
                  |> Array.map (fun (p,_hwm) -> 
                    let o = offsets |> Array.pick (fun (p',o) -> if p = p' then Some o else None)
                    p,o)
                let! oksAndEnds' = offsetsOutOfRange outOfRange
                return combineFetchResponses oksAndEnds oksAndEnds'
              else
                return oksAndEnds

            | Failure ex ->
              Log.warn "fetch_exception|generation_id=%i topic=%s partition_offsets=%s error=%O" state.state.generationId topic (Printers.partitionOffsetPairs offsets) ex
              do! Group.leaveInternal consumer.groupMember state
              return raise ex })

      /// Handles out of range offsets.
      and offsetsOutOfRange (attemptedOffsets:(Partition * Offset)[]) = async {
        let partitions = attemptedOffsets |> Array.map fst |> set
        let! timeOffsets = Offsets.offsets consumer.conn topic partitions [|Time.EarliestOffset;Time.LatestOffset|] 1
        let msg =
          timeOffsets
          |> Map.toSeq
          |> Seq.map (fun (time,offsetRes) -> 
            offsetRes.topics
            |> Seq.map (fun (_tn,os) ->
              let os =
                os
                |> Seq.map (fun o -> sprintf "offset=%i" (o.offsets |> Seq.tryItem 0 |> Option.getOr -1L))
                |> String.concat " ; "
              sprintf "time=%i %s" time os)
            |> String.concat " ; ")
          |> String.concat " ; "
        Log.warn "offset_out_of_range|topic=%s attempted_offsets=%s offset_info=[%s]" topic (Printers.partitionOffsetPairs attemptedOffsets) msg
        match cfg.outOfRangeAction with
        | HaltConsumer ->
          Log.error "halting_consumer|topic=%s attempted_offsets=%s" topic (Printers.partitionOffsetPairs attemptedOffsets)
          return raise (exn(sprintf "offset_out_of_range|topic=%s offset=%s latest_offset_info=[%s]" topic (Printers.partitionOffsetPairs attemptedOffsets) msg))
        | HaltPartition -> 
          Log.warn "halting_partition_fetch|topic=%s last_attempted_offsets=%s" topic (Printers.partitionOffsetPairs attemptedOffsets)
          return None
        | ResumeConsumerWithFreshInitialFetchTime ->
          let offsetInfo = timeOffsets |> Map.find cfg.initialFetchTime
          let freshOffsets = 
            offsetInfo.topics
            |> Seq.collect (fun (tn,ps) ->
              ps
              |> Seq.map (fun p -> tn, p.partition, p.offsets.[0]))
            |> Seq.choose (fun (tn,p,o) ->
              if tn = topic && Set.contains p partitions then Some (p,o)
              else None)
            |> Seq.toArray
          Log.info "resuming_fetch_from_fresh_offset|topic=%s initial_fetch_time=%i fresh_offsets=%s" topic cfg.initialFetchTime (Printers.partitionOffsetPairs freshOffsets)
          return! tryFetch freshOffsets }
             
      // multiplexed stream of all fetch responses for this consumer
      let fetchStream =
        let initRetryQueue = 
          RetryQueue.create cfg.endOfTopicPollPolicy fst
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
              let! res = tryFetch offsets
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
                  Log.info "end_of_topic_partition_reached|topic=%s %s" topic msg

                return Some (mss, (nextOffsets,retryQueue)) })
                      
      // consumes fetchStream and dispatches to per-partition buffers
      let fetchProcess = async {
        use! _cnc = Async.OnCancel (fun () -> 
          Log.warn "cancelling_fetch_process|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i"
            cfg.groupId state.state.generationId state.state.memberId cfg.topic (assignedPartitions.Length))

        Log.info "starting_fetch_process|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i" 
          cfg.groupId state.state.generationId state.state.memberId cfg.topic (assignedPartitions.Length)

        return!
          Async.tryFinnallyWithAsync
            (async {
              do!
                fetchStream
                |> AsyncSeq.iterAsync (fun mss -> async {
                  let! _ =
                    mss
                    |> Seq.map (fun ms -> async {
                      let buf = partitionBuffers |> Map.find ms.partition
                      do! buf |> BoundedMb.put (Some ms) })
                    |> Async.Parallel
                  return () }) })
              (async {
                Log.info "fetch_process_stopping|group_id=%s generation_id=%i member_id=%s topic=%s" 
                  cfg.groupId state.state.generationId state.state.memberId cfg.topic
                do! Async.Sleep 1000 // flush logs
                if not fetchProcessCancellationToken.IsCancellationRequested then
                  fetchProcessCancellationToken.Cancel ()
                return () })
              (fun ex -> async { 
                Log.error "fetch_process_errored|group_id=%s generation_id=%i member_id=%s topic=%s partition_count=%i error=%O" 
                  cfg.groupId state.state.generationId state.state.memberId cfg.topic (assignedPartitions.Length) ex
                return raise ex }) }
            
      Async.Start (fetchProcess, fetchProcessCancellationToken.Token)
        
      let partitionStreams =
        partitionBuffers
        |> Map.toSeq
        |> Seq.map (fun (p,buf) -> 
          let tryTake = BoundedMb.take buf |> Async.cancelTokenWith fetchProcessCancellationToken.Token (konst None)
          p, AsyncSeq.replicateUntilNoneAsync tryTake)
        |> Seq.toArray

      return partitionStreams }
    
    Group.generationsInternal consumer.groupMember
    |> AsyncSeq.mapAsync (fun state -> async {
      let! partitionStreams = consume state
      return state.state,partitionStreams })

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The handler accepts the topic, partition, message set and an async computation which commits offsets corresponding to the message set.
  let consume 
    (handler:GroupMemberState -> ConsumerMessageSet -> Async<unit>) 
    (consumer:Consumer) : Async<unit> =
      consumer
      |> generations
      |> AsyncSeq.iterAsyncParallel (fun (groupMemberState,partitionStreams) ->
        partitionStreams
        |> Seq.map (fun (_p,stream) -> stream |> AsyncSeq.iterAsync (handler groupMemberState))
        |> Async.Parallel
        |> Async.Ignore//)
        |> Async.cancelTokenWith groupMemberState.closed id)

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The offsets will be enqueued to be committed after the handler completes, and the commits will be invoked at
  /// the specified interval.
  let consumePeriodicCommit 
    (commitInterval:TimeSpan)
    (handler:GroupMemberState -> ConsumerMessageSet -> Async<unit>)
    (consumer:Consumer) : Async<unit> = async {
      use commitQueue = Offsets.createPeriodicCommitQueue (commitInterval, commitOffsets consumer)
      let handler s ms = async {
        do! handler s ms
        Offsets.enqueuePeriodicCommit commitQueue (ConsumerMessageSet.commitPartitionOffsets ms) }
      do! consumer |> consume handler
      do! Offsets.flushPeriodicCommit commitQueue }

  /// Returns a stream of message sets across all partitions assigned to the consumer.
  /// The buffer size is the size of the buffer into which messages sets are read before the buffer exerts
  /// backpressure on the underlying consumer.
  let stream (bufferSize:int) (consumer:Consumer) = asyncSeq {
    use mb = BoundedMb.create bufferSize
    let handle s ms = BoundedMb.put (s,ms) mb
    let! _ = Async.StartChild (consume handle consumer)
    yield! AsyncSeq.replicateInfiniteAsync (BoundedMb.take mb) }

  /// Returns the current consumer state.
  let state (c:Consumer) : Async<ConsumerState> = async {
    let! state = Group.stateInternal c.groupMember
    let assignments = ConsumerGroup.decodeMemberAssignment state.state.memberAssignment
    return 
      { ConsumerState.assignments = assignments
        memberId = state.state.memberId
        leaderId = state.state.leaderId
        members = state.state.members
        generationId = state.state.generationId
        assignmentStrategy = state.state.protocolName } }

  /// Closes the consumer and leaves the consumer group.
  /// This causes all underlying streams to complete.
  let close (c:Consumer) = async {
    return! Group.leave c.groupMember }