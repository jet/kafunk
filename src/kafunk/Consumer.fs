namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks

open Kafunk

/// Kafka consumer configuration.
type ConsumerConfig = {
  
  /// The consumer group id shared by consumers in the group.
  groupId : GroupId
  
  /// The topic to consume.
  topic : TopicName
  
  /// The session timeout period, such that if no heartbeats are received within the
  /// period, a consumer is ejected from the consumer group.
  sessionTimeout : SessionTimeout
  
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
  offsetRetentionTime : int64

  /// The time of offsets to fetch if no offsets are stored (usually for a new group).
  initialFetchTime : Time
  
  /// The poll policy to employ when the end of the topic is reached.
  endOfTopicPollPolicy : RetryPolicy
  
  /// The action to take when a consumer attempts to fetch an out of range offset.
  outOfRangeAction : ConsumerOffsetOutOfRangeAction

  /// The size of the per-partition fetch buffer in terms of message set count.
  /// When at capacity, fetching stops until the buffer is drained.
  fetchBufferSize : int

} with
    
    /// Creates a consumer configuration.
    static member create 
      (groupId:GroupId, topic:TopicName, ?initialFetchTime, ?fetchMaxBytes, ?sessionTimeout, 
          ?heartbeatFrequency, ?offsetRetentionTime, ?fetchMinBytes, ?fetchMaxWaitMs, ?endOfTopicPollPolicy, ?outOfRangeAction, ?fetchBufferSize) =
      {
        groupId = groupId
        topic = topic
        sessionTimeout = defaultArg sessionTimeout 20000
        heartbeatFrequency = defaultArg heartbeatFrequency 10
        fetchMinBytes = defaultArg fetchMinBytes 0
        fetchMaxWaitMs = defaultArg fetchMaxWaitMs 0
        fetchMaxBytes = defaultArg fetchMaxBytes 100000
        offsetRetentionTime = defaultArg offsetRetentionTime -1L
        initialFetchTime = defaultArg initialFetchTime Time.EarliestOffset
        endOfTopicPollPolicy = defaultArg endOfTopicPollPolicy (RetryPolicy.constantMs 10000)
        outOfRangeAction = defaultArg outOfRangeAction ConsumerOffsetOutOfRangeAction.HaltConsumer
        fetchBufferSize = defaultArg fetchBufferSize 1
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
  generationId : GenerationId
  memberId : MemberId
  leaderId : LeaderId
  assignments : TopicPartitionAssignment[]
  closed : TaskCompletionSource<bool>
} 

/// Partition assignment for an individual consumer.
and TopicPartitionAssignment = {
  topic : TopicName
  partition : Partition
  initOffset : Offset
}

/// A consumer.
type Consumer = private {
  conn : KafkaConn
  cfg : ConsumerConfig
  state : MVar<ConsumerState>
}

/// A set of messages for a consumer from an individual topic-partition.
type ConsumerMessageSet =
  struct
    val topic : TopicName
    val partition : Partition
    val messageSet : MessageSet
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

    /// Returns the lag = high watermark offset - last offset
    static member lag (ms:ConsumerMessageSet) =
      ms.highWatermarkOffset - ConsumerMessageSet.lastOffset ms

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
    
  // TODO: refactor
  let private peekTask (f:'a -> 'b) (t:Task<'a>) (a:Async<'b>) = async {
    if t.IsCompleted then return f t.Result
    else return! a }

  /// Closes a consumer group specifying whether a new generation should begin.
  let private close (state:ConsumerState) (rejoin:bool) : Async<unit> =
    peekTask
      ignore
      state.closed.Task
      (async {
        if rejoin then
          if state.closed.TrySetResult rejoin then
            Log.warn "closing_generation|generation_id=%i member_id=%s leader_id=%s" state.generationId state.memberId state.leaderId
        else
          if state.closed.TrySetResult rejoin then
            Log.warn "closing_consumer|generation_id=%i member_id=%s leader_id=%s" state.generationId state.memberId state.leaderId
        })

  /// Closes a consumer group and causes a new generation to begin.
  let private closeGeneration (state:ConsumerState) : Async<unit> =
    close state true

  /// Closes a consumer group and causes a new generation to begin.
  let private closeConsumer (state:ConsumerState) : Async<unit> =
    close state false

  /// Commits the specified offsets within the consumer group.
  let private commit (conn:KafkaConn) (cfg:ConsumerConfig) (state:ConsumerState) (topic:TopicName) (offsets:(Partition * Offset)[]) : Async<unit> = 
    peekTask
      (ignore)
      (state.closed.Task)
      (async {
        //Log.trace "committing_offset|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i retention=%i" topic partition cfg.groupId state.memberId state.generationId offset cfg.offsetRetentionTime
        let req = OffsetCommitRequest(cfg.groupId, state.generationId, state.memberId, cfg.offsetRetentionTime, [| topic, offsets |> Array.map (fun (p,o) -> p,o,"") |])
        let! res = Kafka.offsetCommit conn req |> Async.Catch
        match res with
        | Success res ->
          if res.topics.Length = 0 then
            Log.error "offset_committ_failed|group_id=%s member_id=%s generation_id=%i topic=%s offsets=%A" cfg.groupId state.memberId state.generationId topic offsets
            return failwith "offset commit failed!"
          let errors =
            res.topics
            |> Seq.collect (fun (_,ps) ->
              ps
              |> Seq.choose (fun (p,ec) ->
                match ec with
                | ErrorCode.NoError -> None
                | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode -> Some (p,ec)
                | _ -> failwithf "unsupported error_code=%i" ec))
          if not (Seq.isEmpty errors) then
            do! closeGeneration state
            return ()
          else
            return ()
        | Failure ex ->
          Log.warn "commit_offset_failure|generation_id=%i error=%O" state.generationId ex
          do! closeConsumer state
          return () })

  /// Explicitly commits offsets to a consumer group.
  let commitOffsets (c:Consumer) (offsets:(Partition * Offset)[]) = async {
    Log.info "comitting_offsets|offsets=%s" (Printers.partitionOffsetPairs offsets)
    let! state = MVar.get c.state
    return! commit c.conn c.cfg state c.cfg.topic offsets }

  /// Fetches the starting offset for the specified topic * partitions.
  let private fetchOffsets (c:Consumer) (topic:TopicName) (partitions:Partition[]) : Async<(Partition * Offset)[]> = async {
    let conn = c.conn
    let cfg = c.cfg
    Log.info "fetching_group_member_offsets|group_id=%s time=%i topic=%s partitions=%A" cfg.groupId cfg.initialFetchTime topic partitions 
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
        Log.info "offsets_not_available_at_group_coordinator|group_id=%s topic=%s missing_offset_partitions=%A" cfg.groupId topic (missing |> Array.map fst)
        let offsetReq = OffsetRequest(-1, [| topic, missing |> Array.map (fun (p,_) -> p, cfg.initialFetchTime, 1) |])
        let! offsetRes = Kafka.offset conn offsetReq
        //let offsetResStr = OffsetResponse.Print offsetRes
        //Log.info "offset_response|%s" offsetResStr
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

  /// Joins the consumer group.
  /// - Join group.
  /// - Sync group (assign partitions to members).
  /// - Start heartbeats.
  /// - Fetch initial offsets.
  let rec private join (consumer:Consumer) (prevMemberId:MemberId option) = async {
      
    let conn = consumer.conn
    let cfg = consumer.cfg

    let protocolType = ProtocolType.consumer
    let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, [|cfg.topic|], Binary.empty)
    let assignmentStrategy : AssignmentStrategy = "range" // roundrobin
    let groupProtocols =
      GroupProtocols(
        [| assignmentStrategy, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta |])

    match prevMemberId with
    | None -> 
      Log.info "joining_consumer_group|group_id=%s topic=%s" cfg.groupId cfg.topic

    | Some prevMemberId -> 
      Log.info "rejoining_consumer_group|group_id=%s topic=%s member_id=%s" cfg.groupId cfg.topic prevMemberId

    let! _ = conn.GetGroupCoordinator cfg.groupId

    let! joinGroupRes = async {
      let req = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, defaultArg prevMemberId "", protocolType, groupProtocols)
      let! res = Kafka.joinGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ ->
        return failwithf "unsupported join_group error_code=%i" res.errorCode }
      
    match joinGroupRes with
    | Failure ec ->
      Log.warn "join_group_error|error_code=%i" ec
      match ec with
      | ErrorCode.UnknownMemberIdCode -> 
        Log.warn "resetting_member_id"
        return! join consumer None
      | _ -> 
        return! join consumer prevMemberId

    | Success joinGroupRes ->
                           
      Log.info "join_group_response|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s" 
        cfg.groupId 
        joinGroupRes.memberId 
        joinGroupRes.generationId 
        joinGroupRes.leaderId
        joinGroupRes.groupProtocol
                          
      let! syncGroupRes = async {
        if joinGroupRes.members.members.Length > 0 then
          Log.info "joined_as_leader|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            cfg.groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
          let members = joinGroupRes.members.members
          let! topicPartitions = conn.GetMetadata [|cfg.topic|]
          let topicPartitions =
            topicPartitions
            |> Map.toSeq
            |> Seq.collect (fun (t,ps) -> ps |> Seq.map (fun p -> t,p))
            |> Seq.toArray
            |> Array.groupInto members.Length
          let memberAssignments =
            (members,topicPartitions)
            ||> Array.zip 
            |> Array.map (fun ((memberId,_),ps) ->
              let assignment = 
                ps 
                |> Seq.groupBy fst 
                |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
                |> Seq.toArray
              let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment(assignment))
              memberId, (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment))
          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment(memberAssignments))
          let! res = Kafka.syncGroup conn req
          match res.errorCode with
          | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
            return None
          | _ ->
            return Some res
        else
          Log.info "joined_as_follower|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            cfg.groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
          let! res = Kafka.syncGroup conn req
          match res.errorCode with
          | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
            return None
          | _ ->
            return Some res }
        
      match syncGroupRes with
      | None ->
        return! join consumer (Some joinGroupRes.memberId)

      | Some syncGroupRes ->
                
        let assignment,_ = 
          ConsumerGroupMemberAssignment.read syncGroupRes.memberAssignment

        Log.info "received_sync_group_response|member_assignment=[%s]"
          (String.concat ", " (assignment.partitionAssignment.assignments |> Seq.map (fun (tn,ps) -> sprintf "topic=%s partitions=%A" tn ps))) 
      
        if assignment.partitionAssignment.assignments.Length = 0 then
          return failwith "no partitions assigned!"

        let! initOffsets =
          assignment.partitionAssignment.assignments
          |> Seq.map (fun (tn,ps) -> async {
            let! offsets = fetchOffsets consumer tn ps
            return tn,offsets })
          |> Async.Parallel

        let assignments =
          initOffsets
          |> Array.collect (fun (tn,os) -> os |> Array.map (fun (p,o) -> { topic = tn ; partition = p ; initOffset = o }))
        
        let assignmentsStr =
          assignments
          |> Seq.map (fun a -> sprintf "[partition=%i init_offset=%i]" a.partition a.initOffset)
          |> String.concat " ; "
          
        Log.info "fetched_initial_offsets|topic=%s offsets=%s" cfg.topic assignmentsStr

        let state =
          {
            memberId = joinGroupRes.memberId
            leaderId = joinGroupRes.leaderId
            generationId = joinGroupRes.generationId
            assignments = assignments
            closed = Tasks.TaskCompletionSource<bool>()
          }
          
        conn.CancellationToken.Register (fun () ->
          Log.info "closing_consumer_group_on_connection_close" 
          closeConsumer state |> Async.Start) |> ignore

        let heartbeatSleep = cfg.sessionTimeout / cfg.heartbeatFrequency

        /// Starts the hearbeat process.
        let rec heartbeat (state:ConsumerState) =
          peekTask
            ignore
            state.closed.Task
            (async {
              let req = HeartbeatRequest(cfg.groupId, state.generationId, state.memberId)
              let! res = Kafka.heartbeat conn req |> Async.Catch
              match res with
              | Success res ->
                match res.errorCode with
                | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode ->
                  do! closeGeneration state
                  return ()
                | _ ->
                  do! Async.Sleep heartbeatSleep
                  return! heartbeat state
              | Failure ex ->
                Log.warn "heartbeat_failure|generation_id=%i error=%O" state.generationId ex
                do! closeConsumer state
                return () })

        Log.info "starting_heartbeats|heartbeat_frequency=%i session_timeout=%i heartbeat_sleep=%i" cfg.heartbeatFrequency cfg.sessionTimeout heartbeatSleep
        Async.Start (heartbeat state)
        let! _ = consumer.state |> MVar.put state
        return state }

  /// Creates a participant in the consumer groups protocol and joins the group.
  let createAsync (conn:KafkaConn) (cfg:ConsumerConfig) = async {
    let consumer = { conn = conn ; cfg = cfg ; state = MVar.create () }
    let! _ = join consumer None
    return consumer }

  /// Creates a consumer.
  let create (conn:KafkaConn) (cfg:ConsumerConfig) =
    createAsync conn cfg |> Async.RunSynchronously

  /// Returns an async sequence corresponding to generations of the consumer group protocol.
  let generations (consumer:Consumer) =

    let cfg = consumer.cfg
    let topic = cfg.topic
    let fetch = Kafka.fetch consumer.conn |> AsyncFunc.catch

    /// Initiates consumption of a single generation of the consumer group protocol.
    let consume (state:ConsumerState) = async {
      
      // initialize per-partition messageset buffers
      let partitionBuffers =
        state.assignments
        |> Seq.map (fun p -> p.partition, BoundedMb.create cfg.fetchBufferSize)
        |> Map.ofSeq

      /// Fetches the specified offsets.
      /// Returns a set of message sets and an end of topic list.
      /// Returns None if the generation closed.
      let rec tryFetch (offsets:(Partition * Offset)[]) : Async<(ConsumerMessageSet[] * (Partition * HighwaterMarkOffset)[]) option> = 
        peekTask
          (fun _ -> None)
          (state.closed.Task)
          (async {
            let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| cfg.topic, offsets |> Array.map (fun (p,o) -> p,o,cfg.fetchMaxBytes) |])
            let! res = fetch req
            match res with
            | Success res ->
              
              // TODO: compression
              let oks,ends,outOfRange =
                res.topics
                |> Seq.collect (fun (t,ps) ->
                  ps 
                  |> Seq.map (fun (p,ec,hwmo,mss,ms) -> 
                    match ec with
                    | ErrorCode.NoError ->
                      if mss = 0 then Choice2Of3 (p,hwmo)
                      else Choice1Of3 (ConsumerMessageSet(t, p, ms, hwmo))
                    | ErrorCode.OffsetOutOfRange -> 
                      Choice3Of3 (p,hwmo)
                    | _ -> failwithf "unsupported fetch error_code=%i" ec))
                |> Seq.partitionChoices3
             
              if outOfRange.Length > 0 then
                let outOfRange = 
                  outOfRange 
                  |> Array.map (fun (p,_hwm) -> 
                    let o = offsets |> Array.pick (fun (p',o) -> if p = p' then Some o else None)
                    p,o)
                let! oks' = offsetsOutOfRange outOfRange
                return 
                  oks' 
                  |> Option.map (fun (oks',ends') -> Array.append oks oks', Array.append ends ends')
              else
                return Some (oks,ends)

            | Failure ex ->
              Log.warn "fetch_exception|generation_id=%i topic=%s partition_offsets=%A error=%O" state.generationId topic offsets ex
              do! closeConsumer state
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
        Log.warn "offset_out_of_range|topic=%s attempted_offsets=%A offset_info=[%s]" topic attemptedOffsets msg
        match cfg.outOfRangeAction with
        | HaltConsumer ->
          Log.error "halting_consumer|topic=%s attempted_offsets=%A" topic attemptedOffsets
          return raise (exn(sprintf "offset_out_of_range|topic=%s offset=%A latest_offset_info=[%s]" topic attemptedOffsets msg))
        | HaltPartition -> 
          Log.warn "halting_partition_fetch|topic=%s last_attempted_offsets=%A" topic attemptedOffsets
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
          Log.info "resuming_fetch_from_fresh_offset|topic=%s initial_fetch_time=%i fresh_offsets=%A" topic cfg.initialFetchTime freshOffsets
          return! tryFetch freshOffsets }
             
      // multiplexed stream of all fetch responses for this consumer
      let fetchStream =
        let initRetryQueue = 
          RetryQueue.create cfg.endOfTopicPollPolicy fst
        let initOffsets =
          state.assignments
          |> Array.map (fun a -> a.partition, a.initOffset)
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
                  Log.info "end_of_topic_partition_reached|%s" msg

                return Some (mss, (nextOffsets,retryQueue)) })
                      
      // consumes fetchStream and dispatches to per-partition buffers
      let fetchProcess = async {
        Log.info "starting_fetch_process|generation_id=%i assignments=%i" state.generationId (state.assignments.Length)
        return!
          Async.tryFinnallyAsync
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
                Log.info "fetch_process_stopping|generation_id=%i" state.generationId
                do! Async.Sleep 1000 // flush logs
                let! _ =
                  partitionBuffers
                  |> Map.toSeq
                  |> Seq.map (fun (_,buf) -> buf |> BoundedMb.put None)
                  |> Async.Parallel
                return () }) }
      
      Async.Start (fetchProcess)
        
      let partitionStreams =
        partitionBuffers
        |> Map.toSeq
        |> Seq.map (fun (p,buf) -> p, AsyncSeq.replicateUntilNoneAsync (BoundedMb.take buf))
        |> Seq.toArray

      return partitionStreams }
    
    let rec loop () = asyncSeq {
      let! state = MVar.get consumer.state
      let! partitionStreams = consume state
      yield state.generationId,partitionStreams
      let! rejoin = state.closed.Task |> Async.AwaitTask
      if rejoin then
        let! _ = join consumer (Some state.memberId)
        yield! loop () }

    loop ()

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The handler accepts the topic, partition, message set and an async computation which commits offsets corresponding to the message set.
  let consume 
    (handler:ConsumerMessageSet -> Async<unit>) 
    (consumer:Consumer) : Async<unit> =
      consumer
      |> generations 
      |> AsyncSeq.iterAsync (fun (_generationId,partitionStreams) ->
          partitionStreams
          |> Seq.map (fun (_p,stream) -> stream |> AsyncSeq.iterAsync (handler))
          |> Async.Parallel
          |> Async.Ignore)

  /// Starts consumption using the specified handler.
  /// The handler will be invoked in parallel across topic/partitions, but sequentially within a topic/partition.
  /// The offsets will be enqueued to be committed after the handler completes, and the commits will be invoked at
  /// the specified interval.
  let consumePeriodicCommit 
    (commitInterval:TimeSpan)
    (handler:ConsumerMessageSet -> Async<unit>)
    (consumer:Consumer) : Async<unit> = async {
      use commitQueue = Offsets.createPeriodicCommitQueue (commitInterval, commitOffsets consumer)
      let handler ms = async {
        do! handler ms
        Offsets.enqueuePeriodicCommit commitQueue (ConsumerMessageSet.commitPartitionOffsets ms) }
      return! consumer |> consume handler }