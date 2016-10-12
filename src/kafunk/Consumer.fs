namespace Kafunk

open System
open System.Threading
open System.Threading.Tasks

open Kafunk

type ConsumerConfig = {
  groupId : GroupId
  topics : TopicName[]
  sessionTimeout : SessionTimeout
  heartbeatFrequency : int32
  fetchMinBytes : MinBytes
  fetchMaxWaitMs : MaxWaitTime
  metadataFetchTimeoutMs : int32
  totalBufferMemory : int32
  fetchBufferBytes : MaxBytes
  clientId : string
  socketReceiveBuffer : int32
  reconnectBackoffMs : int32
  offsetRetentionTime : int64
  initialFetchTime : Time
}
  with
    static member create (groupId:GroupId, topics:TopicName[], ?initialFetchTime, ?fetchBufferBytes) =
      {
        groupId = groupId
        topics = topics
        sessionTimeout = 10000
        heartbeatFrequency = 4
        fetchMinBytes = 0
        fetchMaxWaitMs = 0
        metadataFetchTimeoutMs = 0
        totalBufferMemory = 10000
        fetchBufferBytes = defaultArg fetchBufferBytes 1000000
        clientId = Guid.NewGuid().ToString("N")
        socketReceiveBuffer = 1000
        reconnectBackoffMs = 0
        offsetRetentionTime = -1L
        initialFetchTime = defaultArg initialFetchTime Time.EarliestOffset
      }



/// High-level consumer API.
module Consumer =

  let private Log = Log.create "Kafunk.Consumer"
  
  /// Stats corresponding to a single generation of the consumer group protocol.
  type GenerationState = {
    generationId : GenerationId
    memberId : MemberId
    leaderId : LeaderId
    assignments : TopicPartitionAssignment[]
    //cancellationToken : CancellationTokenSource
    //cancelled : Async<unit>
    closed : Tasks.TaskCompletionSource<unit>
  } 

  and TopicPartitionAssignment = {    
    topic : TopicName
    partition : Partition
    initOffset : Offset
  }

  // TODO: refactor (back to Cancellation token, or Hopac Alt?)
  let peekTask (f:'a -> 'b) (t:Task<'a>) (a:Async<'b>) = async {
    if t.IsCompleted then return f t.Result
    else return! a }


  /// Returns an async sequence corresponding to generations of the consumer group protocol.
  /// A generation changes when the group changes, particularly when members join or leave.
  /// Each generation contains a set of async sequences corresponding to messages in a single topic-partition.
  /// The message set is accompanied by an async computation which when evaluated, commits the checkpoint
  /// corresponding to the message set.
  let consume (conn:KafkaConn) (cfg:ConsumerConfig) = asyncSeq {
        
    // TODO: configurable
    let protocolType = ProtocolType.consumer
    let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, Binary.empty)
    let assignmentStrategy : AssignmentStrategy = "range" // roundrobin
    let groupProtocols =
      GroupProtocols(
        [| assignmentStrategy, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta |])
                            

    /// Joins the consumer group.
    /// - Join group.
    /// - Sync group (assign partitions to members).
    /// - Fetch initial offsets.
    let rec join (prevMemberId:MemberId option) = async {      
      match prevMemberId with
      | None -> 
        Log.info "initializing_consumer|group_id=%s" cfg.groupId

      | Some prevMemberId -> 
        Log.info "rejoining_consumer_group|group_id=%s member_id=%s" cfg.groupId prevMemberId

      let! _ = conn.GetGroupCoordinator (cfg.groupId)

      let! joinGroupRes = async {
        let initMemberId = defaultArg prevMemberId ""
        let req = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, initMemberId, protocolType, groupProtocols)
        let! res = Kafka.joinGroup conn req
        match res.errorCode with
        | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
          return Failure res.errorCode
        | _ ->
          return Success res }
      
      match joinGroupRes with
      | Failure ec ->        
        Log.warn "join_group_error"
        do! Async.Sleep 100
        match ec with
        | ErrorCode.UnknownMemberIdCode -> 
          Log.warn "resetting_member_id"
          return! join None
        | _ -> return! join prevMemberId

      | Success joinGroupRes ->
                           
        Log.info "join_group_response|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s" 
          cfg.groupId 
          joinGroupRes.memberId 
          joinGroupRes.generationId 
          joinGroupRes.leaderId
          joinGroupRes.groupProtocol
                          
        let! syncGroupRes = async {
          if joinGroupRes.members.members.Length > 0 then          
            Log.info "joined_as_leader"
            let members = joinGroupRes.members.members      
            let! topicPartitions = conn.GetMetadata cfg.topics          
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
            Log.info "joined_as_follower"
            let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
            let! res = Kafka.syncGroup conn req
            match res.errorCode with
            | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
              return None
            | _ ->
              return Some res }
        
        match syncGroupRes with
        | None ->
          return! join (Some joinGroupRes.memberId)

        | Some syncGroupRes ->           
                
          let assignment,_ = 
            ConsumerGroupMemberAssignment.read syncGroupRes.memberAssignment

          Log.info "received_sync_group_response|member_assignment=[%s]"
            (String.concat ", " (assignment.partitionAssignment.assignments |> Seq.map (fun (tn,ps) -> sprintf "topic=%s partitions=%A" tn ps))) 
      
          /// Fetches the starting offset for the specified topic * partition.
          let fetchInitOffset (tn:TopicName, p:Partition) = async {
            Log.info "fetching_group_member_offset|topic=%s partition=%i group_id=%s time=%i" tn p cfg.groupId cfg.initialFetchTime
            try
              let req = OffsetFetchRequest(cfg.groupId, [| tn, [| p |] |])
              let! res = Kafka.offsetFetch conn req
              let _topic,ps = res.topics.[0]
              let (_p,offset,_metadata,ec) = ps.[0]
              match ec with
              | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode  ->
                return failwith "restart join process"
              | _ ->
                if offset = -1L then
                  Log.info "offset_not_available_at_group_coordinator|group_id=%s member_id=%s topic=%s partition=%i generation=%i" cfg.groupId joinGroupRes.memberId tn p joinGroupRes.generationId
                  let offsetReq = OffsetRequest(-1, [| tn, [| p,cfg.initialFetchTime,1 |] |])
                  let! offsetRes = Kafka.offset conn offsetReq
                  let _,ps = offsetRes.topics.[0]
                  return ps.[0].offsets.[0]
                else
                  return offset
            with ex ->
              Log.error "fetch_offset_error|error=%O" ex
              //do! close ()
              return raise ex }

          let! initOffsets =
            assignment.partitionAssignment.assignments
            |> Seq.collect (fun (tn,ps) -> ps |> Seq.map (fun p -> tn,p))
            |> Seq.map (fun (tn,p) -> async {
              let! offset = fetchInitOffset (tn,p)
              return { topic = tn ; partition = p ; initOffset = offset } })
            |> Async.Parallel
        
          Log.info "fetched_initial_offsets|"

          let state =
            {
              memberId = joinGroupRes.memberId
              leaderId = joinGroupRes.leaderId
              generationId = joinGroupRes.generationId
              assignments = initOffsets
              closed = Tasks.TaskCompletionSource<unit>()
            }
          
          conn.CancellationToken.Register (fun () -> state.closed.TrySetResult() |> ignore) |> ignore

          /// Starts the hearbeat process.
          let rec heartbeat (state:GenerationState) =
            peekTask
              id
              state.closed.Task
              (async {
                let req = HeartbeatRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId)
                let! res = Kafka.heartbeat conn req
                match res.errorCode with
                | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode ->
                  do! close state
                  return ()
                | _ ->
                  do! Async.Sleep (cfg.sessionTimeout / cfg.heartbeatFrequency)
                  return! heartbeat state })

          Log.info "starting_heartbeats|heartbeat_frequency=%i session_timeout=%i" cfg.heartbeatFrequency cfg.sessionTimeout
          //Async.Start (heartbeat state', cts.Token)
          Async.Start (heartbeat state)
          return state }

    /// Closes a consumer group.
    /// The current generation stops emitting.
    /// A new join operation will begin.
    and close (state:GenerationState) : Async<unit> =
      peekTask
        id
        state.closed.Task
        (async {
          if state.closed.TrySetResult () then
            Log.warn "closing_consumer_group|generation_id=%i member_id=%s leader_id=%s" state.generationId state.memberId state.leaderId
          else
            Log.warn "concurrent_close_request_received|generation_id=%i member_id=%s leader_id=%s" state.generationId state.memberId state.leaderId
          })

    /// Initiates consumption of a single generation of the consumer group protocol.
    let consume (state:GenerationState) = async {

      /// Returns an async sequence corresponding to the consumption of an individual partition.
      let consumePartition (topic:TopicName, partition:Partition, initOffset:FetchOffset) = async {        
           
        /// Commits the specified offset within the consumer group.
        let commitOffset (offset:FetchOffset) : Async<unit> = async {
          //Log.trace "committing_offset|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i retention=%i" topic partition cfg.groupId state.memberId state.generationId offset cfg.offsetRetentionTime
          let req = OffsetCommitRequest(cfg.groupId, state.generationId, state.memberId, cfg.offsetRetentionTime, [| topic, [| partition, offset, "" |] |])
          let! res = Kafka.offsetCommit conn req
          if res.topics.Length > 0 then
            let (tn,ps) = res.topics.[0]
            let (p,ec) = ps.[0]
            match ec with
            | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode  ->
              do! close state
              return ()
            | _ ->
              //Log.trace "offset_comitted|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i" tn p cfg.groupId state.memberId state.generationId offset
              return ()
          else          
            Log.error "offset_committ_failed|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i" topic partition cfg.groupId state.memberId state.generationId offset
            return failwith "offset commit failed!" }

        /// Fetches the specified offset.
        let rec fetch (offset:FetchOffset) : Async<FetchResponse option> = 
          peekTask
            (fun _ -> None)
            (state.closed.Task)
            (async {
              let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [| partition, offset, cfg.fetchBufferBytes |] |])
              let! res = Kafka.fetch conn req
              if res.topics.Length = 0 then
                return failwith "nothing returned in fetch response!"
              else
                let _,partitions = res.topics.[0]
                let _,ec,highWatermarkOffset,_mss,ms = partitions.[0]
                match ec with
                | ErrorCode.OffsetOutOfRange | ErrorCode.UnknownTopicOrPartition | ErrorCode.NotLeaderForPartition ->
                  do! close state
                  return None
                | _ ->
                  if ms.messages.Length = 0 then
                    Log.info "reached_end_of_stream|topic=%s partition=%i offset=%i high_watermark_offset=%i" topic partition offset highWatermarkOffset
                    do! Async.Sleep 10000
                    return! fetch offset 
                  else
                    //let ms = Compression.decompress ms
                    return Some res })

        let rec fetch2 (offset:FetchOffset) = async {
          try
            return! fetch offset
          with 
            | EscalationException (ec,res) when ec = ErrorCode.OffsetOutOfRange ->
              Log.warn "offset_exception_escalated|topic=%s partition=%i error_code=%i res=%A" topic partition ec res   
              let offsetReq = OffsetRequest(-1, [| topic, [| partition, cfg.initialFetchTime, 1 |] |])                         
              let! offsetRes = Kafka.offset conn offsetReq
              let (tn,ps) = offsetRes.topics.[0]
              let p = ps.[0]
              Log.warn "fetched_topic_offsets|topic=%s partition=%i latest_offset=%i attempted_offset=%i delta=%i" tn p.partition p.offsets.[0] offset (p.offsets.[0] - offset)
              do! Async.Sleep 5000
              return! fetch2 p.offsets.[0] }

        let fetch = fetch2

        /// Fetches a stream of messages starting at the specified offset.
        let rec fetchStream (offset:FetchOffset) = asyncSeq {          
          let! res = fetch offset
          match res with
          | None -> ()
          | Some res ->          
            let _,partitions = res.topics.[0]
            let _,_ec,highWatermarkOffset,_mss,ms = partitions.[0]
            //let ms = Compression.decompress ms
            let commit = commitOffset offset
            yield ms,commit
            try                                              
              let nextOffset = MessageSet.nextOffset ms highWatermarkOffset
              yield! fetchStream nextOffset
            with ex ->
              Log.error "next offset failed"
              return raise ex }
      
        return fetchStream initOffset }
     
      let! partitionStreams =
        state.assignments
        |> Seq.map (fun a -> async {
          let! stream = consumePartition (a.topic,a.partition,a.initOffset)
          return a.topic,a.partition,stream })
        |> Async.Parallel

      return partitionStreams }

    /// Emits generations of the consumer group protocol.
    let rec generations (prevState:GenerationState option) = asyncSeq {
      let! state = join (prevState |> Option.map (fun s -> s.memberId))
      let! topics = consume state
      yield state.generationId,topics
      do! state.closed.Task |> Async.AwaitTask
      yield! generations (Some state) }
                   
    yield! generations None }

  
  let callback 
    (handler:MessageSet * Async<unit> -> Async<unit>) 
    (consumer:AsyncSeq<GenerationId * (TopicName * Partition * AsyncSeq<MessageSet * Async<unit>>)[]>) : Async<unit> = async {
      return!
        consumer
        |> AsyncSeq.iterAsync (fun (_generationId,topics) -> async {          
          return!
            topics
            |> Seq.map (fun (_tn,_p,stream) -> async {              
              return! stream |> AsyncSeq.iterAsync handler })
            |> Async.Parallel
            |> Async.Ignore }) }

  let callbackCommit
    (commit:MessageSet * Async<unit> -> Async<unit>)
    (handler:MessageSet -> Async<unit>) =
    callback (fun (ms,c) -> async {
      do! handler ms
      do! commit (ms,c) })

  let callbackCommitAfter =
    callbackCommit (fun (_,commit) -> commit)

  let callbackCommitAsync =
    callbackCommit (fun (_,commit) -> async { return Async.Start commit })
    

  