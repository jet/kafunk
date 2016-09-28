namespace Kafunk

open System
open System.Threading

open Kafunk

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
        autoOffsetReset = AutoOffsetReset.Anything
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

and AutoOffsetReset =
  | Smallest
  | Largest
  | Disable
  | Anything



/// High-level consumer API.
module Consumer =

  let private Log = Log.create "Kafunk.Consumer"
  
  /// Stats corresponding to a single generation of the consumer group protocol.
  type ConsumerState = {
    generationId : GenerationId
    memberId : MemberId
    memberAssignment : ConsumerGroupMemberAssignment
    cancellationToken : CancellationToken
  } with
    static member topicPartitions (s:ConsumerState) =
      s.memberAssignment.partitionAssignment.assignments  


  /// Given a consumer configuration, initiates the consumer group protocol.
  /// Returns an async sequence of states where each state corresponds to a
  /// generation in the group protocol. The state contains streams for the
  /// topics specified in the configuration. Whenever there is a change in
  /// the consumer group, or a failure, the protocol restarts and returns a
  /// new generation once successful. If there are failures surpassing
  /// configured thresholds, the resulting sequence throws an exception.
  let consume (conn:KafkaConn) (cfg:ConsumerConfig) = async {
    
    //let _ = conn.CancellationToken.Register (fun () -> Log.info "connection_cancelled")

    let cts = CancellationTokenSource.CreateLinkedTokenSource conn.CancellationToken
    let state : MVar<ConsumerState> = MVar.create ()

    let states = asyncSeq {
      while true do
        let! s = MVar.take state
        yield s }

    // TODO: configurable
    let protocolType = ProtocolType.consumer
    let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, Binary.empty)
    let assignmentStrategy : AssignmentStrategy = "range" // roundrobin
    let groupProtocols =
      GroupProtocols(
        [| assignmentStrategy, toArraySeg ConsumerGroupProtocolMetadata.size ConsumerGroupProtocolMetadata.write consumerProtocolMeta |])


    let rec join (prevState:ConsumerState option) = async {

      Log.info "initializing_consumer|group_id=%s" cfg.groupId

      let cts = CancellationTokenSource.CreateLinkedTokenSource cts.Token
      let! _ = conn.GetGroupCoordinator (cfg.groupId)

      let! joinGroupRes =
        let initMemberId = defaultArg (prevState |> Option.map (fun s -> s.memberId)) ""
        let joinGroupReq = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, initMemberId, protocolType, groupProtocols)
        Kafka.joinGroup conn joinGroupReq
      
      Log.info "join_group_response|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s" 
        cfg.groupId 
        joinGroupRes.memberId 
        joinGroupRes.generationId 
        joinGroupRes.leaderId
        joinGroupRes.groupProtocol
        

      let close () = async {
        Log.info "closing_consumer_group|group_id=%s member_id=%s generation_id=%i leader_id=%s member_count=%i" 
          cfg.groupId 
          joinGroupRes.memberId 
          joinGroupRes.generationId 
          joinGroupRes.leaderId
          joinGroupRes.members.members.Length
        return cts.Cancel() }

      let rec heartbeat () = async {
        let! ct = Async.CancellationToken
        if ct.IsCancellationRequested then
          Log.info "stopping_heartbeats"
          return ()
        else
          Log.trace "sending_heartbeat|cancelled=%b" ct.IsCancellationRequested
          let req = HeartbeatRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId)
          let! res = Kafka.heartbeat conn req
          match res.errorCode with
          | ErrorCode.IllegalGenerationCode ->
            do! close ()
            return ()
          | _ ->
            do! Async.Sleep (cfg.sessionTimeout / cfg.heartbeatFrequency)
            return! heartbeat () }
            
      let! topicPartitions = conn.GetMetadata cfg.topics

      let! syncGroupRes = async {
        if joinGroupRes.members.members.Length > 0 then          
          Log.info "joined_as_leader;assigning_partitions"
          let members = joinGroupRes.members.members
          
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
              memberId, (toArraySeg ConsumerGroupMemberAssignment.size ConsumerGroupMemberAssignment.write assignment)
            )
                      
          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment(memberAssignments))
          let! res = Kafka.syncGroup conn req
          return res
        else
          Log.info "joined_as_follower;awaiting_assignment"
          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
          let! res = Kafka.syncGroup conn req
          return res }
      
      let assignment,_ = 
        ConsumerGroupMemberAssignment.read syncGroupRes.memberAssignment
      
      Log.info "received_sync_group_response|member_assignment=[%s]"
        (String.concat ", " (assignment.partitionAssignment.assignments |> Seq.map (fun (tn,ps) -> sprintf "topic=%s partitions=%A" tn ps))) 
      
      Log.info "starting_heartbeats|heartbeat_frequency=%i session_timeout=%i" cfg.heartbeatFrequency cfg.sessionTimeout
      Async.Start (heartbeat (), cts.Token)
            
      let state' =
        {
          memberId = joinGroupRes.memberId
          generationId = joinGroupRes.generationId
          memberAssignment = assignment
          cancellationToken = cts.Token
        }

      do! state |> MVar.put state'

      return () }
              
    let consume state = asyncSeq {
        
      let memberId = state.memberId
      let generationId = state.generationId
           
      let stream (topic:TopicName, partition:Partition) : Async<AsyncSeq<MessageSet * Async<unit>>> = async {

        let fetchOffset () = async {
          Log.info "fetching_group_member_offset|topic=%s partition=%i group_id=%s time=%i" topic partition cfg.groupId cfg.initialFetchTime
          try
            let req = OffsetFetchRequest(cfg.groupId, [| topic, [| partition |] |])
            let! res = Kafka.offsetFetch conn req                                          
            let _topic,ps = res.topics.[0]
            let (_p,offset,_metadata,_ec) = ps.[0]
            if offset = -1L then
              Log.info "offset_not_available_at_group_coordinator|group_id=%s member_id=%s topic=%s partition=%i generation=%i" cfg.groupId memberId topic partition generationId
              let offsetReq = OffsetRequest(-1, [| topic, [| partition,cfg.initialFetchTime,1 |] |])
              let! offsetRes = Kafka.offset conn offsetReq
              let _,ps = offsetRes.topics.[0]
              return ps.[0].offsets.[0]
            else
              return offset
          with ex ->
            Log.error "fetch_offset_error|error=%O" ex
            //do! close ()
            return raise ex }

        let! initOffset = fetchOffset ()

        Log.info "fetched_initial_offset|topic=%s partition=%i offset=%i group_id=%s member_id=%s generation_id=%i" 
          topic
          partition
          initOffset
          cfg.groupId
          memberId
          generationId

        let commitOffset (offset:Offset) = async {
          Log.trace "committing_offset|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i retention=%i" topic partition cfg.groupId memberId generationId offset cfg.offsetRetentionTime
          let req = OffsetCommitRequest(cfg.groupId, generationId, memberId, cfg.offsetRetentionTime, [| topic, [| partition, offset, "" |] |])
          let! res = Kafka.offsetCommit conn req
          if res.topics.Length > 0 then
            let (tn,ps) = res.topics.[0]
            let (p,ec) = ps.[0]
            Log.trace "offset_comitted|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i" tn p cfg.groupId memberId generationId offset
            return ()
          else          
            Log.error "offset_committ_failed|topic=%s partition=%i group_id=%s member_id=%s generation_id=%i offset=%i" topic partition cfg.groupId memberId generationId offset
            return failwith "offset commit failed!" }
            //return () }

        let rec fetchStream (offset:FetchOffset) = asyncSeq {
          //Log.trace "sending_fetch_request|topic=%s partition=%i member_id=%s offset=%i" topic partition memberId offset
          
          let rec fetchRange offset = async {
            let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [| partition, offset, cfg.fetchBufferBytes |] |])
            let! res = Kafka.fetch conn req
            let _,partitions = res.topics.[0]
            let _,_ec,highWatermarkOffset,_mss,ms = partitions.[0]
            if ms.messages.Length = 0 then
              Log.info "reached_end_of_stream|topic=%s partition=%i offset=%i high_watermark_offset=%i" topic partition offset highWatermarkOffset
              do! Async.Sleep 10000
              return! fetchRange offset 
            else
              //let ms = Compression.decompress ms
              //let commit = commitOffset (offset)
              return res }
          
          let! res = fetchRange offset
          //let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [| partition, offset, cfg.fetchBufferBytes |] |])
          //let! res = Kafka.fetch conn req
          let _,partitions = res.topics.[0]
          let _,_ec,highWatermarkOffset,_mss,ms = partitions.[0]
          //let ms = Compression.decompress ms
          let commit = commitOffset offset
          yield ms,commit

          // TODO: ensure monotonic?                                                          
          let nextOffset =MessageSet.nextOffset ms
          if nextOffset <= highWatermarkOffset then
            yield! fetchStream nextOffset
          else
            Log.error "offset_calculation_errored|next_offset=%i high_watermark_offset=%i" nextOffset highWatermarkOffset
            do! Async.Sleep -1 }

        return fetchStream initOffset }
        
      let! partitionStreams =
        ConsumerState.topicPartitions state
        |> Seq.collect (fun (tn,ps) ->
          ps
          |> Seq.map (fun p -> async {
            let! stream = stream (tn,p)
            return tn,p,stream }))
        |> Async.Parallel
            
      yield generationId,partitionStreams }


    let! _ = join None

    return states |> AsyncSeq.collect consume
         
  }