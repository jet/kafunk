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
  let consume (conn:KafkaConn) (cfg:ConsumerConfig) = asyncSeq {
    
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

      Log.info "initializing consumer group_id=%s" cfg.groupId

      let! _ = conn.GetGroupCoordinator (cfg.groupId)

      let cts = new CancellationTokenSource()

      let! joinGroupRes =
        let initMemberId = defaultArg (prevState |> Option.map (fun s -> s.memberId)) ""
        let joinGroupReq = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, initMemberId, protocolType, groupProtocols)
        Kafka.joinGroup conn joinGroupReq
      
      Log.info "join group response|group_id=%s member_id=%s generation_id=%i leader_id=%s" 
        cfg.groupId 
        joinGroupRes.memberId 
        joinGroupRes.generationId 
        joinGroupRes.leaderId

      let close () = async {
        Log.info "closing consumer group|group_id=%s member_id=%s generation_id=%i leader_id=%s member_count=%i" 
          cfg.groupId 
          joinGroupRes.memberId 
          joinGroupRes.generationId 
          joinGroupRes.leaderId
          joinGroupRes.members.members.Length
        return cts.Cancel() }

      let rec heartbeat () = async {
        Log.trace "sending heartbeat|group_id=%s member_id=%s generation_id=%i" cfg.groupId joinGroupRes.memberId joinGroupRes.generationId
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
          Log.info "joined as leader, creating assignments..."
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
          Log.info "joined as follower, awaiting assignment...."
          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
          let! res = Kafka.syncGroup conn req
          return res }
      
      let assignment,_ = 
        ConsumerGroupMemberAssignment.read syncGroupRes.memberAssignment
      
      Log.info "received sync group response|member_assignment=[%s]"
        (String.concat ", " (assignment.partitionAssignment.assignments |> Seq.map (fun (tn,ps) -> sprintf "topic=%s partitions=%A" tn ps))) 

      Log.info "starting heartbeats..."
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
          
    let! _ = join None
    
    yield!
      states
      |> AsyncSeq.collect (fun state -> asyncSeq {
        
        let memberId = state.memberId
        let generationId = state.generationId
    
        let stream (topic:TopicName, partition:Partition) : Async<AsyncSeq<MessageSet * Async<unit>>> = async {

          let fetchOffset () = async {
            try
              let req = OffsetFetchRequest(cfg.groupId, [| topic, [| partition |] |])
              let! res = Kafka.offsetFetch conn req
              let _topic,ps = res.topics.[0]
              let (_p,offset,_metadata,_ec) = ps.[0]
              return offset
            with ex ->
              do! close ()
              return raise ex }

          let! initOffset = fetchOffset ()

          Log.info "fetched initial offset|topic=%s partition=%i offset=%i group_id=%s member_id=%s generation_id=%i" 
            topic
            partition
            initOffset
            cfg.groupId
            memberId
            generationId

          let commitOffset (offset:Offset) = async {
            let req = OffsetCommitRequest(cfg.groupId, generationId, memberId, cfg.offsetRetentionTime, [| topic, [|partition, offset, null|] |])
            let! res = Kafka.offsetCommit conn req
            if res.topics.Length > 0 then              
              ()
            return () }

          let rec go (offset:FetchOffset) = asyncSeq {
            let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [| partition, offset, cfg.fetchBuffer|] |])
            let! res = Kafka.fetch conn req
            let _,partitions = res.topics.[0]
            let _,ec,_hmo,_mss,ms = partitions.[0]
            if ec = ErrorCode.NoError then
              let nextOffset = MessageSet.nextOffset ms            
              let ms = Compression.decompress ms
              let commit = commitOffset (offset)
              yield ms,commit
              yield! go nextOffset
            else
              () }

          return go initOffset }
        
        let! partitionStreams =
          ConsumerState.topicPartitions state
          |> Seq.collect (fun (tn,ps) ->
            ps
            |> Seq.map (fun p -> async {
              let! stream = stream (tn,p)
              return tn,p,stream }))
          |> Async.Parallel
            
        yield generationId,partitionStreams })
         
  }