namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open Kafunk

/// The group protocol.
type GroupProtocol = {

  /// The group protocol type.
  protocolType : ProtocolType

  /// The protocols (e.g. versions).
  protocols : Async<(ProtocolName * ProtocolMetadata)[]>

  /// Called by the leader to assign member specific states given a selected protocol name.
  assign : KafkaConn -> ProtocolName -> (MemberId * ProtocolMetadata)[] -> Async<(MemberId * MemberAssignment)[]>

}

/// Group member configuration.
type GroupConfig = {
  
  /// The group id shared by members in the group.
  groupId : GroupId
    
  /// The session timeout period, in milliseconds, such that if no heartbeats are received within the
  /// period, a group members is ejected from the group.
  /// Default: 20000
  sessionTimeout : SessionTimeout
  
  /// The time during which a member must rejoin a group after a rebalance.
  /// If the member doesn't rejoin within this time, it will be ejected.
  /// Supported in v0.10.1.
  /// Default: 20000
  rebalanceTimeout : RebalanceTimeout

  /// The number of times to send heartbeats within a session timeout period.
  /// Default: 10
  heartbeatFrequency : int32

  /// The group protocol.
  protocol : GroupProtocol

}

/// Internal state corresponding to a single generation of the consumer group protocol.
type GroupMemberState = {  
  
  /// The group generation.
  generationId : GenerationId
  
  /// The member id.
  memberId : MemberId
  
  /// The member id of the group leader.
  leaderId : LeaderId  
  
  /// Leader assigned member state
  state : MemberAssignment

} 

type internal GroupMemberStateWrapper = {  
  state : GroupMemberState
  closed : TaskCompletionSource<bool>
} 

/// A member of a group.
type GroupMember = internal {
  conn : KafkaConn
  config : GroupConfig
  state : MVar<GroupMemberStateWrapper>
}

/// Operations on Kafka groups.
[<Compile(Module)>]
module Group = 
  
  let private Log = Log.create "Kafunk.Group"

  /// Checks if the group state is closed, in which cases it evaluates f.
  /// Otherwise, evaluates the async computation.
  let internal tryAsync (state:GroupMemberStateWrapper) (f:bool -> 'a) (a:Async<'a>) : Async<'a> = async {
    let t = state.closed.Task
    if t.IsCompleted then return f t.Result
    else return! a }
    
  /// Closes a consumer group specifying whether a new generation should begin.
  let internal close (state:GroupMemberStateWrapper) (rejoin:bool) : Async<unit> =
    tryAsync
      state
      ignore      
      (async {
        if rejoin then
          if state.closed.TrySetResult rejoin then
            Log.warn "closing_generation|generation_id=%i member_id=%s leader_id=%s" state.state.generationId state.state.memberId state.state.leaderId
        else
          if state.closed.TrySetResult rejoin then
            Log.warn "closing_consumer|generation_id=%i member_id=%s leader_id=%s" state.state.generationId state.state.memberId state.state.leaderId
        })

  /// Closes a consumer group and causes a new generation to begin.
  let internal closeGeneration (state:GroupMemberStateWrapper) : Async<unit> =
    close state true

  /// Closes a consumer group and causes a new generation to begin.
  let internal closeConsumer (state:GroupMemberStateWrapper) : Async<unit> =
    close state false

  let internal state (gm:GroupMember) = 
    gm.state |> MVar.get

  /// Joins a group.
  let join (gm:GroupMember) (prevState:GroupMemberState option) = async {
      
    let conn = gm.conn
    let cfg = gm.config
    let groupId = cfg.groupId
    let protocolType = cfg.protocol.protocolType
    let! protocols = cfg.protocol.protocols
    let protocolNames = protocols |> Array.map (fun (n,_) -> n)
    let sessionTimeout = cfg.sessionTimeout
    let rebalanceTimeout = cfg.rebalanceTimeout
    
    /// Attempts the initial group handshake.
    let join (prevMemberId:MemberId option) = async {
      let! _ = conn.GetGroupCoordinator groupId
      let req = JoinGroup.Request(groupId, sessionTimeout, rebalanceTimeout, defaultArg prevMemberId "", protocolType, GroupProtocols(protocols))
      let! res = Kafka.joinGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode                               
      | ErrorCode.NoError ->
        return Success res      
      | _ -> 
        return failwithf "unknown_join_group_error=%i" res.errorCode  }
  
    /// Synchronizes the group after all members have joined.
    let syncGroupLeader (joinGroupRes:JoinGroup.Response) = async {
                   
      let! memberAssignments = 
        cfg.protocol.assign conn joinGroupRes.groupProtocol joinGroupRes.members.members

      let req = SyncGroupRequest(groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment(memberAssignments))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ -> 
        return failwithf "unknown syncgroup error_code=%i" res.errorCode }

    let syncGroupFollower (joinGroupRes:JoinGroup.Response) = async {
      let req = SyncGroupRequest(groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ ->
        return failwithf "unknown syncgroup error_code=%i" res.errorCode }
    
    /// Starts the heartbeating process to remain in the group.
    let hearbeat (joinGroupRes:JoinGroup.Response, syncGroupRes:SyncGroupResponse) = async {

      let heartbeatSleep = cfg.sessionTimeout / cfg.heartbeatFrequency

      Log.info "starting_group_heartbeats|group_id=%s generation_id=%i member_id=%s heartbeat_frequency=%i session_timeout=%i heartbeat_sleep=%i" 
        groupId joinGroupRes.generationId joinGroupRes.memberId cfg.heartbeatFrequency cfg.sessionTimeout heartbeatSleep

      let state =
        {
          state =  
            {
              memberId = joinGroupRes.memberId
              leaderId = joinGroupRes.leaderId
              generationId = joinGroupRes.generationId
              state = syncGroupRes.memberAssignment 
            }
          closed = TaskCompletionSource<bool>()
        }
          
      conn.CancellationToken.Register (fun () ->
        Log.info "closing_consumer_group_on_connection_close" 
        closeConsumer state |> Async.Start) |> ignore
      
      /// The hearbeat process.
      let rec heartbeat (state:GroupMemberStateWrapper) =
        tryAsync
          state
          ignore            
          (async {
            let req = HeartbeatRequest(cfg.groupId, state.state.generationId, state.state.memberId)
            let! res = Kafka.heartbeat conn req |> Async.Catch
            match res with
            | Success res ->
              match res.errorCode with
              | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
                do! closeGeneration state
                return ()
              | _ ->
                do! Async.Sleep heartbeatSleep
                return! heartbeat state
            | Failure ex ->
              Log.warn "heartbeat_failure|generation_id=%i error=%O" state.state.generationId ex
              do! closeConsumer state
              return () })

      Async.Start (heartbeat state)
      let! _ = gm.state |> MVar.put state
      return () }


    /// Joins a group, syncs the group, starts the heartbeat process.
    let rec joinSyncHeartbeat (prevMemberId:MemberId option) = async {
      
      match prevMemberId with
      | None -> 
        Log.info "joining_group|group_id=%s protocol_type=%s protocol_names=%A" groupId protocolType protocolNames
      | Some prevMemberId -> 
        Log.info "rejoining_group|group_id=%s protocol_type=%s protocol_names=%A member_id=%s" groupId protocolType protocolNames prevMemberId

      let! joinGroupRes = join prevMemberId
      match joinGroupRes with
      | Success joinGroupRes ->        
                
        if joinGroupRes.members.members.Length > 0 then
          Log.info "joined_group_as_leader|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId // redundant, but good for logs
            joinGroupRes.groupProtocol
          let! syncGroupRes = syncGroupLeader joinGroupRes
          match syncGroupRes with
          | Success syncGroupRes ->
            return! hearbeat (joinGroupRes,syncGroupRes)
          
          | Failure ec ->
            Log.warn "sync_group_error|error_code=%i" ec
            return! joinSyncHeartbeat prevMemberId
        
        else
          Log.info "joined_group_as_follower|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol          
          let! syncGroupRes = syncGroupFollower joinGroupRes
          match syncGroupRes with
          | Success syncGroupRes ->
            return! hearbeat (joinGroupRes,syncGroupRes)
          
          | Failure ec ->
            Log.warn "sync_group_error|error_code=%i" ec
            return! joinSyncHeartbeat prevMemberId

      | Failure joinGroupErr ->
        Log.warn "join_group_error|error_code=%i" joinGroupErr
        match joinGroupErr with
        | ErrorCode.UnknownMemberIdCode -> 
          Log.warn "resetting_member_id"
          return! joinSyncHeartbeat None
        | _ -> 
          return! joinSyncHeartbeat prevMemberId }

    return! joinSyncHeartbeat (prevState |> Option.map (fun s -> s.memberId)) }

  let internal generations (gm:GroupMember) =    
    let rec loop () = asyncSeq {
      let! state = state gm
      yield state
      let! rejoin = state.closed.Task |> Async.AwaitTask
      if rejoin then
        let! _ = join gm (Some state.state)
        yield! loop () }
    loop ()

  let createJoin (conn:KafkaConn) (config:GroupConfig) = async {
    let gm = 
      { GroupMember.conn = conn
        state = MVar.create ()
        config = config }
    let! _ = join gm None
    return gm }

  let leave (gm:GroupMember) = async {
    let! state = MVar.get gm.state
    let req = LeaveGroupRequest(gm.config.groupId, state.state.memberId)
    let! res = Kafka.leaveGroup gm.conn req
    match res.errorCode with
    | ErrorCode.NoError -> return ()
    | _ ->  return () }

