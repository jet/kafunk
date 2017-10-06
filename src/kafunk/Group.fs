namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open Kafunk


/// Internal state corresponding to a single generation of the group protocol.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type GroupMemberState = {
  
  /// The group generation.
  generationId : GenerationId
  
  /// The member id.
  memberId : MemberId
  
  /// The member id of the group leader.
  leaderId : LeaderId
  
  /// The members of the group.
  /// Available only to the leader.
  members : (MemberId * ProtocolMetadata)[]

  /// Leader assigned member state
  memberAssignment : MemberAssignment

  /// The selected protocol.
  protocolName : ProtocolName

  /// Cancelled when the group is closed.
  closed : CancellationToken

} 

/// A group protocol.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type GroupProtocol = {

  /// The group protocol type.
  protocolType : ProtocolType

  /// Returns the supported protocols (ie versions) alogn with protocol metadata.
  protocols : GroupMember -> GroupMemberState option -> Async<(ProtocolName * ProtocolMetadata)[]>

  /// Called by the leader to assign member specific states given a the previous state, if any, and the selected protocol name.
  assign : GroupMember -> GroupMemberState option -> ProtocolName -> (MemberId * ProtocolMetadata)[] -> Async<(MemberId * MemberAssignment)[]>

}

/// Group member configuration.
and GroupConfig = {
  
  /// The group id shared by members in the group.
  groupId : GroupId
    
  /// The group protocol.
  protocol : GroupProtocol

  /// The session timeout period, in milliseconds, such that if no heartbeats are received within the
  /// period, a group members is ejected from the group.
  sessionTimeout : SessionTimeout
  
  /// The time during which a member must rejoin a group after a rebalance.
  /// If the member doesn't rejoin within this time, it will be ejected.
  /// Supported in v0.10.1.
  rebalanceTimeout : RebalanceTimeout

  /// The number of times to send heartbeats within a session timeout period.
  /// Default: 10
  heartbeatFrequency : int32

}

/// A member of a group.
and [<NoEquality;NoComparison;AutoSerializable(false)>] GroupMember = internal {
  conn : KafkaConn
  config : GroupConfig
  state : SVar<GroupMemberStateWrapper>
}

/// The action to take upon leaving the group.
and internal GroupLeaveAction = 
  
  /// Leave without rejoin.
  | LeaveGroup
  
  /// Leave and rejoin.
  | LeaveAndRejoin of ec:ErrorCode

and internal GroupMemberStateWrapper = {
  state : GroupMemberState
  closed : IVar<GroupLeaveAction>
} 

/// Operations on Kafka groups.
[<Compile(Module)>]
module Group = 
  
  let private Log = Log.create "Kafunk.Group"

  /// Checks if the group state is closed, in which cases it evaluates f.
  /// Otherwise, evaluates the async computation.
  /// NB: this does not handle scenarios when the group state is closed shortly after the async computation is invoked.
  let internal tryAsync (state:GroupMemberStateWrapper) (f:GroupLeaveAction -> 'a) (a:Async<'a>) : Async<'a> = async {
    let t = state.closed.Task
    if t.IsCompleted then return f t.Result
    else return! a }
    
  /// Closes a group specifying whether a new generation should begin.
  let private leaveGroup (gm:GroupMember) (state:GroupMemberStateWrapper) (action:GroupLeaveAction) : Async<bool> =
    tryAsync
      state
      (fun _ -> false)
      (async {
        match action with
        | GroupLeaveAction.LeaveAndRejoin ec ->
          if IVar.tryPut action state.closed then
            Log.info "leaving_group_to_rejoin|conn_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s error_code=%i"
              gm.conn.Config.connId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId ec
            return true
          else
            return false
        | GroupLeaveAction.LeaveGroup ->
          if IVar.tryPut action state.closed then
            Log.info "leaving_group|conn_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s" 
              gm.conn.Config.connId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId
            return true
          else
            return false
        })

  /// Closes a generation and causes a new generation to begin.
  let internal leaveAndRejoin (gm:GroupMember) (state:GroupMemberStateWrapper) (ec:ErrorCode) : Async<unit> =
    leaveGroup gm state (GroupLeaveAction.LeaveAndRejoin ec) |> Async.Ignore

  /// Closes a group and sends a leave group request to Kafka.
  let internal leaveInternal (gm:GroupMember) (state:GroupMemberStateWrapper) : Async<unit> = async {
    let! _ = leaveGroup gm state (GroupLeaveAction.LeaveGroup)
    let req = LeaveGroupRequest(gm.config.groupId, state.state.memberId)
    let! res = Kafka.leaveGroup gm.conn req
    match res.errorCode with
    | ErrorCode.NoError | ErrorCode.UnknownMemberIdCode | ErrorCode.GroupLoadInProgressCode -> 
      return ()
    | ec -> 
      Log.error "group_leave_error|conn_id=%s group_id=%s member_id=%s error_code=%i" 
        gm.conn.Config.connId gm.config.groupId state.state.memberId ec
      return () }

  /// Leaves a group, sending a leave group request to Kafka.
  let leave (gm:GroupMember) = async {
    let! state = SVar.get gm.state
    return! leaveInternal gm state }

  let internal stateInternal (gm:GroupMember) = 
    gm.state |> SVar.get

  /// Returns the group member state.
  let state (gm:GroupMember) = async {
    let! state = stateInternal gm
    return state.state }

  /// Returns a stream of group member states as of the invocation, including the current state.
  let states (gm:GroupMember) =
    gm.state
    |> SVar.tap
    |> AsyncSeq.map (fun s -> s.state)

  /// Joins a group.
  let join (gm:GroupMember) (prevState:GroupMemberState option, prevErrorCode:ErrorCode option) = async {
      
    let conn = gm.conn
    let cfg = gm.config
    let groupId = cfg.groupId
    let protocolType = cfg.protocol.protocolType
    let! protocols = cfg.protocol.protocols gm prevState
    let protocolNames = protocols |> Array.map (fun (n,_) -> n)
    let sessionTimeout = cfg.sessionTimeout
    let rebalanceTimeout = cfg.rebalanceTimeout
    let groupSyncErrorRetryTimeoutMs = 1000 // TODO: retry policy
    let groupJoinMaxAttempts = 10

    /// Attempts the initial group handshake.
    let join (prevMemberId:MemberId option) = async {      
      let req = JoinGroup.Request(groupId, sessionTimeout, rebalanceTimeout, defaultArg prevMemberId "", protocolType, GroupProtocols(protocols))
      let! res = Kafka.joinGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode 
      | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ -> 
        return failwithf "unknown_join_group_error=%i" res.errorCode  }
  
    /// Synchronizes the group after all members have joined.
    let syncGroupLeader (joinGroupRes:JoinGroup.Response) = async {                   
      let! memberAssignments = cfg.protocol.assign gm prevState joinGroupRes.groupProtocol joinGroupRes.members.members
      let req = SyncGroupRequest(groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment(memberAssignments))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode 
      | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ -> 
        return failwithf "unknown syncgroup error_code=%i" res.errorCode }

    let syncGroupFollower (joinGroupRes:JoinGroup.Response) = async {
      let req = SyncGroupRequest(groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment([||]))
      let! res = Kafka.syncGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode 
      | ErrorCode.NotCoordinatorForGroupCode ->
        return Failure res.errorCode
      | ErrorCode.NoError ->
        return Success res
      | _ ->
        return failwithf "unknown syncgroup error_code=%i" res.errorCode }
    
    /// Starts the heartbeating process to remain in the group.
    let hearbeat (groupCoord:Broker, joinGroupRes:JoinGroup.Response, syncGroupRes:SyncGroupResponse) = async {

      let heartbeatSleepMs = cfg.sessionTimeout / cfg.heartbeatFrequency

      Log.info "starting_heartbeat_process|conn_id=%s group_id=%s generation_id=%i member_id=%s heartbeat_frequency=%i session_timeout=%i heartbeat_sleep=%i group_coord=%s" 
        conn.Config.connId groupId joinGroupRes.generationId joinGroupRes.memberId cfg.heartbeatFrequency cfg.sessionTimeout heartbeatSleepMs (Broker.endpoint groupCoord)

      let state =
        let closed = IVar.create ()
        {
          state =
            {
              memberId = joinGroupRes.memberId
              leaderId = joinGroupRes.leaderId
              generationId = joinGroupRes.generationId
              memberAssignment = syncGroupRes.memberAssignment 
              protocolName = joinGroupRes.groupProtocol
              members = joinGroupRes.members.members
              closed = IVar.asCancellationToken closed
            }
          closed = closed
        }
          
      conn.CancellationToken.Register (fun () ->
        Log.info "leaving_group_on_connection_close|conn_id=%s group_id=%s member_id=%s generation_id=%i" 
          conn.Config.connId gm.config.groupId state.state.memberId state.state.generationId 
        Async.Start (leave gm)) |> ignore
      
      /// Sends a heartbeat.
      let heartbeat (count:int) (state:GroupMemberStateWrapper) =
        tryAsync
          state
          (konst false)
          (async {
            Log.trace "sending_heartbeat|conn_id=%s group_id=%s generation=%i member_id=%s n=%i"
              conn.Config.connId cfg.groupId state.state.generationId state.state.memberId count
            let req = HeartbeatRequest(cfg.groupId, state.state.generationId, state.state.memberId)
            let! res = Kafka.heartbeat conn req |> Async.Catch
            match res with
            | Success res ->
              match res.errorCode with
              | ErrorCode.NoError -> 
                return true
              | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode 
              | ErrorCode.NotCoordinatorForGroupCode ->
                Log.warn "heartbeat_error|conn_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s error_code=%i heartbeat_count=%i group_coord=%s" 
                  conn.Config.connId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId res.errorCode count (Broker.endpoint groupCoord)
                do! leaveAndRejoin gm state res.errorCode
                return false
              | ec ->
                return failwithf "unknown_heartbeat_error|conn_id=%s error_code=%i" 
                  conn.Config.connId ec
            | Failure ex ->
              Log.warn "heartbeat_exception|conn_id=%s group_id=%s generation_id=%i group_coord=%s error=\"%O\"" 
                conn.Config.connId gm.config.groupId state.state.generationId (Broker.endpoint groupCoord) ex
              do! leaveInternal gm state
              return false })

      let heartbeatProcess = async {
        let i = ref 0
        while true do
          Async.Start (heartbeat !i state |> Async.Ignore, state.state.closed)
          incr i
          do! Async.Sleep heartbeatSleepMs }

      let! ct = Async.CancellationToken
      let cts = CancellationTokenSource.CreateLinkedTokenSource (ct, state.state.closed)
      Async.Start (heartbeatProcess, cts.Token)

      let! _ = gm.state |> SVar.put state
      return () }


    /// Joins a group, syncs the group, starts the heartbeat process.
    let rec joinSyncHeartbeat (rs:RetryState) (prevMemberId:MemberId option, prevErrorCode:ErrorCode option) = async {     
      
      Log.info "getting_group_coordinator|conn_id=%s group_id=%s protocol_type=%s protocol_names=%A"
        conn.Config.connId groupId protocolType protocolNames

      let! groupCoord = conn.GetGroupCoordinator groupId
            
      if rs.attempt > groupJoinMaxAttempts then 
        failwithf "join_group_failed|group_coord=%O attempt=%i" (Broker.endpoint groupCoord) rs.attempt

      match prevMemberId with
      | None -> 
        Log.info "joining_group|conn_id=%s group_id=%s protocol_type=%s protocol_names=%A group_coord=%s" 
          conn.Config.connId groupId protocolType protocolNames (Broker.endpoint groupCoord)
      | Some prevMemberId -> 
        Log.info "rejoining_group|conn_id=%s group_id=%s protocol_type=%s protocol_names=%A member_id=%s group_coord=%s"
          conn.Config.connId groupId protocolType protocolNames prevMemberId (Broker.endpoint groupCoord)

      let prevMemberId = 
        match prevErrorCode with
        | Some ec when ec = ErrorCode.UnknownMemberIdCode -> 
          Log.warn "resetting_member_id|conn_id=%s group_id=%s error_code=%i prev_member_id=%A"
            conn.Config.connId groupId ec prevMemberId
          None
        | _ -> prevMemberId

      let! joinGroupRes = join prevMemberId
      match joinGroupRes with
      | Success joinGroupRes ->
                
        if joinGroupRes.members.members.Length > 0 then
          Log.info "joined_group_as_leader|conn_id=%s group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            conn.Config.connId
            groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
          let! syncGroupRes = syncGroupLeader joinGroupRes
          match syncGroupRes with
          | Success syncGroupRes ->
            return! hearbeat (groupCoord,joinGroupRes,syncGroupRes)
          
          | Failure ec ->
            Log.warn "sync_group_error|conn_id=%s group_id=%s error_code=%i" 
              conn.Config.connId groupId ec
            do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
            return! joinSyncHeartbeat (RetryState.next rs) (prevMemberId, Some ec)
        
        else
          Log.info "joined_group_as_follower|conn_id=%s group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            conn.Config.connId
            groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
          let! syncGroupRes = syncGroupFollower joinGroupRes
          match syncGroupRes with
          | Success syncGroupRes ->
            return! hearbeat (groupCoord,joinGroupRes,syncGroupRes)
          
          | Failure ec ->
            Log.warn "sync_group_error|conn_id=%s group_id=%s error_code=%i" 
              conn.Config.connId groupId ec
            do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
            return! joinSyncHeartbeat (RetryState.next rs) (prevMemberId, Some ec)

      | Failure joinGroupErr ->
        Log.warn "join_group_error|conn_id=%s group_id=%s error_code=%i prev_member_id=%A" 
          conn.Config.connId groupId joinGroupErr prevMemberId
        do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
        return! joinSyncHeartbeat (RetryState.next rs) (prevMemberId, Some joinGroupErr)  }

    return! joinSyncHeartbeat RetryState.init (prevState |> Option.map (fun s -> s.memberId), prevErrorCode) }

//  let internal generationsInternal (gm:GroupMember) =
//    SVar.tap gm.state

  let internal generationsInternal (gm:GroupMember) =
    let rec loop () = asyncSeq {
      let! state = stateInternal gm
      yield state
      // NB: this will only be reached once consumer is done processing @state
      // unless the sequence is explicitly read ahead. this is acceptable in this case
      // but may be unexpected.
      let! action = IVar.get state.closed
      match action with
      | GroupLeaveAction.LeaveAndRejoin ec ->
        let! _ = join gm (Some state.state, Some ec)
        yield! loop ()
      | _ -> () }
    loop ()

  /// Returns generations of the group protocol.
  let generations (gm:GroupMember) =
    gm
    |> generationsInternal
    |> AsyncSeq.map (fun state -> state.state)

  /// Creates a group member and joins it to the group.
  let createJoin (conn:KafkaConn) (config:GroupConfig) = async {
    
    let groupMember = 
      { GroupMember.conn = conn
        state = SVar.create ()
        config = config }

    let! _ = join groupMember (None, None)
    
//    let rec rejoinProc () = async {
//      let! state = stateInternal groupMember
//      let! action = IVar.get state.closed
//      match action with
//      | GroupLeaveAction.LeaveAndRejoin ec ->
//        let! _ = join groupMember (Some state.state, Some ec)
//        return! rejoinProc ()
//      | GroupLeaveAction.LeaveGroup -> 
//        return () }
//    
//    let! _ = Async.StartChild (rejoinProc ())

    return groupMember }

