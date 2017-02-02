namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open Kafunk

/// Internal state corresponding to a single generation of the group protocol.
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
type GroupProtocol = {

  /// The group protocol type.
  protocolType : ProtocolType

  /// The protocols (e.g. versions).
  protocols : Async<(ProtocolName * ProtocolMetadata)[]>

  /// Called by the leader to assign member specific states given a the previous state, if any, and the selected protocol name.
  assign : KafkaConn -> GroupMemberState option -> ProtocolName -> (MemberId * ProtocolMetadata)[] -> Async<(MemberId * MemberAssignment)[]>

}

/// Group member configuration.
type GroupConfig = {
  
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

/// The action to take upon closing the group.
type internal GroupCloseAction = 
  
  /// Close the group entirely without rejoin.
  | LeaveGroup
  
  /// Close the generation and rejoin.
  | CloseGenerationAndRejoin of ec:ErrorCode


type internal GroupMemberStateWrapper = {
  state : GroupMemberState
  closed : IVar<GroupCloseAction>
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
  let internal tryAsync (state:GroupMemberStateWrapper) (f:GroupCloseAction -> 'a) (a:Async<'a>) : Async<'a> = async {
    let t = state.closed.Task
    if t.IsCompleted then return f t.Result
    else return! a }
    
  /// Closes a group specifying whether a new generation should begin.
  let private close (gm:GroupMember) (state:GroupMemberStateWrapper) (action:GroupCloseAction) : Async<bool> =
    tryAsync
      state
      (fun _ -> false)
      (async {
        match action with
        | GroupCloseAction.CloseGenerationAndRejoin ec ->
          if IVar.tryPut action state.closed then
            Log.warn "closing_group_generation|client_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s error_code=%i"
              gm.conn.Config.clientId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId ec
            return true
          else
            return false
        | GroupCloseAction.LeaveGroup ->
          if IVar.tryPut action state.closed then
            Log.warn "closing_group|client_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s" 
              gm.conn.Config.clientId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId
            return true
          else
            return false
        })

  /// Closes a generation and causes a new generation to begin.
  let internal closeGenerationAndRejoin (gm:GroupMember) (state:GroupMemberStateWrapper) (ec:ErrorCode) : Async<unit> =
    close gm state (GroupCloseAction.CloseGenerationAndRejoin ec) |> Async.Ignore

  /// Closes a group and sends a leave group request to Kafka.
  let internal leaveInternal (gm:GroupMember) (state:GroupMemberStateWrapper) : Async<unit> = async {
    let! _ = close gm state (GroupCloseAction.LeaveGroup)
    let req = LeaveGroupRequest(gm.config.groupId, state.state.memberId)
    let! res = Kafka.leaveGroup gm.conn req
    match res.errorCode with
    | ErrorCode.NoError | ErrorCode.UnknownMemberIdCode | ErrorCode.GroupLoadInProgressCode -> 
      return ()
    | ec -> 
      Log.error "group_leave_error|client_id=%s group_id=%s member_id=%s error_code=%i" 
        gm.conn.Config.clientId gm.config.groupId state.state.memberId ec
      return () }

  /// Leaves a group, sending a leave group request to Kafka.
  let leave (gm:GroupMember) = async {
    let! state = MVar.get gm.state
    return! leaveInternal gm state }

  let internal stateInternal (gm:GroupMember) = 
    gm.state |> MVar.get

  /// Returns the group member state.
  let state (gm:GroupMember) = async {
    let! state = stateInternal gm
    return state.state }

  /// Joins a group.
  let join (gm:GroupMember) (prevState:GroupMemberState option, prevErrorCode:ErrorCode option) = async {
      
    let conn = gm.conn
    let cfg = gm.config
    let groupId = cfg.groupId
    let protocolType = cfg.protocol.protocolType
    let! protocols = cfg.protocol.protocols
    let protocolNames = protocols |> Array.map (fun (n,_) -> n)
    let sessionTimeout = cfg.sessionTimeout
    let rebalanceTimeout = cfg.rebalanceTimeout
    let groupSyncErrorRetryTimeoutMs = 1000

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
        cfg.protocol.assign conn prevState joinGroupRes.groupProtocol joinGroupRes.members.members

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

      let heartbeatSleepMs = cfg.sessionTimeout / cfg.heartbeatFrequency

      Log.info "starting_heartbeat_process|client_id=%s group_id=%s generation_id=%i member_id=%s heartbeat_frequency=%i session_timeout=%i heartbeat_sleep=%i" 
        conn.Config.clientId groupId joinGroupRes.generationId joinGroupRes.memberId cfg.heartbeatFrequency cfg.sessionTimeout heartbeatSleepMs

      let closed = IVar.create ()

      let state =
        {
          state =
            {
              memberId = joinGroupRes.memberId
              leaderId = joinGroupRes.leaderId
              generationId = joinGroupRes.generationId
              memberAssignment = syncGroupRes.memberAssignment 
              protocolName = joinGroupRes.groupProtocol
              members = joinGroupRes.members.members
              closed = IVar.toCancellationToken closed
            }
          closed = closed
        }
          
      conn.CancellationToken.Register (fun () ->
        Log.info "closing_group_on_connection_close|client_id=%s group_id=%s member_id=%s generation_id=%i" 
          conn.Config.clientId gm.config.groupId state.state.memberId state.state.generationId 
        leave gm |> Async.Start) |> ignore
      
      /// Sends a heartbeat.
      let heartbeat (count:int) (state:GroupMemberStateWrapper) =
        tryAsync
          state
          ignore
          (async {
            let req = HeartbeatRequest(cfg.groupId, state.state.generationId, state.state.memberId)
            let! res = Kafka.heartbeat conn req |> Async.Catch
            match res with
            | Success res ->
              match res.errorCode with
              | ErrorCode.NoError -> 
                return ()
              | ErrorCode.IllegalGenerationCode | ErrorCode.UnknownMemberIdCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
                Log.warn "heartbeat_error|client_id=%s group_id=%s generation_id=%i member_id=%s leader_id=%s error_code=%i heartbeat_count=%i" 
                  conn.Config.clientId gm.config.groupId state.state.generationId state.state.memberId state.state.leaderId res.errorCode count
                do! closeGenerationAndRejoin gm state res.errorCode
                return ()
              | ec ->
                return failwithf "unknown_heartbeat_error|client_id=%s error_code=%i" 
                  conn.Config.clientId ec
            | Failure ex ->
              Log.warn "heartbeat_exception|client_id=%s group_id=%s generation_id=%i error=%O" 
                conn.Config.clientId gm.config.groupId state.state.generationId ex
              do! leaveInternal gm state
              return () })

      let heartbeatProcess =
        AsyncSeq.intervalMs heartbeatSleepMs
        |> AsyncSeq.skip 1
        |> AsyncSeq.mapiAsync (fun i _ -> async.Return i)
        |> AsyncSeq.iterAsyncParallel (fun i -> heartbeat (int i) state)

      let! ct = Async.CancellationToken
      let cts = CancellationTokenSource.CreateLinkedTokenSource (ct, state.state.closed)
      Async.Start (heartbeatProcess, cts.Token)

      let! _ = gm.state |> MVar.put state
      return () }


    /// Joins a group, syncs the group, starts the heartbeat process.
    let rec joinSyncHeartbeat (prevMemberId:MemberId option, prevErrorCode:ErrorCode option) = async {
      
      match prevMemberId with
      | None -> 
        Log.info "joining_group|client_id=%s group_id=%s protocol_type=%s protocol_names=%A" 
          conn.Config.clientId groupId protocolType protocolNames
      | Some prevMemberId -> 
        Log.info "rejoining_group|client_id=%s group_id=%s protocol_type=%s protocol_names=%A member_id=%s"
          conn.Config.clientId groupId protocolType protocolNames prevMemberId

      let prevMemberId = 
        match prevErrorCode with
        | Some ec when ec = ErrorCode.UnknownMemberIdCode -> 
          Log.warn "resetting_member_id|client_id=%s group_id=%s error_code=%i prev_member_id=%A"
            conn.Config.clientId groupId ec prevMemberId
          None
        | _ -> prevMemberId

      let! joinGroupRes = join prevMemberId
      match joinGroupRes with
      | Success joinGroupRes ->
                
        if joinGroupRes.members.members.Length > 0 then
          Log.info "joined_group_as_leader|client_id=%s group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            conn.Config.clientId
            groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
          let! syncGroupRes = syncGroupLeader joinGroupRes
          match syncGroupRes with
          | Success syncGroupRes ->
            return! hearbeat (joinGroupRes,syncGroupRes)
          
          | Failure ec ->
            Log.warn "sync_group_error|client_id=%s group_id=%s error_code=%i" 
              conn.Config.clientId groupId ec
            do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
            return! joinSyncHeartbeat (prevMemberId, Some ec)
        
        else
          Log.info "joined_group_as_follower|client_id=%s group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            conn.Config.clientId
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
            Log.warn "sync_group_error|client_id=%s group_id=%s error_code=%i" 
              conn.Config.clientId groupId ec
            do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
            //let prevMemberId = if ec = ErrorCode.UnknownMemberIdCode then None else prevMemberId
            return! joinSyncHeartbeat (prevMemberId, Some ec)

      | Failure joinGroupErr ->
        Log.warn "join_group_error|client_id=%s group_id=%s error_code=%i prev_member_id=%A" 
          conn.Config.clientId groupId joinGroupErr prevMemberId
        do! Async.Sleep groupSyncErrorRetryTimeoutMs // TODO: configure as RetryPolicy
        return! joinSyncHeartbeat (prevMemberId, Some joinGroupErr)  }

    return! joinSyncHeartbeat (prevState |> Option.map (fun s -> s.memberId), prevErrorCode) }

  let internal generationsInternal (gm:GroupMember) =
    let rec loop () = asyncSeq {
      let! state = stateInternal gm
      yield state
      // NB: this will only be reached once consumer is done processing @state
      // unless the sequence is explicitly read ahead. this is acceptable in this case
      // but may be unexpected.
      let! action = IVar.get state.closed
      match action with
      | GroupCloseAction.CloseGenerationAndRejoin ec ->
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
    let gm = 
      { GroupMember.conn = conn
        state = MVar.create ()
        config = config }
    let! _ = join gm (None, None)
    return gm }

