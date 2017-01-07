namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open Kafunk

type GroupConfig = {
  
  /// The group id shared by members in the group.
  groupId : GroupId
    
  /// The session timeout period, in milliseconds, such that if no heartbeats are received within the
  /// period, a group members is ejected from the consumer group.
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

  /// The supported group protocol, consisting of multiple version.
  groupProtocol : ProtocolType * (ProtocolName * ProtocolMetadata)[]

  /// Called by the leader to assign member specific states.
  assign : ProtocolName -> (MemberId * MemberMetadata)[] -> Async<(MemberId * MemberAssignment)[]>

}

/// Internal state corresponding to a single generation of the consumer group protocol.
type GroupMemberState = {  
  generationId : GenerationId
  memberId : MemberId
  leaderId : LeaderId  
  /// Leader assigned member state
  /// e.g. partitions for a consumer group
  state : MemberAssignment
} 

type internal GroupMemberStateWrapper = {  
  state : GroupMemberState
  closed : TaskCompletionSource<bool>
} 

type GroupMember = internal {
  conn : KafkaConn
  config : GroupConfig
  state : MVar<GroupMemberStateWrapper>
}

/// Operations on Kafka groups.
[<Compile(Module)>]
module Group = 
  
  let private Log = Log.create "Kafunk.Group"

  // TODO: refactor
  let private peekTask (f:'a -> 'b) (t:Task<'a>) (a:Async<'b>) = async {
    if t.IsCompleted then return f t.Result
    else return! a }

  /// Closes a consumer group specifying whether a new generation should begin.
  let internal close (state:GroupMemberStateWrapper) (rejoin:bool) : Async<unit> =
    peekTask
      ignore
      state.closed.Task
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
  let rec join (gm:GroupMember) (prevMemberId:MemberId option) = async {
      
    let conn = gm.conn
    let cfg = gm.config

    match prevMemberId with
    | None -> 
      Log.info "joining_consumer_group|group_id=%s" cfg.groupId

    | Some prevMemberId -> 
      Log.info "rejoining_consumer_group|group_id=%s member_id=%s" cfg.groupId prevMemberId

    let! _ = conn.GetGroupCoordinator cfg.groupId

    let! joinGroupRes = async {
      let protocolType,groupProtocols = cfg.groupProtocol
      let req = JoinGroup.Request(cfg.groupId, cfg.sessionTimeout, cfg.rebalanceTimeout, defaultArg prevMemberId "", protocolType, GroupProtocols(groupProtocols))
      let! res = Kafka.joinGroup conn req
      match res.errorCode with
      | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
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
        return! join gm None
      | _ -> 
        return! join gm prevMemberId

    | Success joinGroupRes ->
                           
      Log.info "join_group_response|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s" 
        cfg.groupId 
        joinGroupRes.memberId 
        joinGroupRes.generationId 
        joinGroupRes.leaderId
        joinGroupRes.groupProtocol

      // TODO: handle joinGroupRes.groupProtocol

      let! syncGroupRes = async {
        if joinGroupRes.members.members.Length > 0 then
          Log.info "joined_as_leader|group_id=%s member_id=%s generation_id=%i leader_id=%s group_protocol=%s"
            cfg.groupId 
            joinGroupRes.memberId 
            joinGroupRes.generationId 
            joinGroupRes.leaderId
            joinGroupRes.groupProtocol
                   
          let! memberAssignments = cfg.assign joinGroupRes.groupProtocol joinGroupRes.members.members

          let req = SyncGroupRequest(cfg.groupId, joinGroupRes.generationId, joinGroupRes.memberId, GroupAssignment(memberAssignments))
          let! res = Kafka.syncGroup conn req
          match res.errorCode with
          | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
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
          | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode | ErrorCode.NotCoordinatorForGroupCode ->
            return None
          | _ ->
            return Some res }
        
      match syncGroupRes with
      | None ->
        return! join gm (Some joinGroupRes.memberId)

      | Some syncGroupRes ->
                
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

        let heartbeatSleep = cfg.sessionTimeout / cfg.heartbeatFrequency

        /// Starts the hearbeat process.
        let rec heartbeat (state:GroupMemberStateWrapper) =
          peekTask
            ignore
            state.closed.Task
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

        Log.info "starting_heartbeats|heartbeat_frequency=%i session_timeout=%i heartbeat_sleep=%i" cfg.heartbeatFrequency cfg.sessionTimeout heartbeatSleep
        Async.Start (heartbeat state)
        let! _ = gm.state |> MVar.put state
        return () }

  let internal generations (gm:GroupMember) =    
    let rec loop () = asyncSeq {
      let! state = state gm
      yield state
      let! rejoin = state.closed.Task |> Async.AwaitTask
      if rejoin then
        let! _ = join gm (Some state.state.memberId)
        yield! loop () }
    loop ()

  let createJoin (conn:KafkaConn) (config:GroupConfig) = async {
    let gm = 
      { GroupMember.conn = conn
        state = MVar.create ()
        config = config }
    let! _ = join gm None
    return gm }
