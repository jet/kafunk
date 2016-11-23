#nowarn "40"
namespace Kafunk

open FSharp.Control
open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic

open Kafunk

/// Routing topic/partition and groups to channels.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Routing =

  /// Partitions a fetch request by topic/partition and wraps each one in a request.
  let partitionFetchReq (req:FetchRequest) =
    req.topics
    |> Seq.collect (fun (tn, ps) -> ps |> Array.map (fun (p, o, mb) -> (tn, p, o, mb)))
    |> Seq.groupBy (fun (tn, ps, _, _) ->  (tn, ps))
    |> Seq.map (fun (tp, reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> t, ps |> Seq.map (fun (_, p, o, mb) -> (p, o, mb)) |> Seq.toArray)
        |> Seq.toArray
      let req = new FetchRequest(req.replicaId, req.maxWaitTime, req.minBytes, topics)
      tp, RequestMessage.Fetch req)
    |> Seq.toArray

  /// Unwraps a set of responses as fetch responses and joins them into a single response.
  let concatFetchRes (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toFetch
    |> (fun rs -> new FetchResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.FetchResponse)

  /// Partitions a produce request by topic/partition.
  let partitionProduceReq (req:ProduceRequest) =
    req.topics
    |> Seq.collect (fun (t, ps) -> ps |> Array.map (fun (p, mss, ms) -> (t, p, mss, ms)))
    |> Seq.groupBy (fun (t, p, _, _) -> (t, p))
    |> Seq.map (fun (tp, reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> (t, (ps |> Seq.map (fun (_, p, mss, ms) -> (p, mss, ms)) |> Seq.toArray)))
        |> Seq.toArray
      let req = new ProduceRequest(req.requiredAcks, req.timeout, topics)
      (tp, RequestMessage.Produce req))
    |> Seq.toArray

  let concatProduceResponses (rs:ProduceResponse[]) =
    let topics = rs |> Array.collect (fun r -> r.topics)
    let tt = rs |> Seq.map (fun r -> r.throttleTime) |> Seq.max
    new ProduceResponse(topics, tt)

  let concatProduceResponseMessages (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toProduce
    |> concatProduceResponses
    |> ResponseMessage.ProduceResponse

  /// Partitions an offset request by topic/partition.
  let partitionOffsetReq (req:OffsetRequest) =
    req.topics
    |> Seq.collect (fun (t, ps) -> ps |> Array.map (fun (p, tm, mo) -> (t, p, tm, mo)))
    |> Seq.groupBy (fun (t, p, _, _) -> (t, p))
    |> Seq.map (fun (tp, reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> (t, (ps |> Seq.map (fun (_, p, mss, ms) -> (p, mss, ms)) |> Seq.toArray)))
        |> Seq.toArray
      let req = new OffsetRequest(req.replicaId, topics)
      tp, RequestMessage.Offset req)
    |> Seq.toArray

  let concatOffsetResponses (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toOffset
    |> (fun rs -> new OffsetResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.OffsetResponse)


  /// The routing table provides host information for the leader of a topic/partition
  /// or a group coordinator.
  type Routes = {
    bootstrapHost : EndPoint
    nodeToHost : Map<NodeId, EndPoint>
    topicToNode : Map<TopicName * Partition, NodeId>
    groupToHost : Map<GroupId, EndPoint>
  } with
  
    static member ofBootstrap (ep:EndPoint) =
      {
        bootstrapHost = ep
        nodeToHost = Map.empty
        topicToNode = Map.empty
        groupToHost = Map.empty
      }

    static member tryFindHostForTopic (rt:Routes) (tn:TopicName, p:Partition) =
      rt.topicToNode |> Map.tryFind (tn,p) |> Option.bind (fun nid -> rt.nodeToHost |> Map.tryFind nid)
  
    static member tryFindHostForGroup (rt:Routes) (gid:GroupId) =
      rt.groupToHost |> Map.tryFind gid
  
    static member addMetadata (metadata:MetadataResponse) (rt:Routes) =
      {
        rt with
        
          nodeToHost = 
            rt.nodeToHost
            |> Map.addMany (metadata.brokers |> Seq.map (fun b -> b.nodeId, EndPoint.parse (b.host, b.port)))
        
          topicToNode = 
            rt.topicToNode
            |> Map.addMany (
                metadata.topicMetadata
                |> Seq.collect (fun tmd ->
                  tmd.partitionMetadata
                  |> Seq.map (fun pmd -> (tmd.topicName, pmd.partitionId), pmd.leader)))
      }

    static member addGroupCoordinator (gid:GroupId, host:Host, port:Port) (rt:Routes) =
      {
        rt with groupToHost = rt.groupToHost |> Map.add gid (EndPoint.parse (host,port))
      }

    static member topicPartitions (x:Routes) =
      x.topicToNode 
      |> Map.toSeq 
      |> Seq.map fst 
      |> Seq.groupBy fst
      |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
      |> Map.ofSeq
      

  /// A route is a result where success is a set of request and host pairs
  /// and failure is a set of request and missing route pairs.
  /// A request can target multiple topics and as such, multiple brokers.
  type RouteResult = Result<(RequestMessage * EndPoint)[], MissingRouteResult>
        
  /// Indicates a missing route.
  and MissingRouteResult =
    | MissingTopicRoute of topic:TopicName
    | MissingGroupRoute of group:GroupId

  /// Performs request routing based on cluster metadata.
  /// Fetch, produce and offset requests are routed to the broker which is the leader for that topic, partition.
  /// Group related requests are routed to the respective broker.
  let route (routes:Routes) : RequestMessage -> RouteResult =

    // route to bootstrap broker
    let bootstrapRoute (req:RequestMessage) : RouteResult = 
      Success [| req, routes.bootstrapHost |]

    // route to leader of a topic/partition
    let topicRoute xs =
      xs
      |> Result.traverse (fun ((tn,p),req) ->
        match Routes.tryFindHostForTopic routes (tn,p) with
        | Some host -> Success (req,host)
        | None -> Failure (MissingTopicRoute tn))

    // route to group
    let groupRoute req gid =
      match Routes.tryFindHostForGroup routes gid with
      | Some host -> Success [| req,host |]
      | None -> Failure (MissingGroupRoute gid)

    fun (req:RequestMessage) ->
      match req with
      | Metadata _ -> bootstrapRoute req
      | GroupCoordinator _ -> bootstrapRoute req
      | DescribeGroups _ -> bootstrapRoute req
      | ListGroups _req -> bootstrapRoute req
      | Fetch req -> req |> partitionFetchReq |> topicRoute
      | Produce req -> req |> partitionProduceReq |> topicRoute
      | Offset req -> req |> partitionOffsetReq |> topicRoute
      | OffsetCommit r -> groupRoute req r.consumerGroup
      | OffsetFetch r -> groupRoute req r.consumerGroup
      | JoinGroup r -> groupRoute req r.groupId
      | SyncGroup r -> groupRoute req r.groupId
      | Heartbeat r -> groupRoute req r.groupId
      | LeaveGroup r -> groupRoute req r.groupId

 


/// Indicates an action to take in response to a request error.
type RetryAction =
  
  // TODO: generalize these 3
  | RefreshMetadataAndRetry of topics:TopicName[]
  | RefreshGroupCoordinator of groupId:GroupId
  | WaitAndRetry


  | Escalate
  | PassThru
  with

    static member errorRetryAction (ec:ErrorCode) =
      match ec with
      | ErrorCode.NoError -> None
      
      | ErrorCode.LeaderNotAvailable | ErrorCode.RequestTimedOut | ErrorCode.GroupLoadInProgressCode | ErrorCode.GroupCoordinatorNotAvailableCode
      | ErrorCode.NotEnoughReplicasAfterAppendCode | ErrorCode.NotEnoughReplicasCode | ErrorCode.UnknownTopicOrPartition ->      
        Some (RetryAction.WaitAndRetry)
      
      | ErrorCode.NotLeaderForPartition | ErrorCode.UnknownTopicOrPartition ->
        Some (RetryAction.RefreshMetadataAndRetry [||])

      | ErrorCode.NotCoordinatorForGroupCode | ErrorCode.IllegalGenerationCode -> 
        Some (RetryAction.PassThru) // escalate to consumer group logic.
      
      | _ ->
        Some (RetryAction.Escalate)

    /// TODO: collect all errors
    static member tryFindError (res:ResponseMessage) =
      match res with
      | ResponseMessage.MetadataResponse r ->
        r.topicMetadata
        |> Seq.tryPick (fun x -> 
          RetryAction.errorRetryAction x.topicErrorCode
          |> Option.map (fun action -> x.topicErrorCode,action,""))

      | ResponseMessage.OffsetResponse r ->
        r.topics
        |> Seq.tryPick (fun (_tn,ps) -> 
          ps
          |> Seq.tryPick (fun x -> 
            RetryAction.errorRetryAction x.errorCode
            |> Option.map (fun action -> x.errorCode,action,"")))

      | ResponseMessage.FetchResponse r ->
        r.topics 
        |> Seq.tryPick (fun (_tn,pmd) -> 
          pmd 
          |> Seq.tryPick (fun (p,ec,_,_,_) -> 
            RetryAction.errorRetryAction ec
            |> Option.map (fun action -> ec, action, sprintf "error_code=%i partition=%i" ec p)))

      | ResponseMessage.ProduceResponse r ->
        r.topics
        |> Seq.tryPick (fun (_tn,ps) ->
          ps
          |> Seq.tryPick (fun (_p,ec,_os) -> 
            RetryAction.errorRetryAction ec
            |> Option.map (fun action -> ec,action,"")))
      
      | ResponseMessage.GroupCoordinatorResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action,"")

      | ResponseMessage.HeartbeatResponse r ->
        match r.errorCode with 
        | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
          Some (r.errorCode,RetryAction.PassThru,"")
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action,"")

      | ResponseMessage.OffsetFetchResponse r -> 
        r.topics
        |> Seq.tryPick (fun (_t,ps) ->
          ps
          |> Seq.tryPick (fun (_p,_o,_md,ec) -> 
            match ec with
            | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
              Some (ec,RetryAction.PassThru,"")
            | _ ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec,action,"")))

      | ResponseMessage.OffsetCommitResponse r ->
        r.topics
        |> Seq.tryPick (fun (_tn,ps) ->
          ps
          |> Seq.tryPick (fun (_p,ec) -> 
            match ec with
            | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
              Some (ec,RetryAction.PassThru,"")
            | _ ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec,action,"")))
                        
      | ResponseMessage.JoinGroupResponse r ->
        match r.errorCode with
        | ErrorCode.UnknownMemberIdCode ->
          Some (r.errorCode,RetryAction.PassThru,"")
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action,"")

      | ResponseMessage.SyncGroupResponse r ->
        match r.errorCode with 
        | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
          Some (r.errorCode,RetryAction.PassThru,"")
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action,"")
      
      | ResponseMessage.LeaveGroupResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action,"")

      | ResponseMessage.DescribeGroupsResponse r ->
        r.groups
        |> Seq.tryPick (fun (ec,_,_,_,_,_) -> 
          RetryAction.errorRetryAction ec
          |> Option.map (fun action -> ec,action,""))

      | ResponseMessage.ListGroupsResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action,"")



/// Kafka connection configuration.
/// http://kafka.apache.org/documentation.html#connectconfigs
type KafkaConnCfg = {
  
  /// The bootstrap brokers to attempt connection to.
  bootstrapServers : Uri list
  
  /// The client id.
  clientId : ClientId
  
  /// TCP connection configuration.
  tcpConfig : Chan.Config 

} with

  /// Creates a Kafka configuration object given the specified list of broker hosts to bootstrap with.
  /// The first host to which a successful connection is established is used for a subsequent metadata request
  /// to build a routing table mapping topics and partitions to brokers.
  static member create (bootstrapServers:Uri list, ?clientId:ClientId, ?tcpConfig) =
    { bootstrapServers = bootstrapServers
      clientId = match clientId with Some clientId -> clientId | None -> Guid.NewGuid().ToString("N")
      tcpConfig = defaultArg tcpConfig (Chan.Config.create()) }


/// Connection state.
type ConnState = {
  routes : Routing.Routes
  channels : Map<EndPoint, Chan>
  version : int
} with
  
  static member tryFindChanByEndPoint (ep:EndPoint) (s:ConnState) =
    s.channels |> Map.tryFind ep

  static member private updateChannels (f:Map<EndPoint, Chan> -> Map<EndPoint, Chan>) (s:ConnState) =
    {
      s with 
        channels = f s.channels
        version = s.version + 1
    }

  static member addChannel (ep:EndPoint, ch:Chan) (s:ConnState) =
    ConnState.updateChannels (Map.add ep ch) s

  static member updateRoutes (f:Routing.Routes -> Routing.Routes) (s:ConnState) =
    {
      s with 
          routes = f s.routes
          version = s.version + 1
    }

  static member bootstrap (bootstrapEp:EndPoint, bootstrapCh:Chan) =
    {
      channels = [bootstrapEp,bootstrapCh] |> Map.ofList
      routes = Routing.Routes.ofBootstrap bootstrapEp
      version = 0
    }

exception EscalationException of errorCode:ErrorCode * res:ResponseMessage

/// A connection to a Kafka cluster.
/// This is a stateful object which maintains request/reply sessions with brokers.
/// It acts as a context for API operations, providing filtering and fault tolerance.
type KafkaConn internal (cfg:KafkaConnCfg) =

  static let Log = Log.create "Kafunk.Conn"

  let bootstrapConnectBackoff = Backoff.constant 5000 |> Backoff.maxAttempts 3
  let waitRetrySleepMs = 5000

  let stateCell : MVar<ConnState> = MVar.create ()
  let cts = new CancellationTokenSource()

  let connCh state host = async {
    match state |> ConnState.tryFindChanByEndPoint host with
    | Some _ -> return state
    | None ->
      Log.info "creating_channel|host=%A" host
      let! ep,ch = Chan.connectEndPoint cfg.tcpConfig cfg.clientId host
      return state |> ConnState.addChannel (ep,ch) }

  /// Connects to the first available broker in the bootstrap list and returns the 
  /// initial routing table.
  let rec bootstrap (cfg:KafkaConnCfg) =
    let update (_:ConnState option) = 
      cfg.bootstrapServers
      |> AsyncSeq.ofSeq
      |> AsyncSeq.traverseAsyncResult Exn.monoid (fun uri -> async {
        try
          Log.info "connecting_to_bootstrap_brokers|client_id=%s host=%s:%i" cfg.clientId uri.Host uri.Port
          let! ep,ch = Chan.connectHost cfg.tcpConfig cfg.clientId (uri.Host,uri.Port)
          let state = ConnState.bootstrap (ep, ch)
          return Success state
        with ex ->
          Log.error "errored_connecting_to_bootstrap_host|host=%s:%i error=%O" uri.Host uri.Port ex
          return Failure ex })
      |> Faults.retryResultThrow
          id 
          Exn.monoid
          bootstrapConnectBackoff
    stateCell |> MVar.putOrUpdateAsync update

  /// Discovers cluster metadata.
  and getMetadata (callerState:ConnState) (topics:TopicName[]) =
    let update currentState = async {
      if currentState.version = callerState.version then
        let! metadata = Chan.metadata (send currentState) (Metadata.Request(topics))
        Log.info "received_cluster_metadata|%s" (MetadataResponse.Print metadata)
        return currentState |> ConnState.updateRoutes (Routing.Routes.addMetadata metadata)
      else
        return currentState }
    stateCell |> MVar.updateAsync update

  /// Discovers a coordinator for the group.
  and getGroupCoordinator (callerState:ConnState) (groupId:GroupId) =
    let update currentState = async {
      if currentState.version = callerState.version then
        let! group = Chan.groupCoordinator (send currentState) (GroupCoordinatorRequest(groupId))
        Log.info "received_group_coordinator|%s" (GroupCoordinatorResponse.Print group)
        return 
          currentState 
          |> ConnState.updateRoutes (Routing.Routes.addGroupCoordinator (groupId,group.coordinatorHost,group.coordinatorPort))
      else
        return currentState }
    stateCell |> MVar.updateAsync update

  /// Sends the request based on discovered routes.
  and send (state:ConnState) (req:RequestMessage) = async {
    match Routing.route state.routes req with
    | Success requestRoutes ->
      let sendHost (req:RequestMessage, host:EndPoint) = async {
        match state |> ConnState.tryFindChanByEndPoint host with
        | Some ch -> 
          return!
            req
            |> Chan.send ch
            |> Async.Catch
            |> Async.bind (fun res -> async {
              match res with
              | Success res ->
                match RetryAction.tryFindError res with
                | None -> 
                  return res
                | Some (errorCode,action,msg) ->
                  Log.error "response_errored|error_code=%i retry_action=%A message=%s res=%A" errorCode action msg res
                  match action with
                  | RetryAction.PassThru ->
                    return res
                  | RetryAction.Escalate ->
                    return raise (EscalationException (errorCode,res))
                  | RetryAction.RefreshGroupCoordinator gid ->
                    let! state' = getGroupCoordinator state gid
                    return! send state' req
                  | RetryAction.RefreshMetadataAndRetry topics ->
                    let! state' = getMetadata state topics
                    return! send state' req
                  | RetryAction.WaitAndRetry ->
                    do! Async.Sleep waitRetrySleepMs
                    return! send state req //})
              | Failure ex ->
                Log.error "channel_failure_escalated|host=%A request=%s error=%O" host (RequestMessage.Print req) ex
//                //let! state' = getMetadata state topics
                //do! Async.Sleep waitRetrySleepMs
                //return! send state req })
                return raise ex })
        | None ->
          let! state' = stateCell |> MVar.updateAsync (fun state' -> connCh state' host)
          return! send state' req }
      
      /// Sends requests to routed hosts in parallel and gathers the results.
      let scatterGather (gather:ResponseMessage[] -> ResponseMessage) = async {
        if requestRoutes.Length = 1 then
          return! sendHost requestRoutes.[0]
        else
          return!
            requestRoutes
            |> Seq.map sendHost
            |> Async.Parallel
            |> Async.map gather }
 
      match req with
      | RequestMessage.Offset _ -> 
        return! scatterGather Routing.concatOffsetResponses
      | RequestMessage.Fetch _ ->
        return! scatterGather Routing.concatFetchRes
      | RequestMessage.Produce _ ->
        return! scatterGather Routing.concatProduceResponseMessages
      | _ -> 
        // single-broker request
        return! sendHost requestRoutes.[0]

    | Failure (Routing.MissingTopicRoute topic) ->
      Log.warn "missing_topic_partition_route|topic=%s request=%A" topic req
      let! state' = getMetadata state [|topic|]
      return! send state' req

    | Failure (Routing.MissingGroupRoute group) ->
      Log.warn "missing_group_goordinator_route|group=%s" group
      let! state' = getGroupCoordinator state group
      return! send state' req

    }
    
  /// Gets the cancellation token triggered when the connection is closed.
  member internal __.CancellationToken = cts.Token

  member private __.GetState () =
    let state = MVar.getFastUnsafe stateCell
    if state.IsNone then
      invalidOp "Connection state unavailable; must not be connected."
    else
      state.Value

  member internal __.Send (req:RequestMessage) : Async<ResponseMessage> = async {
    let state = __.GetState ()
    return! send state req }
  
  /// Connects to a broker from the bootstrap list.
  member internal __.Connect () = async {
    let! _ = bootstrap cfg
    return () }

  member internal __.GetGroupCoordinator (groupId:GroupId) = async {
    let state = __.GetState ()
    return! getGroupCoordinator state groupId }

  member internal __.GetMetadata (topics:TopicName[]) = async {
    let state = __.GetState ()
    let! state' = getMetadata state topics
    return state'.routes |> Routing.Routes.topicPartitions }

  member __.Close () =
    Log.info "closing_connection|client_id=%s" cfg.clientId
    cts.Cancel()
    (stateCell :> IDisposable).Dispose()
    
  interface IDisposable with
    member __.Dispose () =
      __.Close ()


/// Kafka API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Kafka =

  let private Log = Log.create "Kafunk"
  
  let connAsync (cfg:KafkaConnCfg) = async {
    let conn = new KafkaConn(cfg)
    do! conn.Connect ()
    return conn }

  let conn cfg =
    connAsync cfg |> Async.RunSynchronously

  let connHostAsync (host:string) =
    let uri = KafkaUri.parse host
    let cfg = KafkaConnCfg.create [uri]
    connAsync cfg

  let connHost host =
    connHostAsync host |> Async.RunSynchronously

  let metadata (c:KafkaConn) (req:Metadata.Request) : Async<MetadataResponse> =
    Chan.metadata c.Send req

  let fetch (c:KafkaConn) (req:FetchRequest) : Async<FetchResponse> =
    Chan.fetch c.Send req

  let produce (c:KafkaConn) (req:ProduceRequest) : Async<ProduceResponse> =
    let chan = c.Send
    Chan.produce chan req

  let offset (c:KafkaConn) (req:OffsetRequest) : Async<OffsetResponse> =
    Chan.offset c.Send req

  let groupCoordinator (c:KafkaConn) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> =
    Chan.groupCoordinator c.Send req

  let offsetCommit (c:KafkaConn) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> =
    Chan.offsetCommit c.Send req

  let offsetFetch (c:KafkaConn) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    Chan.offsetFetch c.Send req

  let joinGroup (c:KafkaConn) (req:JoinGroup.Request) : Async<JoinGroup.Response> =
    Chan.joinGroup c.Send req

  let syncGroup (c:KafkaConn) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    Chan.syncGroup c.Send req

  let heartbeat (c:KafkaConn) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    Chan.heartbeat c.Send req

  let leaveGroup (c:KafkaConn) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    Chan.leaveGroup c.Send req

  let listGroups (c:KafkaConn) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    Chan.listGroups c.Send req

  let describeGroups (c:KafkaConn) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    Chan.describeGroups c.Send req

  /// Composite operations.
  module Composite =

    let topicOffsets (conn:KafkaConn) (time:Time, maxOffsets:MaxNumberOfOffsets) (topic:TopicName) = async {
      Log.info "getting_offsets|topic=%s time=%i" topic time
      let! metadata = conn.GetMetadata [|topic|]
      let topics =
        metadata
        |> Map.toSeq
        |> Seq.map (fun (tn,ps) ->
          let ps = ps |> Array.map (fun p -> p,time,maxOffsets)
          tn,ps)
        |> Seq.toArray
      let offsetReq = OffsetRequest(-1, topics)
      let! offsetRes = offset conn offsetReq
      return offsetRes }