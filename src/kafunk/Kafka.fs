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

/// Connection state.
type internal ConnState = {
  bootstrapBroker : Chan option
  brokersByEndPoint : Map<EndPoint, Chan>
  brokersByTopicPartition : Map<TopicName * Partition, Chan>
  brokersByGroup : Map<GroupId, Chan>
  topics : Map<TopicName * Partition, NodeId>
  brokers : Map<NodeId, Chan>
  version : int
} with
  
  static member bootstrap (bootstrapCh:Chan) =
    {
      bootstrapBroker = Some bootstrapCh
      brokersByEndPoint = Map.empty
      brokersByTopicPartition = Map.empty
      brokersByGroup = Map.empty
      topics = Map.empty
      brokers = Map.empty
      version = 0
    }

//  static member topicPartitions (s:ConnState) =
//    s.brokersByTopicPartition 
//    |> Map.toSeq 
//    |> Seq.map fst 
//    |> Seq.groupBy fst
//    |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
//    |> Map.ofSeq

  static member topicPartitions (s:ConnState) =
    s.topics
    |> Seq.map (fun kvp -> kvp.Key)
    |> Seq.groupBy fst
    |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
    |> Map.ofSeq

  static member tryFindGroupCoordinatorBroker (groupId:GroupId) (s:ConnState) =
    s.brokersByGroup
    |> Map.tryFind groupId

  static member tryFindTopicPartitionBroker (tn:TopicName, p:Partition) (s:ConnState) =
    s.brokersByTopicPartition
    |> Map.tryFind (tn,p)

  static member tryFindBrokerByEndPoint (ep:EndPoint) (s:ConnState) =
    s.brokersByEndPoint
    |> Map.tryFind ep
    
  static member updateTopicPartitions (brokers:(NodeId * Chan) seq, topicNodes:seq<TopicName * Partition * NodeId>) (s:ConnState) =
    let brokers = s.brokers |> Map.addMany brokers
    {
      s with
          brokers = brokers
          topics = s.topics |> Map.addMany (topicNodes |> Seq.map (fun (t,p,n) -> (t,p),n))
          brokersByEndPoint = Map.addMany (brokers |> Seq.map (fun kvp -> Chan.endpoint kvp.Value, kvp.Value)) s.brokersByEndPoint
          brokersByTopicPartition = 
            s.brokersByTopicPartition
            |> Map.addMany (topicNodes|> Seq.map (fun (tn,p,leaderId) -> (tn, p), Map.find leaderId brokers))
          version = s.version + 1
    }

  static member updateGroupCoordinator (gid:GroupId, nodeId:CoordinatorId, ch:Chan) (s:ConnState) =
    {
      s with
        brokersByGroup = s.brokersByGroup |> Map.add gid ch
        brokersByEndPoint = s.brokersByEndPoint |> Map.add (Chan.endpoint ch) ch
        brokers = s.brokers |> Map.add nodeId ch
        version = s.version + 1
    }

  static member updateBootstrap (ch:Chan) (s:ConnState) =
    {
      s with
        bootstrapBroker = Some ch
        brokersByEndPoint = s.brokersByEndPoint |> Map.add (Chan.endpoint ch) ch
        version = s.version + 1
    }

  static member removeBroker (ch:Chan) (s:ConnState) =
    let ep = Chan.endpoint ch
    let topicKeys = 
      s.brokersByTopicPartition
      |> Seq.choose (fun kvp -> 
        let ep' = Chan.endpoint kvp.Value
        if ep' = ep then Some kvp.Key
        else None)
    let groupKeys = 
      s.brokersByGroup
      |> Seq.choose (fun kvp -> 
        let ep' = Chan.endpoint kvp.Value
        if ep' = ep then Some kvp.Key
        else None)
    {
      s with
        bootstrapBroker = 
          match s.bootstrapBroker with
          | Some ch when Chan.endpoint ch = ep -> None
          | s -> s
        brokersByEndPoint = s.brokersByEndPoint |> Map.remove ep
        brokersByGroup = s.brokersByGroup |> Map.removeAll groupKeys
        brokersByTopicPartition = s.brokersByTopicPartition |> Map.removeAll topicKeys
        version = s.version + 1
    }


type internal RouteType =
  | BootstrapRoute
  | TopicRoute of TopicName[]
  | GroupRoute of GroupId 
  with 
    static member ofRequest (req:RequestMessage) =
      match req with
      | RequestMessage.DescribeGroups _ -> BootstrapRoute
      | RequestMessage.Fetch r -> TopicRoute (r.topics |> Array.map fst)
      | RequestMessage.GroupCoordinator _ -> BootstrapRoute
      | RequestMessage.Heartbeat r -> GroupRoute r.groupId
      | RequestMessage.JoinGroup r -> GroupRoute r.groupId
      | RequestMessage.LeaveGroup r -> GroupRoute r.groupId
      | RequestMessage.ListGroups _ -> BootstrapRoute
      | RequestMessage.Metadata _ -> BootstrapRoute
      | RequestMessage.Offset _ -> BootstrapRoute
      | RequestMessage.OffsetCommit r -> GroupRoute r.consumerGroup
      | RequestMessage.OffsetFetch r -> GroupRoute r.consumerGroup
      | RequestMessage.Produce r -> TopicRoute (r.topics |> Array.map fst)
      | RequestMessage.SyncGroup r -> GroupRoute r.groupId


/// A route is a result where success is a set of request and host pairs
/// and failure is a set of request and missing route pairs.
/// A request can target multiple topics and as such, multiple brokers.
type internal RouteResult = Result<(RequestMessage * Chan)[], RouteType>

/// Routing topic/partition and groups to channels.
[<Compile(Module)>]
module internal Routing =

  /// Partitions a fetch request by topic/partition and wraps each one in a request.
  let private partitionFetchReq (state:ConnState) (req:FetchRequest) =
    req.topics
    |> Seq.collect (fun (tn, ps) -> ps |> Array.map (fun (p, o, mb) -> (tn, p, o, mb)))
    |> Seq.groupBy (fun (tn, p, _, _) -> ConnState.tryFindTopicPartitionBroker (tn, p) state |> Result.ofOptionMap (fun () -> tn))
    |> Seq.map (fun (ch,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> t, ps |> Seq.map (fun (_, p, o, mb) -> (p, o, mb)) |> Seq.toArray)
        |> Seq.toArray
      let req = new FetchRequest(req.replicaId, req.maxWaitTime, req.minBytes, topics)
      ch, RequestMessage.Fetch req)
    |> Seq.toArray

  /// Partitions a produce request by topic/partition.
  let private partitionProduceReq (state:ConnState) (req:ProduceRequest) =
    req.topics
    |> Seq.collect (fun (t, ps) -> ps |> Array.map (fun (p, mss, ms) -> (t, p, mss, ms)))
    |> Seq.groupBy (fun (t, p, _, _) -> ConnState.tryFindTopicPartitionBroker (t, p) state |> Result.ofOptionMap (fun () -> t))
    |> Seq.map (fun (ep,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> (t, (ps |> Seq.map (fun (_, p, mss, ms) -> (p, mss, ms)) |> Seq.toArray)))
        |> Seq.toArray
      let req = new ProduceRequest(req.requiredAcks, req.timeout, topics)
      (ep, RequestMessage.Produce req))
    |> Seq.toArray

  let private partitionOffsetReq (state:ConnState) (req:OffsetRequest) =
    req.topics
    |> Seq.collect (fun (t, ps) -> ps |> Array.map (fun (p, tm, mo) -> (t, p, tm, mo)))
    |> Seq.groupBy (fun (t, p, _, _) -> ConnState.tryFindTopicPartitionBroker (t, p) state |> Result.ofOptionMap (fun () -> t))
    |> Seq.map (fun (ep,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t, _, _, _) -> t)
        |> Seq.map (fun (t, ps) -> (t, (ps |> Seq.map (fun (_, p, mss, ms) -> (p, mss, ms)) |> Seq.toArray)))
        |> Seq.toArray
      let req = new OffsetRequest(req.replicaId, topics)
      ep, RequestMessage.Offset req)
    |> Seq.toArray

  let concatFetchRes (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toFetch
    |> (fun rs -> 
      let res =
        if rs.Length = 0 then 
          new FetchResponse (0, [||])
        else
          let tt = rs |> Seq.map (fun r -> r.throttleTime) |> Seq.max
          new FetchResponse(tt, rs |> Array.collect (fun r -> r.topics))
      ResponseMessage.FetchResponse res)

  let concatProduceResponses (rs:ProduceResponse[]) =
    let topics = rs |> Array.collect (fun r -> r.topics)
    let tt = rs |> Seq.map (fun r -> r.throttleTime) |> Seq.max
    new ProduceResponse(topics, tt)

  let concatProduceResponseMessages (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toProduce
    |> concatProduceResponses
    |> ResponseMessage.ProduceResponse

  let concatOffsetResponses (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toOffset
    |> (fun rs -> new OffsetResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.OffsetResponse)

  let route (state:ConnState) : RequestMessage -> Result<(RequestMessage * Chan)[], RouteType> =

    let bootstrapRoute (req:RequestMessage) =
      match state.bootstrapBroker with
      | Some ch -> Success [| req, ch |]
      | None -> Failure (RouteType.BootstrapRoute)

    let topicRoute (xs:(Result<Chan, TopicName> * RequestMessage)[]) =
      xs
      |> Result.traverse (fun (routeRes,req) ->
        match routeRes with
        | Success ch -> Success (req,ch)
        | Failure tn -> Failure (RouteType.TopicRoute [|tn|]))

    let groupRoute req gid =
      match ConnState.tryFindGroupCoordinatorBroker gid state with
      | Some ch -> Success [| req,ch |]
      | None -> Failure (RouteType.GroupRoute gid)

    fun (req:RequestMessage) ->
      match req with
      | Metadata _ -> bootstrapRoute req
      | GroupCoordinator _ -> bootstrapRoute req
      | DescribeGroups _ -> bootstrapRoute req
      | ListGroups _req -> bootstrapRoute req
      | Fetch req -> req |> partitionFetchReq state |> topicRoute
      | Produce req -> req |> partitionProduceReq state |> topicRoute
      | Offset req -> req |> partitionOffsetReq state |> topicRoute
      | OffsetCommit r -> groupRoute req r.consumerGroup
      | OffsetFetch r -> groupRoute req r.consumerGroup
      | JoinGroup r -> groupRoute req r.groupId
      | SyncGroup r -> groupRoute req r.groupId
      | Heartbeat r -> groupRoute req r.groupId
      | LeaveGroup r -> groupRoute req r.groupId

 


/// Indicates an action to take in response to a request error.
type private RetryAction =
  
  // refresh routes
  | RefreshMetadataAndRetry of topics:TopicName[]

  // wait
  | WaitAndRetry

  // escalate
  | Escalate
  | PassThru

  with

    static member errorRetryAction (ec:ErrorCode) =
      match ec with
      | ErrorCode.NoError -> None
      
      | ErrorCode.LeaderNotAvailable | ErrorCode.RequestTimedOut | ErrorCode.GroupLoadInProgressCode | ErrorCode.GroupCoordinatorNotAvailableCode
      | ErrorCode.NotEnoughReplicasAfterAppendCode | ErrorCode.NotEnoughReplicasCode ->
        Some (RetryAction.WaitAndRetry)

      | ErrorCode.NotCoordinatorForGroupCode | ErrorCode.IllegalGenerationCode | ErrorCode.OffsetOutOfRange | ErrorCode.UnknownMemberIdCode -> 
        Some (RetryAction.PassThru)
      
      | ErrorCode.UnknownTopicOrPartition ->
        Some (RetryAction.Escalate)
      | ErrorCode.InvalidMessage ->
        Some (RetryAction.Escalate)
      | _ ->
        Some (RetryAction.Escalate)

    /// TODO: collect all errors
    static member tryFindError (res:ResponseMessage) =
      match res with
      | ResponseMessage.MetadataResponse r ->
        r.topicMetadata
        |> Seq.tryPick (fun x -> 
          RetryAction.errorRetryAction x.topicErrorCode
          |> Option.map (fun action -> x.topicErrorCode,action))

      | ResponseMessage.OffsetResponse r ->
        r.topics
        |> Seq.tryPick (fun (tn,ps) -> 
          ps
          |> Seq.tryPick (fun x -> 
            match x.errorCode with
            | ErrorCode.NoError -> None
            | ErrorCode.UnknownTopicOrPartition | ErrorCode.NotLeaderForPartition -> 
              Some (x.errorCode, RetryAction.RefreshMetadataAndRetry [|tn|])
            | _ -> Some (x.errorCode, RetryAction.Escalate)))

      | ResponseMessage.FetchResponse r ->
        r.topics 
        |> Seq.tryPick (fun (tn,pmd) -> 
          pmd 
          |> Seq.tryPick (fun (_p,ec,_,_,_) -> 
            match ec with
            | ErrorCode.NoError -> None
            | ErrorCode.NotLeaderForPartition -> Some (ec, RetryAction.RefreshMetadataAndRetry [|tn|])
            | ec ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec, action)))

      | ResponseMessage.ProduceResponse r ->
        r.topics
        |> Seq.tryPick (fun (tn,ps) ->
          ps
          |> Seq.tryPick (fun (_p,ec,_os) ->
            match ec with
            | ErrorCode.NoError -> None
            | ErrorCode.NotLeaderForPartition -> Some (ec, RetryAction.RefreshMetadataAndRetry [|tn|])
            | ec ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec,action)))
      
      | ResponseMessage.GroupCoordinatorResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action)

      | ResponseMessage.HeartbeatResponse r ->
        match r.errorCode with 
        | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
          Some (r.errorCode,RetryAction.PassThru)
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action)

      | ResponseMessage.OffsetFetchResponse r -> 
        r.topics
        |> Seq.tryPick (fun (_t,ps) ->
          ps
          |> Seq.tryPick (fun (_p,_o,_md,ec) -> 
            match ec with
            | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
              Some (ec,RetryAction.PassThru)
            | _ ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec,action)))

      | ResponseMessage.OffsetCommitResponse r ->
        r.topics
        |> Seq.tryPick (fun (_tn,ps) ->
          ps
          |> Seq.tryPick (fun (_p,ec) -> 
            match ec with
            | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
              Some (ec,RetryAction.PassThru)
            | _ ->
              RetryAction.errorRetryAction ec
              |> Option.map (fun action -> ec,action)))
                        
      | ResponseMessage.JoinGroupResponse r ->
        match r.errorCode with
        | ErrorCode.UnknownMemberIdCode ->
          Some (r.errorCode,RetryAction.PassThru)
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action)

      | ResponseMessage.SyncGroupResponse r ->
        match r.errorCode with 
        | ErrorCode.UnknownMemberIdCode | ErrorCode.IllegalGenerationCode | ErrorCode.RebalanceInProgressCode ->
          Some (r.errorCode,RetryAction.PassThru)
        | _ ->
          RetryAction.errorRetryAction r.errorCode
          |> Option.map (fun action -> r.errorCode,action)
      
      | ResponseMessage.LeaveGroupResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action)

      | ResponseMessage.DescribeGroupsResponse r ->
        r.groups
        |> Seq.tryPick (fun (ec,_,_,_,_,_) -> 
          RetryAction.errorRetryAction ec
          |> Option.map (fun action -> ec,action))

      | ResponseMessage.ListGroupsResponse r ->
        RetryAction.errorRetryAction r.errorCode
        |> Option.map (fun action -> r.errorCode,action)




/// Operations for parsing Kafka URIs.
module KafkaUri =

  open System
  open System.Text.RegularExpressions

  let [<Literal>] DefaultPortKafka = 9092
  let [<Literal>] UriSchemeKafka = "kafka"
  let private KafkaBrokerUriRegex = Regex("^(?<scheme>(kafka|tcp)://)?(?<host>[-._\w]+)(:(?<port>[\d]+))?", RegexOptions.Compiled)

  /// Parses a string into a Kafka Uri.
  let parse (host:string) =
    let m = KafkaBrokerUriRegex.Match host
    if not m.Success then invalidArg "host" (sprintf "invalid host string '%s'" host)
    else
      let host = m.Groups.["host"].Value
      let port = 
        let g = m.Groups.["port"]
        if g.Success then Int32.Parse g.Value
        else DefaultPortKafka
      let ub = UriBuilder(UriSchemeKafka, host, port)
      ub.Uri


/// Kafka connection configuration.
type KafkaConfig = {
  
  /// The Kafka server version.
  version : Version

  /// The bootstrap brokers to attempt connection to.
  bootstrapServers : Uri list
  
  /// The retry policy for connecting to bootstrap brokers.
  bootstrapConnectRetryPolicy : RetryPolicy

  /// The retry policy for broker requests.
  requestRetryPolicy : RetryPolicy

  /// The client id.
  clientId : ClientId
  
  /// TCP connection configuration.
  tcpConfig : ChanConfig 

} with

  /// The default Kafka server version = 0.9.0.
  static member DefaultVersion = Version.Parse "0.9.0"

  /// The default broker channel configuration.
  static member DefaultChanConfig = ChanConfig.create ()

  /// The default client id = Guid.NewGuid().
  static member DefaultClientId = Guid.NewGuid().ToString("N")

  /// The default bootstrap broker connection retry policy = RetryPolicy.constantBoundedMs 5000 3.
  static member DefaultBootstrapConnectRetryPolicy = RetryPolicy.constantBoundedMs 5000 3

  /// The default request retry policy = RetryPolicy.constantBoundedMs 1000 10.
  static member DefaultRequestRetryPolicy = RetryPolicy.constantBoundedMs 1000 10

  /// Creates a Kafka configuration object.
  static member create (bootstrapServers:Uri list, ?clientId:ClientId, ?tcpConfig, ?bootstrapConnectRetryPolicy, ?requestRetryPolicy, ?version) =
    { version = defaultArg version KafkaConfig.DefaultVersion
      bootstrapServers = bootstrapServers
      bootstrapConnectRetryPolicy = defaultArg bootstrapConnectRetryPolicy KafkaConfig.DefaultBootstrapConnectRetryPolicy
      requestRetryPolicy = defaultArg requestRetryPolicy KafkaConfig.DefaultRequestRetryPolicy
      clientId = match clientId with Some clientId -> clientId | None -> KafkaConfig.DefaultClientId
      tcpConfig = defaultArg tcpConfig KafkaConfig.DefaultChanConfig }


/// An exception used to wrap failures which are to be escalated.
type EscalationException (errorCode:ErrorCode, req:RequestMessage, res:ResponseMessage, msg:string) =
  inherit Exception (sprintf "Kafka exception|error_code=%i request=%s response=%s message=%s" errorCode (RequestMessage.Print req) (ResponseMessage.Print res) msg)

/// A connection to a Kafka cluster.
/// This is a stateful object which maintains request/reply sessions with brokers.
/// It acts as a context for API operations, providing filtering and fault tolerance.
type KafkaConn internal (cfg:KafkaConfig) =

  static let Log = Log.create "Kafunk.Conn"
  
  let stateCell : MVar<ConnState> = MVar.create ()
  let cts = new CancellationTokenSource()

  // NB: The presence of the critical boolean flag is unfortunate but required to
  // address reentrancy issues. railures are recovered inside of a critical region
  // to prevent a thundering herd problem where many concurrent requests are failing
  // and attempting to reover. This works well, except in cases where the recovery may
  // itself need recovery (such as when a metadata refresh requires a bootstrap rediscovery).
  // Another shortcoming is that the Producer has its own recovery semantics atop a Chan, while
  // the Consumer does not. The Producer needs to handle recovery explicitly because it must
  // reconfigure its broker queues. The problem is that the Producer foregoes the routing
  // capabilities provided by the underlying connection.

  /// Connects to the broker at the specified host.
  let rec connBroker (connState:ConnState option) (host:Host, port:Port) = async {
    let! ips = async {
      match IPAddress.tryParse host with
      | Some ip ->
        return [|ip|]
      | None ->
        let! ips = Dns.IPv4.getAllAsync host
        Log.info "discovered_dns|client_id=%s host=%s ips=[%s]" cfg.clientId host (Printers.stringsCsv ips)
        return ips }
    return!
      ips
      |> Seq.map (fun ip -> EndPoint.ofIPAddressAndPort (ip, port))
      |> AsyncSeq.ofSeq
      |> AsyncSeq.traverseAsyncResult Exn.monoid (fun ep -> async {
        match connState |> Option.bind (ConnState.tryFindBrokerByEndPoint ep) with
        | Some ch when not ch.task.IsCompleted ->
          return Success ch
        | _ ->
          try
            let! ch = Chan.connect (cfg.version, cfg.tcpConfig, cfg.clientId) ep
            return Success ch
          with ex ->
            return Failure ex }) }
  
  /// Removes a broker from the cluster view.
  and removeBroker (state:ConnState) (ch:Chan) = async {
    Log.warn "removing_broker|client_id=%s ep=%O" cfg.clientId (Chan.endpoint ch)
    return state |> ConnState.removeBroker ch }

  /// Removes a broker from the cluster view.
  and removeBrokerAndApply (ch:Chan) = async {
    return!
      stateCell
      |> MVar.updateAsync (fun state -> removeBroker state ch) }

  /// Connects to the first available bootstrap broker.
  and bootstrap =
    let update (prevState:ConnState option) = async { 
      Log.info "connecting_to_bootstrap_brokers|client_id=%s brokers=%A" cfg.clientId cfg.bootstrapServers
      return!
        cfg.bootstrapServers
        |> AsyncSeq.ofSeq
        |> AsyncSeq.traverseAsyncResult Exn.monoid (fun uri -> async {
          let! ch = connBroker prevState (uri.Host,uri.Port)
          match ch with
          | Success ch ->
            let state =
              match prevState with
              | Some s -> ConnState.updateBootstrap ch s
              | None -> ConnState.bootstrap ch
            return Success state
          | Failure e ->
            return Failure e }) }
    let update =
      update
      |> Faults.AsyncFunc.retryResultThrowList 
          (fun errs -> exn("Failed to connect to a bootstrap broker.", Exn.ofSeq errs)) 
          cfg.bootstrapConnectRetryPolicy
    update
    
  /// Connects to the first available broker in the bootstrap list and returns the 
  /// initial routing table.
  and getAndApplyBootstrap = async {
    return!
      stateCell 
      |> MVar.putOrUpdateAsync bootstrap }

  /// Fetches metadata and returns an updated connection state.
  and metadata (state:ConnState) (topics:TopicName[]) = async {
    let send =
      (fun req -> routeSendInternal true RetryState.init state req)
      |> AsyncFunc.dimap RequestMessage.Metadata (ResponseMessage.toMetadata)
    let! metadata = send (Metadata.Request(topics))
    Log.info "received_cluster_metadata|%s" (MetadataResponse.Print metadata)
    let noLeader =
      metadata.topicMetadata
      |> Seq.collect (fun tmd -> 
        tmd.partitionMetadata 
        |> Seq.choose (fun p -> 
          if p.leader = -1 then Some (tmd.topicName, p.partitionId)
          else None))
      |> Seq.toArray
    if noLeader.Length > 0 then
      Log.warn "leaderless_partitions_detected|partitions=%s" (Printers.topicPartitions noLeader)
    let! brokers = 
      metadata.brokers 
      |> Seq.map (fun b -> async {
        let! ch = connBroker (Some state) (b.host, b.port)
        return ch |> Result.map (fun ch -> b.nodeId, ch) })
      |> Async.Parallel
      |> Async.map (Result.traverse id >> Result.throw)
    let topicNodes =
      metadata.topicMetadata 
      |> Seq.collect (fun tmd -> 
        tmd.partitionMetadata 
        |> Seq.choose (fun pmd -> 
          if pmd.leader >= 0 then Some (tmd.topicName, pmd.partitionId, pmd.leader) 
          else None))
    return state |> ConnState.updateTopicPartitions (brokers, topicNodes) }

  /// Fetches and applies metadata to the current connection.
  and getAndApplyMetadata (callerState:ConnState) (topics:TopicName[]) =
    stateCell
    |> MVar.updateAsync (fun (currentState:ConnState) -> async {
      if currentState.version > callerState.version then 
        Log.trace "skipping_metadata_update|current_version=%i caller_version=%i" currentState.version callerState.version
        return currentState 
      else
        let! state' = metadata currentState topics
        return state' })

  /// Refreshes metadata for existing topics.
  and refreshMetadata (critical:bool) (callerState:ConnState) =
    let topics = 
      ConnState.topicPartitions callerState
      |> Seq.map (fun kvp -> kvp.Key)
      |> Seq.toArray
    Log.info "refreshing_metadata|client_id=%s topics=%A" cfg.clientId topics
    if critical then metadata callerState topics
    else getAndApplyMetadata callerState topics

  /// Fetches group coordinator metadata.
  and groupCoordinator (state:ConnState) (groupId:GroupId) = async {
    let send = 
      routeSendInternal true RetryState.init state
      |> AsyncFunc.dimap RequestMessage.GroupCoordinator (ResponseMessage.toGroupCoordinator)
    let! res = send (GroupCoordinatorRequest(groupId))
    Log.info "received_group_coordinator|client_id=%s group_id=%s %s" 
      cfg.clientId groupId (GroupCoordinatorResponse.Print res)
    let! ch = connBroker (Some state) (res.coordinatorHost, res.coordinatorPort) |> Async.map Result.throw
    return 
      state 
      |> ConnState.updateGroupCoordinator (groupId, res.coordinatorId, ch) }

  /// Fetches the group coordinator and applies the state to the current connection.
  and getAndApplyGroupCoordinator (callerState:ConnState) (groupId:GroupId) =
    stateCell 
    |> MVar.updateAsync (fun (currentState:ConnState) -> async {
      if currentState.version > callerState.version then 
        Log.trace "skipping_group_coordinator_update|current_version=%i caller_version=%i" currentState.version callerState.version
        return currentState 
      else
        let! state' = groupCoordinator currentState groupId
        return state' })

  /// Sends the request based on discovered routes.
  and routeSendInternal (critical:bool) (rs:RetryState) (state:ConnState) (req:RequestMessage) = async {
    match Routing.route state req with
    | Success routes ->
      let scatterGather (gather:ResponseMessage[] -> ResponseMessage) = async {
        if routes.Length = 1 then
          let req,ch = routes.[0]
          return! sendInternal critical rs state ch req
        else
          return!
            routes
            |> Seq.map (fun (req,ch) -> sendInternal critical rs state ch req)
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
        let req,ch = routes.[0]
        return! sendInternal critical rs state ch req

    | Failure rt ->
      Log.trace "missing_route|route_type=%A request=%s" rt (RequestMessage.Print req)
      let! rs' = RetryPolicy.awaitNextState cfg.requestRetryPolicy rs
      match rs' with
      | Some rs -> 
        let! state' = async {
          match rt with
          | RouteType.BootstrapRoute ->
            if critical then return! bootstrap (Some state)
            else return! getAndApplyBootstrap
          | RouteType.GroupRoute gid ->
            if critical then return! groupCoordinator state gid
            else return! getAndApplyGroupCoordinator state gid
          | RouteType.TopicRoute tns ->
            if critical then return! metadata state tns
            else return! getAndApplyMetadata state tns }
        return! routeSendInternal critical rs state' req
      | None ->
        return failwithf "missng_route|attempts=%i route_type=%A" rs.attempt rt }

  /// Sends a request to a specific broker and handles failures.
  and sendInternal (critical:bool) (rs:RetryState) (state:ConnState) (ch:Chan) (req:RequestMessage) = async {
    return!
      req
      |> Chan.send ch
      |> Async.bind (fun chanRes -> async {
        match chanRes with
        | Success res ->
          match RetryAction.tryFindError res with
          | None -> 
            return res
          | Some (errorCode,action) ->
            Log.error "channel_response_errored|client_id=%s endpoint=%O error_code=%i retry_action=%A req=%s res=%s" 
              cfg.clientId (Chan.endpoint ch) errorCode action (RequestMessage.Print req) (ResponseMessage.Print res)
            match action with
            | RetryAction.PassThru ->
              return res
            | RetryAction.Escalate ->
              return raise (EscalationException (errorCode,req,res,(sprintf "endpoint=%O" (Chan.endpoint ch))))
            | RetryAction.RefreshMetadataAndRetry topics ->
              let! rs' = RetryPolicy.awaitNextState cfg.requestRetryPolicy rs
              match rs' with
              | Some rs ->
                let! state' = 
                  if critical then metadata state topics
                  else getAndApplyMetadata state topics
                return! routeSendInternal critical rs state' req
              | None ->
                return failwithf "request_failure|attempt=%i request=%s response=%s" 
                  rs.attempt (RequestMessage.Print req) (ResponseMessage.Print res)
            | RetryAction.WaitAndRetry ->
              let! rs' = RetryPolicy.awaitNextState cfg.requestRetryPolicy rs
              match rs' with
              | Some rs ->
                return! routeSendInternal critical rs state req
              | None ->
                return failwithf "request_failure|attempt=%i request=%s response=%s" 
                  rs.attempt (RequestMessage.Print req) (ResponseMessage.Print res)
        | Failure chanErr ->
          let! rs' = RetryPolicy.awaitNextState cfg.requestRetryPolicy rs
          match rs' with
          | Some rs ->
            let! state' = handleChannelError critical state (ch, req, chanErr)
            return! routeSendInternal critical rs state' req 
          | None ->
            return failwithf 
              "channel_failure|attempt=%i ep=%O request=%s" rs.attempt (Chan.endpoint ch) (RequestMessage.Print req) })
      |> Async.tryWith (fun ex -> async {
        Log.error "channel_exception|client_id=%s endpoint=%O request=%s error=%O" 
          cfg.clientId (Chan.endpoint ch) (RequestMessage.Print req) ex
        return raise ex }) }

  /// Handles a failure to communicate with a broker.
  and handleChannelError (critical:bool) (state:ConnState) (ch:Chan, req:RequestMessage, chanErrs:ChanError list) = async {
    Log.error "recovering_channel_error|client_id=%s endpoint=%O request=%s error=%A" 
      cfg.clientId (Chan.endpoint ch) (RequestMessage.Print req) chanErrs
    let isBootstrapRequest =
      match RouteType.ofRequest req with
      | RouteType.BootstrapRoute -> true
      | _ -> false
    let! state = 
      if critical then removeBroker state ch
      else removeBrokerAndApply ch
    if isBootstrapRequest then
      let! state' = 
        if critical then bootstrap (Some state)
        else getAndApplyBootstrap
      return state'
    else
      return! refreshMetadata critical state }

  /// Gets the cancellation token triggered when the connection is closed.
  member internal __.CancellationToken = cts.Token

  member internal __.Config = cfg

  member private __.GetState () =
    let state = MVar.getFastUnsafe stateCell
    if state.IsNone then
      invalidOp "Connection state unavailable; must not be connected."
    else
      state.Value

  member internal __.Send (req:RequestMessage) : Async<ResponseMessage> = async {
    let state = __.GetState ()
    return! routeSendInternal false RetryState.init state req }
  
  /// Connects to a broker from the bootstrap list.
  member internal __.Connect () = async {
    let! _ = getAndApplyBootstrap
    return () }

  member internal __.GetGroupCoordinator (groupId:GroupId) = async {
    let! state = MVar.get stateCell
    return! getAndApplyGroupCoordinator state groupId }

  member internal __.GetMetadataState (topics:TopicName[]) = async {
    let! state = MVar.get stateCell
    return! getAndApplyMetadata state topics }

  member internal __.GetMetadata (topics:TopicName[]) = async {
    let! state' = __.GetMetadataState topics
    return state' |> ConnState.topicPartitions |> Map.onlyKeys topics }

  member internal __.RemoveBroker (ch:Chan) =
    removeBrokerAndApply ch

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
  
  /// Connects to a Kafka cluster.
  let connAsync (cfg:KafkaConfig) = async {
    let conn = new KafkaConn(cfg)
    do! conn.Connect ()
    return conn }

  /// Connects to a Kafka cluster.
  let conn cfg =
    connAsync cfg |> Async.RunSynchronously

  /// Connects to a Kafka cluster given a default configuration.
  let connHostAsync (host:string) =
    let uri = KafkaUri.parse host
    let cfg = KafkaConfig.create [uri]
    connAsync cfg

  /// Connects to a Kafka cluster given a default configuration.
  let connHost host =
    connHostAsync host |> Async.RunSynchronously

  let metadata (c:KafkaConn) : Metadata.Request -> Async<MetadataResponse> =
    AsyncFunc.dimap RequestMessage.Metadata ResponseMessage.toMetadata c.Send

  let fetch (c:KafkaConn) : FetchRequest -> Async<FetchResponse> =
    AsyncFunc.dimap RequestMessage.Fetch ResponseMessage.toFetch c.Send

  let produce (c:KafkaConn) : ProduceRequest -> Async<ProduceResponse> =
    AsyncFunc.dimap RequestMessage.Produce ResponseMessage.toProduce c.Send

  let offset (c:KafkaConn) : OffsetRequest -> Async<OffsetResponse> =
    AsyncFunc.dimap RequestMessage.Offset ResponseMessage.toOffset c.Send

  let groupCoordinator (c:KafkaConn) : GroupCoordinatorRequest -> Async<GroupCoordinatorResponse> =
    AsyncFunc.dimap RequestMessage.GroupCoordinator ResponseMessage.toGroupCoordinator c.Send

  let offsetCommit (c:KafkaConn) : OffsetCommitRequest -> Async<OffsetCommitResponse> =
    AsyncFunc.dimap RequestMessage.OffsetCommit ResponseMessage.toOffsetCommit c.Send

  let offsetFetch (c:KafkaConn) : OffsetFetchRequest -> Async<OffsetFetchResponse> =
    AsyncFunc.dimap RequestMessage.OffsetFetch ResponseMessage.toOffsetFetch c.Send

  let joinGroup (c:KafkaConn) : JoinGroup.Request -> Async<JoinGroup.Response> =
    AsyncFunc.dimap RequestMessage.JoinGroup ResponseMessage.toJoinGroup c.Send

  let syncGroup (c:KafkaConn) : SyncGroupRequest -> Async<SyncGroupResponse> =
    AsyncFunc.dimap RequestMessage.SyncGroup ResponseMessage.toSyncGroup c.Send

  let heartbeat (c:KafkaConn) : HeartbeatRequest -> Async<HeartbeatResponse> =
    AsyncFunc.dimap RequestMessage.Heartbeat ResponseMessage.toHeartbeat c.Send

  let leaveGroup (c:KafkaConn) : LeaveGroupRequest -> Async<LeaveGroupResponse> =
    AsyncFunc.dimap RequestMessage.LeaveGroup ResponseMessage.toLeaveGroup c.Send

  let listGroups (c:KafkaConn) : ListGroupsRequest -> Async<ListGroupsResponse> =
    AsyncFunc.dimap RequestMessage.ListGroups ResponseMessage.toListGroups c.Send

  let describeGroups (c:KafkaConn) : DescribeGroupsRequest -> Async<DescribeGroupsResponse> =
    AsyncFunc.dimap RequestMessage.DescribeGroups ResponseMessage.toDescribeGroups c.Send



/// Operations on offsets.
module Offsets =

  /// Gets available offsets for the specified topic, at the specified times.
  /// Returns a map of times to offset responses.
  /// If empty is passed in for Partitions, will return information for all partitions.
  let offsets (conn:KafkaConn) (topic:TopicName) (partitions:Partition seq) (times:Time seq) (maxOffsets:MaxNumberOfOffsets) : Async<Map<Time, OffsetResponse>> = async {
    let! partitions = async {
      if Seq.isEmpty partitions then
        let! meta = conn.GetMetadata [|topic|]
        return
          meta
          |> Seq.collect (fun kvp -> kvp.Value)
          |> Seq.toArray
      else
        return partitions |> Seq.toArray }
    return!
      times
      |> Seq.map (fun time -> async {
        let offsetReq = OffsetRequest(-1, [| topic, partitions |> Array.map (fun p -> p,time,maxOffsets) |]) 
        let! offsetRes = Kafka.offset conn offsetReq
        return time,offsetRes })
      |> Async.Parallel
      |> Async.map (Map.ofArray) }

  /// Gets the offset range (Time.EarliestOffset,Time.LatestOffset) for a topic, for the specified partitions.
  /// If empty is passed in for Partitions, will return information for all partitions.
  let offsetRange (conn:KafkaConn) (topic:TopicName) (partitions:Partition seq) : Async<Map<Partition, Offset * Offset>> = async {
    let! offsets = offsets conn topic partitions [Time.EarliestOffset;Time.LatestOffset] 1
    
    let filter (res:OffsetResponse) =
      res.topics
      |> Seq.collect (fun (t,ps) -> 
        if t = topic then ps |> Seq.map (fun p -> p.partition, p.offsets.[0])
        else Seq.empty)
      |> Map.ofSeq

    let earliest = offsets |> Map.find Time.EarliestOffset |> filter
    let latest = offsets |> Map.find Time.LatestOffset |> filter

    let offsets =
      (earliest,latest)
      ||> Map.mergeChoice (fun _ -> function
        | Choice1Of3 (e,l) -> (e,l)
        | Choice2Of3 e -> (e,-1L)
        | Choice3Of3 l -> (-1L,l))
    
    return offsets }



  type private PeriodicCommitQueueMsg =
    | Enqueue of (Partition * Offset) seq
    | Commit of AsyncReplyChannel<unit> option

  /// A queue for offsets to be periodically committed.
  type PeriodicCommitQueue internal (interval:TimeSpan, commit:(Partition * Offset)[] -> Async<unit>) =
  
    let cts = new CancellationTokenSource()

    let rec enqueueLoop (commits:Map<Partition, Offset>) (mb:Mb<_>) = async {
      let! msg = mb.Receive ()
      match msg with
      | Enqueue os ->
        let commits' =
          (commits,os) 
          ||> Seq.fold (fun m (p,o) -> Map.add p o m) 
        return! enqueueLoop commits' mb
      | Commit rep ->
        let offsets =
          commits
          |> Map.toSeq
          |> Seq.map (fun (p,o) -> p,o)
          |> Seq.toArray
        if offsets.Length > 0 then
          do! commit offsets
        rep |> Option.iter (fun r -> r.Reply())
        return! enqueueLoop Map.empty mb }

    let mbp = Mb.Start (enqueueLoop Map.empty, cts.Token)
  
    let rec commitLoop = async {
      do! Async.Sleep interval
      mbp.Post (Commit None)
      return! commitLoop }

    do Async.Start (commitLoop, cts.Token)

    member internal __.Enqueue (os:(Partition * Offset) seq) =
      mbp.Post (Enqueue os)

    member internal __.Flush () =
      mbp.PostAndAsyncReply (fun ch -> Commit (Some ch))

    interface IDisposable with
      member __.Dispose () =
        cts.Cancel ()
        (mbp :> IDisposable).Dispose ()

  /// Creates a periodic offset commit queue which commits enqueued commits at the specified interval.
  let createPeriodicCommitQueue interval = 
    new PeriodicCommitQueue (interval)

  /// Asynchronously enqueues offsets to commit, replacing any existing commits for the specified topic-partitions.
  let enqueuePeriodicCommit (q:PeriodicCommitQueue) (os:(Partition * Offset) seq) =
    q.Enqueue os
    
  /// Commits whatever remains in the queue.
  let flushPeriodicCommit (q:PeriodicCommitQueue) =
    q.Flush ()

  

  