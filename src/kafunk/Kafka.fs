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
  version : int
  channels : Map<EndPoint, Chan>  
  bootstrapHost : EndPoint
  bootstrapBroker : Chan
  nodeToHost : Map<NodeId, EndPoint>
  topicToNode : Map<TopicName * Partition, NodeId>
  groupToHost : Map<GroupId, EndPoint>
} with
  
  static member bootstrap (bootstrapCh:Chan) =
    let ep = Chan.endpoint bootstrapCh
    {
      channels = [ep,bootstrapCh] |> Map.ofList
      bootstrapHost = ep
      bootstrapBroker = bootstrapCh
      nodeToHost = Map.empty
      topicToNode = Map.empty
      groupToHost = Map.empty
      version = 0
    }

  static member topicPartitions (s:ConnState) =
    s.topicToNode 
    |> Map.toSeq 
    |> Seq.map fst 
    |> Seq.groupBy fst
    |> Seq.map (fun (tn,xs) -> tn, xs |> Seq.map snd |> Seq.toArray)
    |> Map.ofSeq

  static member private tryFindChanByEndPoint (ep:EndPoint) (s:ConnState) =
    s.channels |> Map.tryFind ep

//  static member bootstrapBroker (s:ConnState) =    
//    ConnState.tryFindChanByEndPoint s.bootstrapHost s

  static member groupCoordinatorBroker (groupId:GroupId) (s:ConnState) =
    s.groupToHost
    |> Map.tryFind groupId
    |> Option.bind (fun ep -> ConnState.tryFindChanByEndPoint ep s)

  static member topicPartitionBroker (tn:TopicName, p:Partition) (s:ConnState) =
    s.topicToNode
    |> Map.tryFind (tn,p)
    |> Option.bind (fun nid -> 
      s.nodeToHost
      |> Map.tryFind nid
      |> Option.bind (fun ep -> ConnState.tryFindChanByEndPoint ep s))

  static member tryFindChanByNodeId (nid:NodeId) (s:ConnState) =
    s.nodeToHost
    |> Map.tryFind nid
    |> Option.bind (fun ep -> ConnState.tryFindChanByEndPoint ep s)
    
  static member updateTopicPartitionRoutes (brokers:(NodeId * Chan) seq) (topicNodes:seq<TopicName * Partition * NodeId>) (s:ConnState) =
    {
      s with
          nodeToHost = 
            s.nodeToHost
            |> Map.addMany (brokers |> Seq.map (fun (nodeId,ch) -> nodeId, Chan.endpoint ch))
          topicToNode = 
            s.topicToNode
            |> Map.addMany (topicNodes|> Seq.map (fun (tn,p,leaderId) -> (tn, p), leaderId))
          channels = s.channels |> Map.addMany (brokers |> Seq.map (fun (_,ch) -> Chan.endpoint ch, ch))
          version = s.version + 1
    }

  static member updateGroupCoordinatorRoute (ch:Chan) (gid:GroupId, nodeId:CoordinatorId) (s:ConnState) = 
    let ep = Chan.endpoint ch
    {
      s with
        groupToHost = s.groupToHost |> Map.add gid ep
        nodeToHost = s.nodeToHost |> Map.add nodeId ep
        channels = s.channels |> Map.add (Chan.endpoint ch) ch
        version = s.version + 1
    }

  static member updateBootstrap (bootstrapCh:Chan) (s:ConnState) =
    {
      s with
        channels = s.channels |> Map.add (Chan.endpoint bootstrapCh) bootstrapCh
        bootstrapHost = (Chan.endpoint bootstrapCh)
        bootstrapBroker = bootstrapCh
        version = s.version + 1
    }


type RouteType =
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
    |> Seq.groupBy (fun (tn, p, _, _) -> ConnState.topicPartitionBroker (tn, p) state |> Result.ofOptionMap (fun () -> tn))
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
    |> Seq.groupBy (fun (t, p, _, _) -> ConnState.topicPartitionBroker (t, p) state |> Result.ofOptionMap (fun () -> t))
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
    |> Seq.groupBy (fun (t, p, _, _) -> ConnState.topicPartitionBroker (t, p) state |> Result.ofOptionMap (fun () -> t))
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
      Success [| req, state.bootstrapBroker |]

    let topicRoute (xs:(Result<Chan, TopicName> * RequestMessage)[]) =
      xs
      |> Result.traverse (fun (routeRes,req) ->
        match routeRes with
        | Success ch -> Success (req,ch)
        | Failure tn -> Failure (RouteType.TopicRoute [|tn|]))

    let groupRoute req gid =
      match ConnState.groupCoordinatorBroker gid state with
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

  /// The default bootstrap broker connection retry policy = RetryPolicy.constantMs 5000 |> RetryPolicy.maxAttempts 3.
  static member DefaultBootstrapConnectRetryPolicy = RetryPolicy.constantMs 5000 |> RetryPolicy.maxAttempts 3

  /// Creates a Kafka configuration object given the specified list of broker hosts to bootstrap with.
  /// The first host to which a successful connection is established is used for a subsequent metadata request
  /// to build a routing table mapping topics and partitions to brokers.
  static member create (bootstrapServers:Uri list, ?clientId:ClientId, ?tcpConfig, ?bootstrapConnectRetryPolicy, ?version) =
    { version = defaultArg version KafkaConfig.DefaultVersion
      bootstrapServers = bootstrapServers
      bootstrapConnectRetryPolicy = defaultArg bootstrapConnectRetryPolicy KafkaConfig.DefaultBootstrapConnectRetryPolicy
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

  // TODO: configure with RetryPolicy
  let waitRetrySleepMs = 5000

  let metadataRequestRetryPolicy = RetryPolicy.constantBoundedMs 1000 5

  let stateCell : MVar<ConnState> = MVar.create ()
  let cts = new CancellationTokenSource()

  /// Connects to the broker at the specified host.
  let connBroker (host:Host, port:Port) = async {    
    let! ips = async {
      match IPAddress.tryParse host with  
      | Some ip ->
        return [|ip|]
      | None ->
        Log.info "discovering_dns|client_id=%s host=%s" cfg.clientId host
        let! ips = Dns.IPv4.getAllAsync host
        Log.info "discovered_dns|client_id=%s host=%s ips=[%s]" cfg.clientId host (Printers.stringsCsv ips)
        return ips }
    return!
      ips
      |> Seq.map (fun ip -> EndPoint.ofIPAddressAndPort (ip, port))
      |> AsyncSeq.ofSeq
      |> AsyncSeq.traverseAsyncResult Exn.monoid (fun ep -> async {
        try
          let! ch = Chan.connect (cfg.version, cfg.tcpConfig, cfg.clientId) ep
          return Success ch
        with ex ->
          return Failure ex }) }

  /// Connects to the first available broker in the bootstrap list and returns the 
  /// initial routing table.
  let rec bootstrap = async {
    let update (prevState:ConnState option) = async {
      Log.info "connecting_to_bootstrap_brokers|client_id=%s brokers=%A" cfg.clientId cfg.bootstrapServers
      return!
        cfg.bootstrapServers
        |> AsyncSeq.ofSeq
        |> AsyncSeq.traverseAsyncResult Exn.monoid (fun uri -> async {
          let! ch = connBroker (uri.Host,uri.Port)
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
      |> Faults.AsyncFunc.retryResultThrow 
          (fun e -> exn("Failed to connect to a bootstrap broker.", e)) 
          Exn.monoid 
          cfg.bootstrapConnectRetryPolicy
    return!
      stateCell 
      |> MVar.putOrUpdateAsync update }

  /// Discovers TopicName * Partition routes.
  and getMetadata (callerState:ConnState) (topics:TopicName[]) =
    let update (currentState:ConnState) = async {
      if currentState.version = callerState.version then
        let send = 
          Chan.send currentState.bootstrapBroker
          |> AsyncFunc.dimap RequestMessage.Metadata (Result.map (ResponseMessage.toMetadata))
          |> Faults.AsyncFunc.retryResultThrowList 
              (fun errs -> exn(sprintf "Metadata request failed=%A" (List.concat errs))) 
              metadataRequestRetryPolicy
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
            match ConnState.tryFindChanByNodeId b.nodeId currentState with
            | Some ch -> 
              return Success (b.nodeId, ch)
            | None ->
              return! connBroker (b.host, b.port) |> Async.map (Result.map (fun ch -> b.nodeId, ch)) })
          |> Async.Parallel
          |> Async.map (Result.traverse id >> Result.throw)
        let topicNodes =
          metadata.topicMetadata 
          |> Seq.collect (fun tmd -> 
            tmd.partitionMetadata 
            |> Seq.choose (fun pmd -> 
              if pmd.leader >= 0 then Some (tmd.topicName, pmd.partitionId, pmd.leader) 
              else None))
        return currentState |> ConnState.updateTopicPartitionRoutes brokers topicNodes
      else
        return currentState }
    stateCell |> MVar.updateAsync update

  and refreshMetadata (callerState:ConnState) =
    let topics = 
      ConnState.topicPartitions callerState
      |> Seq.map (fun kvp -> kvp.Key)
      |> Seq.toArray
    Log.info "refreshing_metadata|client_id=%s topics=%A" cfg.clientId topics
    getMetadata callerState topics

  /// Discovers a coordinator for the group.
  and getGroupCoordinator (callerState:ConnState) (groupId:GroupId) =
    let update (currentState:ConnState) = async {
      if currentState.version = callerState.version then
        let send = 
          Chan.send currentState.bootstrapBroker
          |> AsyncFunc.dimap RequestMessage.GroupCoordinator (Result.map (ResponseMessage.toGroupCoordinator))
          |> Faults.AsyncFunc.retryResultThrowList 
              (fun errs -> exn(sprintf "Group coordinator request failed=%A" (List.concat errs))) 
              metadataRequestRetryPolicy
        let! res = send (GroupCoordinatorRequest(groupId))
        Log.info "received_group_coordinator|client_id=%s group_id=%s %s" 
          cfg.clientId groupId (GroupCoordinatorResponse.Print res)
        let! ch = async {
          match ConnState.tryFindChanByNodeId res.coordinatorId currentState with
          | Some ch -> 
            return ch
          | None ->      
            let! ch = connBroker (res.coordinatorHost, res.coordinatorPort)
            let ch = Result.throw ch
            return ch }
        return 
          currentState 
          |> ConnState.updateGroupCoordinatorRoute ch (groupId,res.coordinatorId)
      else
        return currentState }
    stateCell |> MVar.updateAsync update

  /// Sends the request based on discovered routes.
  and send (state:ConnState) (req:RequestMessage) = async {
    match Routing.route state req with
    | Success requestRoutes ->
      let sendHost (req:RequestMessage, ch:Chan) = async {
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
                  let! state' = getMetadata state topics
                  return! send state' req
                | RetryAction.WaitAndRetry ->
                  do! Async.Sleep waitRetrySleepMs
                  return! send state req
            | Failure chanErr ->
              let! state' = handleChannelFailure state (req, (Chan.endpoint ch), chanErr)
              return! send state' req })
          |> Async.tryWith (fun ex -> async {
            Log.error "channel_exception_escalated|client_id=%s endpoint=%O request=%s error=%O" 
              cfg.clientId (Chan.endpoint ch) (RequestMessage.Print req) ex
            do! Async.Sleep 1000
            return raise ex }) }
            // TODO: escalate
            //let! state' = handleRequestFailure (state, req, (Chan.endpoint ch), [])
            //return! send state' req }) }
      
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
        return! sendHost requestRoutes.[0]

    | Failure rt ->
      Log.trace "missing_route|route_type=%A request=%s" rt (RequestMessage.Print req)
      let! state' = async {
        match rt with
        | RouteType.BootstrapRoute ->
          return! bootstrap
        | RouteType.GroupRoute gid ->
          return! getGroupCoordinator state gid
        | RouteType.TopicRoute tns ->
          return! getMetadata state tns }      
      return! send state' req }

  and handleChannelFailure (callerState:ConnState) (req:RequestMessage, ep:EndPoint, chanErrs:ChanError list) = async {
    Log.error "recovering_channel_error|client_id=%s endpoint=%O request=%s error=%A" 
      cfg.clientId ep (RequestMessage.Print req) chanErrs
    let isBootstrapRequest =
      match RouteType.ofRequest req with
      | RouteType.BootstrapRoute -> true
      | _ -> false    
    if isBootstrapRequest then
      let! state' = bootstrap
      return state'
    else
      let! state' = refreshMetadata callerState
      return state' }
    
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
    return! send state req }
  
  /// Connects to a broker from the bootstrap list.
  member internal __.Connect () = async {
    let! _ = bootstrap
    return () }

  member internal __.GetGroupCoordinator (groupId:GroupId) = async {
    let state = __.GetState ()
    return! getGroupCoordinator state groupId }

  member internal __.GetMetadataState (topics:TopicName[]) = async {
    let state = __.GetState ()
    let! state' = getMetadata state topics
    return state' }

  member internal __.GetMetadata (topics:TopicName[]) = async {
    let! state' = __.GetMetadataState topics
    return state' |> ConnState.topicPartitions |> Map.onlyKeys topics }

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

  

  