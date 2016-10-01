namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading

open Kafunk
open Kafunk.Prelude
open Kafunk.Protocol

module Message =

  let create value key attrs =
    Message(0, 0y, (defaultArg attrs 0y), (defaultArg key (Binary.empty)), value)

  let ofBuffer data key =
    Message(0, 0y, 0y, (defaultArg  key (Binary.empty)), data)

  let ofBytes value key =
    let key =
      match key with
      | Some key -> Binary.ofArray key
      | None -> Binary.empty
    Message(0, 0y, 0y, key, Binary.ofArray value)

  let ofString (value:string) (key:string) =
    let value = Encoding.UTF8.GetBytes value |> Binary.ofArray
    let key = Encoding.UTF8.GetBytes key |> Binary.ofArray
    Message(0, 0y, 0y, key, value)

  let valueString (m:Message) =
    m.value |> Binary.toString

  let keyString (m:Message) =
    if isNull m.value.Array then null
    else m.value |> Binary.toString

module MessageSet =

  let ofMessage (m:Message) =
    MessageSet([| 0L, Message.size m, m |])

  let ofMessages ms =
    MessageSet(ms |> Seq.map (fun m -> 0L, Message.size m, m) |> Seq.toArray)

  /// Returns the next offset to fetch, by taking the max offset in the
  /// message set and adding one.
  let nextOffset (ms:MessageSet) (hwm:HighwaterMarkOffset) =
    if ms.messages.Length > 0 then
      //let maxOffset = ms.messages |> Seq.map (fun (off, _, _) -> off) |> Seq.max
      let (maxOffset,_,_) = ms.messages.[ms.messages.Length - 1]
      let no = maxOffset + 1L
      if no <= hwm then no
      else 
        failwithf "invalid offset computation maxOffset=%i hwm=%i" maxOffset hwm
    else
      1L

module ProduceRequest =

  let ofMessageSet topic partition ms requiredAcks timeout =
    ProduceRequest(
      (defaultArg requiredAcks RequiredAcks.Local),
      (defaultArg timeout 1000),
      [| topic, [| partition, MessageSet.size ms, ms |] |] )

  let ofMessageSets topic ms requiredAcks timeout =
    ProduceRequest(
      (defaultArg requiredAcks RequiredAcks.Local),
      (defaultArg timeout 1000),
      [| topic, ms |> Array.map (fun (p, ms) -> (p, MessageSet.size ms, ms)) |])

  let ofMessageSetTopics ms requiredAcks timeout =
    ProduceRequest(requiredAcks, timeout,
      ms |> Array.map (fun (t, ms) -> (t, ms |> Array.map (fun (p, ms) -> (p, MessageSet.size ms, ms)))))

module FetchRequest =

  let ofTopicPartition topic partition offset maxWaitTime minBytes maxBytesPerPartition =
    FetchRequest(-1, maxWaitTime, minBytes , [| topic, [| partition,  offset, maxBytesPerPartition |] |])

  let ofTopicPartitions topic ps maxWaitTime minBytes maxBytesPerPartition =
    FetchRequest(-1, maxWaitTime, minBytes, [| (topic, ps |> Array.map (fun (p, o) -> (p, o, maxBytesPerPartition))) |])


// -------------------------------------------------------------------------------------------------------------------------------------





// Connection


[<AutoOpen>]
module internal ResponseEx =

  let wrongResponse () =
    failwith (sprintf "Wrong response!")

  type RequestMessage with    
    /// If a request does not expect a response, returns the default response.
    /// Used to generate responses for requests which don't expect a response from the server.
    static member awaitResponse (x:RequestMessage) =
      match x with
      | RequestMessage.Produce req when req.requiredAcks = RequiredAcks.None ->
        Some(ResponseMessage.ProduceResponse(new ProduceResponse([||], 0)))
      | _ -> None

  type ResponseMessage with
    static member internal toFetch res = match res with FetchResponse x -> x | _ -> wrongResponse ()
    static member internal toProduce res = match res with ProduceResponse x -> x | _ -> wrongResponse ()
    static member internal toOffset res = match res with OffsetResponse x -> x | _ -> wrongResponse ()
    static member internal toGroupCoordinator res = match res with GroupCoordinatorResponse x -> x | _ -> wrongResponse ()
    static member internal toOffsetCommit res = match res with OffsetCommitResponse x -> x | _ -> wrongResponse ()
    static member internal toOffsetFetch res = match res with OffsetFetchResponse x -> x | _ -> wrongResponse ()
    static member internal toJoinGroup res = match res with JoinGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toSyncGroup res = match res with SyncGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toHeartbeat res = match res with HeartbeatResponse x -> x | _ -> wrongResponse ()
    static member internal toLeaveGroup res = match res with LeaveGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toListGroups res = match res with ListGroupsResponse x -> x | _ -> wrongResponse ()
    static member internal toDescribeGroups res = match res with DescribeGroupsResponse x -> x | _ -> wrongResponse ()

    static member isError (x:ResponseMessage) =
      match x with
      | ResponseMessage.DescribeGroupsResponse r -> r.groups |> Seq.exists (fun (ec,_,_,_,_,_) -> ErrorCode.isError ec)
      | ResponseMessage.FetchResponse r -> r.topics |> Seq.exists (fun (_,xs) -> xs |> Seq.exists (fun (_,ec,_,_,_) -> ErrorCode.isError ec))
      | _ -> false
      

  // ------------------------------------------------------------------------------------------------------------------------------
  // printers

  type MetadataResponse with
    static member Print (x:MetadataResponse) =
      let topics =
        x.topicMetadata
        |> Seq.map (fun tmd -> 
          let partitions = 
            tmd.partitionMetadata
            |> Seq.map (fun pmd -> sprintf "(partition_id=%i leader=%i)" pmd.partitionId pmd.leader)
            |> String.concat " ; "
          sprintf "[topic=%s partitions=%s]" tmd.topicName partitions)
        |> String.concat " ; "
      let brokers =
        x.brokers
        |> Seq.map (fun b -> sprintf "(node_id=%i host=%s port=%i)" b.nodeId b.host b.port)
        |> String.concat " ; "
      sprintf "MetadataResponse|brokers=%s|topics=[%s]" brokers topics

  type FetchRequest with
    static member Print (x:FetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps = 
            ps
            |> Seq.map (fun (p,o,_mb) -> sprintf "(partition=%i offset=%i)" p o)
            |> String.concat " ; "        
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "FetchRequest|%s" ts

  type FetchResponse with
    static member Print (x:FetchResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec,hwmo,mss,_ms) -> sprintf "(topic=%s partition=%i error_code=%i high_watermark_offset=%i message_set_size=%i)" tn p ec hwmo mss)
            |> String.concat ";"
          sprintf "topic=%s [%s]" tn ps)
        |> String.concat " ; "
      sprintf "FetchResponse|%s" ts

  type OffsetFetchRequest with
    static member Print (x:OffsetFetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun p -> sprintf "partition=%i" p)
            |> String.concat " ; "
          sprintf "topic=%s partitions=%s" tn ps)
        |> String.concat " ; "
      sprintf "OffsetFetchRequest|group_id=%s topics=%s" x.consumerGroup ts

  type OffsetFetchResponse with
    static member Print (x:OffsetFetchResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,o,_md,ec) -> sprintf "partition=%i offset=%i error_code=%i" p o ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetFetchResponse|%s" ts

  type OffsetCommitRequest with
    static member Print (x:OffsetCommitRequest) =
      let topics =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,o,_) -> sprintf "(partition=%i offset=%i)" p o)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetCommitRequest|group_id=%s member_id=%s generation_id=%i topics=%s" x.consumerGroup x.consumerId x.consumerGroupGenerationId topics

  type OffsetCommitResponse with
    static member Print (x:OffsetCommitResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec) -> sprintf "(partition=%i error_code=%i)" p ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetCommitResponse|%s" ts

  type HeartbeatRequest with
    static member Print (x:HeartbeatRequest) =
      sprintf "HeartbeatRequest|group_id=%s member_id=%s generation_id=%i" x.groupId x.memberId x.generationId

  type HeartbeatResponse with
    static member Print (x:HeartbeatResponse) =
      sprintf "HeartbeatResponse|error_code=%i" x.errorCode

  type RequestMessage with
    static member Print (x:RequestMessage) =
      match x with
      | RequestMessage.Fetch x -> FetchRequest.Print x
      | RequestMessage.OffsetCommit x -> OffsetCommitRequest.Print x
      | RequestMessage.OffsetFetch x -> OffsetFetchRequest.Print x
      | _ ->  sprintf "%A" x

  type ResponseMessage with
    static member Print (x:ResponseMessage) =
      match x with
      | ResponseMessage.MetadataResponse x -> MetadataResponse.Print x
      | ResponseMessage.FetchResponse x -> FetchResponse.Print x
      | ResponseMessage.OffsetCommitResponse x -> OffsetCommitResponse.Print x
      | ResponseMessage.OffsetFetchResponse x -> OffsetFetchResponse.Print x
      | x -> sprintf "%A" x

  // ------------------------------------------------------------------------------------------------------------------------------


/// A request/reply channel to Kafka.
type Chan = RequestMessage -> Async<ResponseMessage>

/// API operations on a generic request/reply channel.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Chan =

  let private Log = Log.create "Kafunk.Chan"

  let send (ch:Chan) req  = ch req

  let metadata (ch:Chan) (req:Metadata.Request) =
    ch (RequestMessage.Metadata req) |> Async.map (function MetadataResponse x -> x | _ -> wrongResponse ())

  let fetch (ch:Chan) (req:FetchRequest) : Async<FetchResponse> =
    ch (RequestMessage.Fetch req) |> Async.map ResponseMessage.toFetch

  let produce (ch:Chan) (req:ProduceRequest) : Async<ProduceResponse> =
    ch (RequestMessage.Produce req) |> Async.map ResponseMessage.toProduce

  let offset (ch:Chan) (req:OffsetRequest) : Async<OffsetResponse> =
    ch (RequestMessage.Offset req) |> Async.map ResponseMessage.toOffset

  let groupCoordinator (ch:Chan) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> =
    ch (RequestMessage.GroupCoordinator req) |> Async.map ResponseMessage.toGroupCoordinator

  let offsetCommit (ch:Chan) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> =
    ch (RequestMessage.OffsetCommit req) |> Async.map ResponseMessage.toOffsetCommit

  let offsetFetch (ch:Chan) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    ch (RequestMessage.OffsetFetch req) |> Async.map ResponseMessage.toOffsetFetch

  let joinGroup (ch:Chan) (req:JoinGroup.Request) : Async<JoinGroup.Response> =
    ch (RequestMessage.JoinGroup req) |> Async.map ResponseMessage.toJoinGroup

  let syncGroup (ch:Chan) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    ch (RequestMessage.SyncGroup req) |> Async.map ResponseMessage.toSyncGroup

  let heartbeat (ch:Chan) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    ch (RequestMessage.Heartbeat req) |> Async.map ResponseMessage.toHeartbeat

  let leaveGroup (ch:Chan) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    ch (RequestMessage.LeaveGroup req) |> Async.map ResponseMessage.toLeaveGroup

  let listGroups (ch:Chan) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    ch (RequestMessage.ListGroups req) |> Async.map ResponseMessage.toListGroups

  let describeGroups (ch:Chan) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    ch (RequestMessage.DescribeGroups req) |> Async.map ResponseMessage.toDescribeGroups


  /// Configuration for an individual TCP connection.
  type Config = {
    useNagle : bool    
    receiveBufferSize : int
  } with
    static member create (?useNagle, ?receiveBufferSize) =
      {
        useNagle=defaultArg useNagle false
        receiveBufferSize=defaultArg receiveBufferSize 8192
      }



  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  /// Only a single channel per endpoint is needed.
  let connect (config:Config) (clientId:ClientId) (ep:IPEndPoint) : Async<Chan> = async {

    /// Builds and connects the socket.
    let conn = async {
      
      let connSocket =
        new Socket(
          ep.AddressFamily,
          SocketType.Stream,
          ProtocolType.Tcp,
          NoDelay=not(config.useNagle),
          ExclusiveAddressUse=true,
          ReceiveBufferSize=config.receiveBufferSize)
      Log.info "connecting|remote_endpoint=%O client_id=%s" ep clientId 
      let! sendRcvSocket = Socket.connect connSocket ep
      Log.info "connected|remote_endpoint=%O local_endpoint=%O" sendRcvSocket.RemoteEndPoint sendRcvSocket.LocalEndPoint
      return sendRcvSocket }

    let recovery (s:Socket, ex:exn) = async {
      Log.info "recovering_tcp_connection|client_id=%s remote_endpoint=%O from error=%O" clientId ep ex
      do! Socket.disconnect s false
      s.Dispose()      
      match ex with
      | :? SocketException as _x ->
        return Resource.Recovery.Recreate
      | _ ->
        return Resource.Recovery.Escalate }

    let! sendRcvSocket = 
      Resource.recoverableRecreate 
        conn 
        recovery

    let send =
      sendRcvSocket
      |> Resource.inject Socket.sendAll   

    // re-connect -> restart
    let receive =
      let receive s b = 
        Socket.receive s b
        |> Async.map (fun received -> 
          if received = 0 then raise(SocketException(int SocketError.ConnectionAborted)) 
          else received)
      sendRcvSocket
      |> Resource.inject receive

    /// An unframed input stream.
    let inputStream =
      Socket.receiveStreamFrom config.receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send (data:Binary.Segment) =
      let framed = data |> Framing.LengthPrefix.frame
      send framed

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let version = 
        match req with
        | RequestMessage.OffsetFetch _ -> 1s
        | RequestMessage.OffsetCommit _ -> 2s
        | _ -> 0s
      let req = Request(version, correlationId, clientId, req)
      let sessionData = toArraySeg Request.size Request.write req
      sessionData, req.apiKey

    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, buf:Binary.Segment) =
      ResponseMessage.readApiKey apiKey buf

    let session =
      Session.requestReply
        Session.corrId encode decode RequestMessage.awaitResponse inputStream send

    return 
      session.Send
      |> AsyncFunc.doBeforeAfterError 
          (fun a -> Log.trace "sending_request|request=%A" (RequestMessage.Print a))
          (fun (_,b) -> Log.trace "received_response|response=%A" (ResponseMessage.Print b))
          (fun (a,e) -> Log.error "request_errored|request=%A error=%O" (RequestMessage.Print a) e) }

  let connectHost (config:Config) (clientId:ClientId) (host:Host, port:Port) = async {
    Log.info "discovering_dns_entries|host=%s" host
    let! ips = Dns.IPv4.getAllAsync host
    let ip = ips.[0]
    let ep = IPEndPoint(ip, port)
    let! ch = connect config clientId ep
    return ch }




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

  let concatProduceResponses (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toProduce
    |> (fun rs -> 
      let topics = rs |> Array.collect (fun r -> r.topics)
      let tt = rs |> Seq.map (fun r -> r.throttleTime) |> Seq.max
      new ProduceResponse(topics, tt) |> ResponseMessage.ProduceResponse)

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
    bootstrapHost : Host * Port
    nodeToHost : Map<NodeId, Host * Port>
    topicToNode : Map<TopicName * Partition, NodeId>
    groupToHost : Map<GroupId, Host * Port>
  } with
  
    static member ofBootstrap (h:Host, p:Port) =
      {
        bootstrapHost = (h,p)
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
            |> Map.addMany (metadata.brokers |> Seq.map (fun b -> b.nodeId, (b.host, b.port)))
        
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
        rt with groupToHost = rt.groupToHost |> Map.add gid (host,port)
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
  type RouteResult = Result<(RequestMessage * (Host * Port))[], MissingRouteResult>
        
  /// Indicates a missing route.
  and MissingRouteResult =
    | MissingTopicRoute of topic:TopicName
    | MissingGroupRoute of group:GroupId

  /// Performs request routing based on cluster metadata.
  /// Fetch, produce and offset requests are routed to the broker which is the leader for that topic, partition.
  /// Group related requests are routed to the respective broker.
  let route (routes:Routes) : RequestMessage -> RouteResult =

    // route to bootstrap broker
    let bootstrapRoute req = Success [| req, routes.bootstrapHost |]

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
      
      | ErrorCode.NotLeaderForPartition | ErrorCode.UnknownTopicOrPartition
        Some (RetryAction.RefreshMetadataAndRetry [||])

      | ErrorCode.NotCoordinatorForGroupCode | ErrorCode.IllegalGenerationCode -> 
        Some (RetryAction.PassThru) // escalate to consumer group logic.
      
      | _ ->
        Some (RetryAction.Escalate)

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

  requestTimeout : TimeSpan

} with

  /// Creates a Kafka configuration object given the specified list of broker hosts to bootstrap with.
  /// The first host to which a successful connection is established is used for a subsequent metadata request
  /// to build a routing table mapping topics and partitions to brokers.
  static member ofBootstrapServers (bootstrapServers:Uri list, ?clientId:ClientId, ?tcpConfig, ?requestTimeout) =
    { bootstrapServers = bootstrapServers
      clientId = match clientId with Some clientId -> clientId | None -> Guid.NewGuid().ToString("N")
      tcpConfig = defaultArg tcpConfig (Chan.Config.create())
      requestTimeout = defaultArg requestTimeout (TimeSpan.FromMilliseconds 5000.0)  }


/// Connection state.
type ConnState = {  
  cfg : KafkaConnCfg
  channels : Map<Host * Port, Chan>
  routes : Routing.Routes
} with
  
  static member tryFindChanByHost (h:Host,p:Port) (s:ConnState) =
    s.channels |> Map.tryFind (h,p)

  static member updateChannels (f:Map<Host * Port, Chan> -> Map<Host * Port, Chan>) (s:ConnState) =
    {
      s with channels = f s.channels
    }

  static member addChannel ((h:Host, p:Port), ch:Chan) (s:ConnState) =
    ConnState.updateChannels (Map.add (h,p) ch) s

  static member updateRoutes (f:Routing.Routes -> Routing.Routes) (s:ConnState) =
    {
      s with routes = f s.routes
    }

  static member ofBootstrap (cfg:KafkaConnCfg, bootstrapHost:Host, bootstrapPort:Port) =
    {
      cfg = cfg
      channels = Map.empty
      routes = Routing.Routes.ofBootstrap (bootstrapHost,bootstrapPort)
    }



type ResponseError =
  | FetchError of FetchResponse
  | ProduceError of ProduceResponse


type ChanResult = Result<ResponseMessage, ResponseError>



exception EscalationException of errorCode:ErrorCode * res:ResponseMessage



/// A connection to a Kafka cluster.
/// This is a stateful object which maintains request/reply sessions with brokers.
/// It acts as a context for API operations, providing filtering and fault tolerance.
type KafkaConn internal (cfg:KafkaConnCfg) =

  static let Log = Log.create "Kafunk.Conn"

  let stateCell : Cell<ConnState> = Cell.create ()
  let cts = new CancellationTokenSource()

  let connHost (cfg:KafkaConnCfg) (h:Host, p:Port) =
    Chan.connectHost cfg.tcpConfig cfg.clientId (h,p)

  /// Connects to the first available broker in the bootstrap list and returns the 
  /// initial routing table.
  let rec bootstrap (cfg:KafkaConnCfg) = async {
    // TODO: serialize
    Log.info "connecting_to_bootstrap_brokers|client_id=%s" cfg.clientId
    return!
      stateCell
      |> Cell.putAsync (async {
        let! state =
          cfg.bootstrapServers
          |> AsyncSeq.ofSeq
          |> AsyncSeq.tryPickAsync (fun uri -> async {
            try
              let! ch = connHost cfg (uri.Host, uri.Port)
              let state = 
                ConnState.ofBootstrap (cfg,uri.Host,uri.Port)
                |> ConnState.addChannel ((uri.Host,uri.Port),ch)            
              return Some state
            with ex ->
              Log.error "errored_connecting_to_bootstrap_host|host=%s port=%i error=%O" uri.Host uri.Port ex
              return None })     
        match state with
        | Some state ->
          return state
        | None ->
          return failwith "unable to connect to bootstrap brokers" } ) }

  /// Discovers cluster metadata.
  and getMetadata (topics:TopicName[]) = async {
    // TODO: TTL check
    return!
      stateCell
      |> Cell.updateAsync (fun state -> async {
        //Log.info "getting_metadata|topics=%s" (String.concat ", " topics)
        let! metadata = Chan.metadata send (Metadata.Request(topics))   
        //Log.info "received_metadata|%s" (MetadataResponse.Print metadata)
        return 
          state 
          |> ConnState.updateRoutes (Routing.Routes.addMetadata metadata) }) }

  /// Discovers a coordinator for the group.
  and getGroupCoordinator (groupId:GroupId) = async {          
    return!
      stateCell
      |> Cell.updateAsync (fun state -> async {        
        let! group = Chan.groupCoordinator send (GroupCoordinatorRequest(groupId))
        return 
          state
          |> ConnState.updateRoutes (Routing.Routes.addGroupCoordinator (groupId,group.coordinatorHost,group.coordinatorPort)) }) }

  /// Sends the request based on discovered routes.
  and send (req:RequestMessage) = async {
    let state = Cell.getFastUnsafe stateCell
    match Routing.route state.routes req with
    | Success reqRoutes ->      
      //Log.trace "request_routed|routes=%A" reqRoutes
      
      let sendHost (req:RequestMessage, host:(Host * Port)) = async {        
        match state |> ConnState.tryFindChanByHost host with
        | Some ch -> 
          return!
            req
            |> Chan.send ch
            |> Async.bind (fun res -> async {
              match RetryAction.tryFindError res with
              | None -> return res
              | Some (errorCode,action,msg) ->   
                Log.error "response_errored|error_code=%i retry_action=%A message=%s res=%A" errorCode action msg res
                match action with
                | RetryAction.PassThru ->
                  return res
                | RetryAction.Escalate ->
                  return raise (EscalationException (errorCode,res))
                | RetryAction.RefreshGroupCoordinator gid ->
                  let! _ = getGroupCoordinator gid
                  return! send req
                | RetryAction.RefreshMetadataAndRetry topics ->
                  let! _ = getMetadata topics
                  return! send req
                | RetryAction.WaitAndRetry ->
                  do! Async.Sleep 5000
                  return! send req })
        | None ->
          let! _ =
            stateCell
            |> Cell.updateAsync (fun state -> async {
              match state |> ConnState.tryFindChanByHost host with
              | Some _ -> return state
              | None ->
                Log.info "creating_channel|host=%A" host
                let! ch = connHost cfg host    
                return state |> ConnState.addChannel (host,ch) })
          return! send req }
      
      let scatterGather (gather:ResponseMessage[] -> ResponseMessage) = async {
        if reqRoutes.Length = 1 then
          return! sendHost reqRoutes.[0]
        else
          return!
            reqRoutes
            |> Seq.map sendHost
            |> Async.Parallel
            |> Async.map gather }        
 
      match req with
      | RequestMessage.Offset _ -> 
        return! scatterGather Routing.concatOffsetResponses
      | RequestMessage.Fetch _ ->
        return! scatterGather Routing.concatFetchRes
      | RequestMessage.Produce _ ->
        return! scatterGather Routing.concatProduceResponses
      | _ -> 
        return! sendHost reqRoutes.[0]

    | Failure (Routing.MissingTopicRoute topic) ->
      Log.warn "incorrect_topic_partition_route, refreshing_metadata|topic=%s request=%A" topic req
      let! _ = getMetadata [|topic|]      
      return! send req

    | Failure (Routing.MissingGroupRoute group) ->      
      Log.warn "incorrect_group_goordinator_route, getting_group_goordinator|group=%s" group
      let! _ = getGroupCoordinator group
      return! send req

    }
    
  /// Gets the cancellation token triggered when the connection is closed.
  member internal __.CancellationToken = cts.Token

  member internal __.Chan : Chan = send
  
  /// Connects to a broker from the bootstrap list.
  member internal __.Connect () = async {
    let! _ = bootstrap cfg
    return () }

  member internal __.GetGroupCoordinator (groupId:GroupId) = async {
    return! getGroupCoordinator groupId }

  member internal __.GetMetadata (topics:TopicName[]) = async {
    let! state = getMetadata topics
    return 
      state.routes 
      |> Routing.Routes.topicPartitions }

  member internal __.GetState () =
    stateCell |> Cell.get

  member __.Close () =
    Log.info "closing_connection"
    //cts.CancelAfter cfg.requestTimeout
    //cts.Dispose()
    cts.Cancel()
    (stateCell :> IDisposable).Dispose()
    

  interface IDisposable with
    member __.Dispose () =
      __.Close ()


/// Kafka API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Kafka =

  let Log = Log.create "Kafunk"
  
  let connAsync (cfg:KafkaConnCfg) = async {
    let conn = new KafkaConn(cfg)
    do! conn.Connect ()
    return conn }

  let conn cfg =
    connAsync cfg |> Async.RunSynchronously

  let connHostAsync (host:string) =
    let uri = KafkaUri.parse host
    let cfg = KafkaConnCfg.ofBootstrapServers [uri]
    connAsync cfg

  let connHost host =
    connHostAsync host |> Async.RunSynchronously

  let metadata (c:KafkaConn) (req:Metadata.Request) : Async<MetadataResponse> =
    Chan.metadata c.Chan req

  let fetch (c:KafkaConn) (req:FetchRequest) : Async<FetchResponse> =
    Chan.fetch c.Chan req

  let produce (c:KafkaConn) (req:ProduceRequest) : Async<ProduceResponse> =
    let chan = c.Chan
    Chan.produce chan req

  let offset (c:KafkaConn) (req:OffsetRequest) : Async<OffsetResponse> =
    Chan.offset c.Chan req

  let groupCoordinator (c:KafkaConn) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> =
    Chan.groupCoordinator c.Chan req

  let offsetCommit (c:KafkaConn) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> =
    Chan.offsetCommit c.Chan req

  let offsetFetch (c:KafkaConn) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    Chan.offsetFetch c.Chan req

  let joinGroup (c:KafkaConn) (req:JoinGroup.Request) : Async<JoinGroup.Response> =
    Chan.joinGroup c.Chan req

  let syncGroup (c:KafkaConn) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    Chan.syncGroup c.Chan req

  let heartbeat (c:KafkaConn) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    Chan.heartbeat c.Chan req

  let leaveGroup (c:KafkaConn) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    Chan.leaveGroup c.Chan req

  let listGroups (c:KafkaConn) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    Chan.listGroups c.Chan req

  let describeGroups (c:KafkaConn) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    Chan.describeGroups c.Chan req

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