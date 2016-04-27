namespace KafkaFs

open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading

open KafkaFs
open KafkaFs.Prelude


// -------------------------------------------------------------------------------------------------------------------------------------
// smart constructors

[<AutoOpen>]
module Constructors =

  type Message with

    static member create (value:Buffer, ?key:Buffer, ?attrs:Attributes) =
      Message(0, 0y, (defaultArg attrs 0y), (defaultArg key (Buffer.empty)), value)

    static member ofBytes (data:Buffer, ?key:Buffer) =
      Message(0, 0y, 0y, (defaultArg  key (Buffer.empty)), data)

    static member ofBytes (value:byte[], ?key:byte[]) =
      let key =
        match key with
        | Some key -> Buffer.ofArray(key)
        | None -> Buffer.empty
      Message(0, 0y, 0y, key, Buffer.ofArray(value))

    static member ofString (value:string, ?key:string) =
      let value = Encoding.UTF8.GetBytes value |> Buffer.ofArray
      let key =
        match key with
        | Some key -> Encoding.UTF8.GetBytes key |> Buffer.ofArray
        | None -> Buffer.empty
      Message(0, 0y, 0y, key, value)

    static member valueString (m:Message) =
      m.value |> Buffer.toString

    static member keyString (m:Message) =
      if isNull m.value.Array then null
      else m.value |> Buffer.toString

  type MessageSet with

    static member ofMessage (m:Message) =
      MessageSet([| 0L, (Message.size m), m |])

    static member ofMessages (ms:Message seq) =
      MessageSet(ms |> Seq.map (fun m -> 0L, Message.size m, m) |> Seq.toArray)

    /// Returns the next offset to fetch, by taking the max offset in the
    /// message set and adding one.
    static member nextOffset (ms:MessageSet) =
      let maxOffset = ms.messages |> Seq.map (fun (off,_,_) -> off) |> Seq.max
      maxOffset + 1L

  type ProduceRequest with

    static member ofMessageSet (topic:TopicName, partition:Partition, ms:MessageSet, ?requiredAcks:RequiredAcks, ?timeout:Protocol.Timeout) =
      ProduceRequest(
        (defaultArg requiredAcks RequiredAcks.Local),
        (defaultArg timeout 1000),
        [| topic , [| partition, (MessageSet.size ms), ms |] |])

    static member ofMessageSets (topic:TopicName, ms:(Partition * MessageSet)[], ?requiredAcks:RequiredAcks, ?timeout:Protocol.Timeout) =
      ProduceRequest(
        (defaultArg requiredAcks RequiredAcks.Local),
        (defaultArg timeout 1000),
        [| topic , ms |> Array.map (fun (p,ms) -> p, (MessageSet.size ms), ms ) |])

    static member ofMessageSetTopics (ms:(TopicName * (Partition * MessageSet)[])[], ?requiredAcks:RequiredAcks, ?timeout:Protocol.Timeout) =
      ProduceRequest(
        (defaultArg requiredAcks RequiredAcks.Local),
        (defaultArg timeout 1000),
        ms |> Array.map (fun (t,ms) -> t , ms |> Array.map (fun (p,ms) -> p, (MessageSet.size ms), ms )))


  type FetchRequest with

    static member ofTopicPartition (topic:TopicName, partition:Partition, offset:FetchOffset, ?maxWaitTime:MaxWaitTime, ?minBytes:MinBytes, ?maxBytesPerPartition:MaxBytes) =
      FetchRequest(-1, (defaultArg maxWaitTime 0), (defaultArg minBytes 0), [| topic, [| partition,  offset, (defaultArg maxBytesPerPartition 1000) |] |])

    static member ofTopicPartitions (topic:TopicName, ps:(Partition * FetchOffset)[], ?maxWaitTime:MaxWaitTime, ?minBytes:MinBytes, ?maxBytesPerPartition:MaxBytes) =
      FetchRequest(-1, (defaultArg maxWaitTime 0), (defaultArg minBytes 0), [| topic, ps |> Array.map (fun (p,o) -> p, o, (defaultArg maxBytesPerPartition 1000)) |])

// Connection


/// A request/reply channel to Kafka.
// TODO: likely needs to become IDisposable, but we'll see how far we can put that off
type Chan = RequestMessage -> Async<ResponseMessage>


[<AutoOpen>]
module internal ResponseEx =

  let wrongResponse () =
    failwith (sprintf "Wrong response!")

  type RequestMessage with
    /// If a request does not expect a response, returns the default response.
    static member awaitResponse (x:RequestMessage) =
      match x with
      | RequestMessage.Produce req when req.requiredAcks = RequiredAcks.None ->
        Some (ResponseMessage.ProduceResponse (new ProduceResponse([||])))
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


/// API operations on a generic request/reply channel.
module internal Api =

  let inline metadata (send:Chan) (req:MetadataRequest) =
    send (RequestMessage.Metadata req) |> Async.map (function MetadataResponse x -> x | _ -> wrongResponse ())

  let inline fetch (send:Chan) (req:FetchRequest) : Async<FetchResponse> =
    send (RequestMessage.Fetch req) |> Async.map ResponseMessage.toFetch

  let inline produce (send:Chan) (req:ProduceRequest) : Async<ProduceResponse> =
    send (RequestMessage.Produce req) |> Async.map ResponseMessage.toProduce

  let inline offset (send:Chan) (req:OffsetRequest) : Async<OffsetResponse> =
    send (RequestMessage.Offset req) |> Async.map ResponseMessage.toOffset

  let inline groupCoordinator (send:Chan) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> =
    send (RequestMessage.GroupCoordinator req) |> Async.map ResponseMessage.toGroupCoordinator

  let inline offsetCommit (send:Chan) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> =
    send (RequestMessage.OffsetCommit req) |> Async.map ResponseMessage.toOffsetCommit

  let inline offsetFetch (send:Chan) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    send (RequestMessage.OffsetFetch req) |> Async.map ResponseMessage.toOffsetFetch

  let inline joinGroup (send:Chan) (req:JoinGroupRequest) : Async<JoinGroupResponse> =
    send (RequestMessage.JoinGroup req) |> Async.map ResponseMessage.toJoinGroup

  let inline syncGroup (send:Chan) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    send (RequestMessage.SyncGroup req) |> Async.map ResponseMessage.toSyncGroup

  let inline heartbeat (send:Chan) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    send (RequestMessage.Heartbeat req) |> Async.map ResponseMessage.toHeartbeat

  let inline leaveGroup (send:Chan) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    send (RequestMessage.LeaveGroup req) |> Async.map ResponseMessage.toLeaveGroup

  let inline listGroups (send:Chan) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    send (RequestMessage.ListGroups req) |> Async.map ResponseMessage.toListGroups

  let inline describeGroups (send:Chan) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    send (RequestMessage.DescribeGroups req) |> Async.map ResponseMessage.toDescribeGroups

module internal Conn =

  let private Log = Log.create "KafkaFunk.Conn"

  let ApiVersion : ApiVersion = 0s

  /// Partitions a fetch request by topic/partition and wraps each one in a request.
  let partitionFetchReq (req:FetchRequest) =
    req.topics
    |> Seq.collect (fun (tn,ps) -> ps |> Array.map (fun (p,o,mb) -> tn,p,o,mb))
    |> Seq.groupBy (fun (tn,ps,_,_) ->  tn,ps)
    |> Seq.map (fun (tp,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t,_,_,_) -> t)
        |> Seq.map (fun (t,(ps)) -> t, ps |> Seq.map (fun (_,p,o,mb) -> p,o,mb) |> Seq.toArray)
        |> Seq.toArray
      let req = new FetchRequest(req.replicaId, req.maxWaitTime, req.minBytes, topics)
      tp,RequestMessage.Fetch req)
    |> Seq.toArray

  /// Unwraps a set of responses as fetch responses and joins them into a single response.
  let concatFetchRes (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toFetch
    |> (fun rs -> new FetchResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.FetchResponse)

  /// Partitions a produce request by topic/partition.
  let partitionProduceReq (req:ProduceRequest) =
    req.topics
    |> Seq.collect (fun (t,ps) -> ps |> Array.map (fun (p,mss,ms) -> t,p,mss,ms))
    |> Seq.groupBy (fun (t,p,_,_) -> t,p)
    |> Seq.map (fun (tp,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t,_,_,_) -> t)
        |> Seq.map (fun (t,ps) -> t, ps |> Seq.map (fun (_,p,mss,ms) -> p,mss,ms) |> Seq.toArray)
        |> Seq.toArray
      let req = new ProduceRequest(req.requiredAcks, req.timeout, topics)
      tp,RequestMessage.Produce req)
    |> Seq.toArray

  let concatProduceResponses (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toProduce
    |> (fun rs -> new ProduceResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.ProduceResponse)

  let concatProduceResponsesMs (rs:ProduceResponse[]) =
    new ProduceResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.ProduceResponse

  let partitionOffsetReq (req:OffsetRequest) =
    req.topics
    |> Seq.collect (fun (t,ps) -> ps |> Array.map (fun (p,tm,mo) -> t,p,tm,mo))
    |> Seq.groupBy (fun (t,p,_,_) -> t,p)
    |> Seq.map (fun (tp,reqs) ->
      let topics =
        reqs
        |> Seq.groupBy (fun (t,_,_,_) -> t)
        |> Seq.map (fun (t,ps) -> t, ps |> Seq.map (fun (_,p,mss,ms) -> p,mss,ms) |> Seq.toArray)
        |> Seq.toArray
      let req = new OffsetRequest(req.replicaId, topics)
      tp,RequestMessage.Offset req)
    |> Seq.toArray

  let concatOffsetResponses (rs:ResponseMessage[]) =
    rs
    |> Array.map ResponseMessage.toOffset
    |> (fun rs -> new OffsetResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.OffsetResponse)


  /// Performs request routing based on cluster metadata.
  /// Fetch, produce and offset requests are routed to the broker which is the leader for that topic, partition.
  /// Group related requests are routed to the respective broker.
  let route (bootstrapChan:Chan) (byTopicPartition:Map<(TopicName * Partition), Chan>) (byGroupId:Map<GroupId, Chan>) : Chan =
    // TODO: optimize single topic/partition case
    fun (req:RequestMessage) -> async {
      match req with
      | Metadata _ ->
        return! bootstrapChan req

      | Fetch req ->
        return!
          req
          |> partitionFetchReq
          |> Seq.map (fun (tp,req) ->
            match byTopicPartition |> Dict.tryGet tp with
            | Some send -> send req
            | None -> failwith "Unable to find route!")
          |> Async.Parallel
          |> Async.map concatFetchRes

      | Produce req ->
        return!
          req
          |> partitionProduceReq
          |> Seq.map (fun (tp,req) ->
            match byTopicPartition |> Dict.tryGet tp with
            | Some send -> send req
            | None -> failwith "")
          |> Async.Parallel
          |> Async.map (concatProduceResponses)

      | Offset req ->
        return!
          req
          |> partitionOffsetReq
          |> Seq.map (fun (tp,req) ->
            match byTopicPartition |> Dict.tryGet tp with
            | Some send -> send req
            | None -> failwith "")
          |> Async.Parallel
          |> Async.map (concatOffsetResponses)

      | GroupCoordinator _ ->
        return! bootstrapChan req

      | OffsetCommit r ->
        match byGroupId |> Dict.tryGet r.consumerGroup with
        | Some send -> return! send req
        | None -> return failwith ""

      | OffsetFetch r ->
        match byGroupId |> Dict.tryGet r.consumerGroup with
        | Some send -> return! send req
        | None -> return failwith ""

      | JoinGroup r ->
        match byGroupId |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""

      | SyncGroup r ->
        match byGroupId |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""

      | Heartbeat r ->
        match byGroupId |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""

      | LeaveGroup r ->
        match byGroupId |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""

      | DescribeGroups req ->
        // TODO
        return failwith ""

      | ListGroups req ->
        // TODO
        return failwith "" }

  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  let rec connect (ep:IPEndPoint, clientId:ClientId) : Async<Chan> = async {

    let receiveBufferSize = 8192

    /// Builds and connects the socket.
    let conn () = async {
      // TODO: lifecycle
      let connSocket =
        new Socket(
          ep.AddressFamily,
          SocketType.Stream,
          ProtocolType.Tcp,
          NoDelay=true,
          ExclusiveAddressUse=true)
      Log.info "connecting...|client_id=%s remote_endpoint=%O" clientId ep
      let! sendRcvSocket = Socket.connect connSocket ep
      Log.info "connected|remote_endpoint=%O" sendRcvSocket.RemoteEndPoint
      return sendRcvSocket }

    let! sendRcvSocket = conn ()
    let sendRcvSocket = DVar.create sendRcvSocket

    let receive =
      sendRcvSocket
      |> DVar.mapFun Socket.receive

    let send =
      sendRcvSocket
      |> DVar.mapFun Socket.sendAll

    // TODO: implement using channels
    let state = ref 0 // 0 - OK, 1 - recovery in progress
    let wh = new ManualResetEventSlim()

    /// Notify of an error and recovery.
    /// If a recovery is in progress, wait for it to complete and return.
    let reset (ex:exn option) = async {
      Log.info "recovering TCP connection|client_id=%s remote_endpoint=%O" clientId ep
      if Interlocked.CompareExchange(state, 1, 0) = 0 then
        wh.Reset()
        let! sendRcvSocket' = conn ()
        DVar.put sendRcvSocket' sendRcvSocket
        wh.Set()
        Interlocked.Exchange(state, 0) |> ignore
      else
        Log.info "recovery alread in progress, waiting...|client_id=%s remote_endpoint=%O" clientId ep
        wh.Wait()
        return () }

    // TODO: handle specific TCP errors only, otherwise escalate to reconnect

    let rec sendErr buf =
      send buf
      |> Async.Catch
      |> Async.bind (function
        | Success n -> async.Return n
        | Failure ex -> async {
          match ex with
          | :? SocketException as x ->
            Log.error "socket exception|error=%O" ex
            do! reset (Some ex)
            return! sendErr buf
          | _ ->
            Log.error "exception=%O" ex
            return raise ex })

    let rec receiveErr buf =
      receive buf
      |> Async.Catch
      |> Async.bind (function
        | Success received when received > 0 -> async.Return received
        | Success _ -> async {
          Log.warn "received 0 bytes indicating a closed TCP connection"
          do! reset None
          return! receiveErr buf }
        | Failure ex -> async {
          match ex with
          | :? SocketException as x ->
            Log.warn "error receiving on socket|error=%O" ex
            do! reset (Some ex)
            return! receiveErr buf
          | _ ->
            Log.error "exception=%O" ex
            return raise ex })

    let send,receive = sendErr,receiveErr

    /// An unframed input stream.
    let inputStream =
      Socket.receiveStreamFrom receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send (data:Buffer) =
      let framed = data |> Framing.LengthPrefix.frame
      send framed


    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let req = Request(ApiVersion, correlationId, clientId, req)
      let sessionData = toArraySeg Request.size Request.write req
      sessionData,req.apiKey

    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, data:Buffer) =
      let res = ResponseMessage.readApiKey (data, apiKey)
      res

    let session =
      Session.requestReply
        Session.corrId encode decode RequestMessage.awaitResponse inputStream send

    return session.Send }


// http://kafka.apache.org/documentation.html#connectconfigs

/// Kafka connection configuration.
type KafkaConnCfg = {

  /// The bootstrap brokers to attempt connection to.
  bootstrapServers : Uri list

  /// The client id.
  clientId : ClientId

}
with

  /// Creates a Kafka configuration object given the specified list of broker hosts to bootstrap with.
  /// The first host to which a successful connection is established is used for a subsequent metadata request
  /// to build a routing table mapping topics and partitions to brokers.
  static member ofBootstrapServers (bootstrapServers:Uri list, ?clientId:ClientId) =
    {
      bootstrapServers = bootstrapServers
      clientId = match clientId with Some clientId -> clientId | None -> Guid.NewGuid().ToString("N")
    }


/// A connection to a Kafka cluster.
/// This is a stateful object which maintains request/reply sessions with brokers.
/// It acts as a context for API operations, providing filtering and fault tolerance.
type KafkaConn internal (cfg:KafkaConnCfg) =

  static let Log = Log.create "KafkaFunc.Conn"

  // note: must call Connect first thing!
  let [<VolatileField>] mutable bootstrapChanField : Chan =
    Unchecked.defaultof<_>

  let bootstrapChan : Chan =
    fun req -> bootstrapChanField req

  // routing tables

  let chanByHost : DVar<Map<Host * Port, Chan>> =
    DVar.create Map.empty

  let hostByNode : DVar<Map<NodeId, Host * Port>> =
    DVar.create Map.empty

  let nodeByTopic : DVar<Map<TopicName * Partition, NodeId>> =
    DVar.create Map.empty

  let hostByGroup : DVar<Map<GroupId, Host * Port>> =
    DVar.create Map.empty

  // derived routing tables

  let hostByTopic : DVar<Map<TopicName * Partition, Host * Port>> =
    DVar.combineLatestWith
      (fun topicNodes nodeHosts ->
       topicNodes
       |> Map.toSeq
       |> Seq.choose (fun (tp,n) ->
         match nodeHosts |> Map.tryFind n with
         | Some host -> Some (tp,host)
         | None -> None)
       |> Map.ofSeq)
      nodeByTopic
      hostByNode
    |> DVar.distinct

  let chanByTopic : DVar<Map<(TopicName * Partition), Chan>> =
    DVar.combineLatestWith
      (fun topicHosts hostChans ->
        topicHosts
        |> Map.toSeq
        |> Seq.map (fun (t,h) ->
          let chan = Map.find h hostChans in
          t,chan)
        |> Map.ofSeq)
      hostByTopic
      chanByHost

  let chanByGroupId : DVar<Map<GroupId, Chan>> =
    DVar.combineLatestWith
      (fun groupHosts hostChans ->
        groupHosts
        |> Map.toSeq
        |> Seq.map (fun (g,h) ->
          let chan = Map.find h hostChans in
          g,chan)
        |> Map.ofSeq)
      hostByGroup
      chanByHost

  let routedChan : Chan =
    DVar.combineLatestWith
      (fun chanByTopic chanByGroup -> Conn.route bootstrapChan chanByTopic chanByGroup)
      chanByTopic
      chanByGroupId
    |> DVar.toFun


  /// Connects to the specified host and adds to routing table.
  let connHost (host:Host, port:Port, nodeId:NodeId option) = async {
    let! ep = Dns.IPv4.getEndpointAsync (host, port)
    let! ch = Conn.connect (ep, cfg.clientId)
    chanByHost |> DVar.update (Map.add (host,port) ch)
    nodeId |> Option.iter (fun nodeId -> hostByNode |> DVar.update (Map.add nodeId (host,port)))
    return ch }

  /// Connects to the specified host unless already connected.
  let connHostNew (host:Host, port:Port, nodeId:NodeId option) = async {
    match chanByHost |> DVar.get |> Map.tryFind (host,port) with
    | Some ch ->
      nodeId |> Option.iter (fun nodeId -> hostByNode |> DVar.update (Map.add nodeId (host,port)))
      return ch
    | None -> return! connHost (host,port,nodeId) }

  /// Connects to the first broker in the bootstrap list.
  let connectBootstrap () = async {
    Log.info "discovering bootstrap brokers...|client_id=%s" cfg.clientId
    let! bootstrapChan =
      cfg.bootstrapServers
      |> AsyncSeq.ofSeq
      |> AsyncSeq.tryPickAsync (fun uri -> async {
        //Log.info "connecting....|client_id=%s host=%s port=%i" cfg.clientId uri.Host uri.Port
        try
          let! ch = connHost (uri.Host,uri.Port,None)
          return Some ch
        with ex ->
          Log.error "error connecting to bootstrap host=%s port=%i error=%O" uri.Host uri.Port ex
          return None })
    match bootstrapChan with
    | Some bootstrapChan ->
      return bootstrapChan
    | None ->
      return failwith "unable to connect to bootstrap brokers" }

  /// Connects to the coordinator broker for the specified group and adds to routing table
  let connectGroupCoordinator (groupId:GroupId) = async {
    let! res = Api.groupCoordinator bootstrapChan (GroupCoordinatorRequest(groupId))
    let! ch = connHostNew (res.coordinatorHost,res.coordinatorPort,Some res.coordinatorId)
    hostByGroup |> DVar.updateIfDistinct (Map.add groupId (res.coordinatorHost,res.coordinatorPort)) |> ignore
    return ch }


  /// Gets the channel.
  member internal __.Chan : Chan =
    if isNull routedChan then
      invalidOp "The connection has not been established!"
    routedChan

  /// Connects to a broker from the bootstrap list.
  member internal __.Connect () = async {
    let! ch = connectBootstrap ()
    bootstrapChanField <- ch }

  /// Gets metadata from the bootstrap channel and updates internal routing tables.
  member private __.ApplyMetadata (metadata:MetadataResponse) = async {
    let hostByNode' =
      metadata.brokers
      |> Seq.map (fun b -> b.nodeId, (b.host,b.port))
      |> Map.ofSeq
    for tmd in metadata.topicMetadata do
      for pmd in tmd.partitionMetadata do
        let (host,port) = hostByNode' |> Map.find pmd.leader // TODO: handle error, but shouldn't happen
        let! _ = connHostNew (host,port, Some pmd.leader)
        nodeByTopic |> DVar.update (Map.add (tmd.topicName,pmd.partitionId) (pmd.leader)) }

  /// Gets metadata from the bootstrap channel and updates internal routing tables.
  member internal this.GetMetadata (topics:TopicName[]) = async {
    let! metadata = Api.metadata bootstrapChan (MetadataRequest(topics))
    do! this.ApplyMetadata metadata
    return metadata }

  /// Gets the group coordinator for the specified group, connects to it, adds to routing table.
  member internal __.ConnectGroupCoordinator (groupId:GroupId) =
    connectGroupCoordinator groupId

  interface IDisposable with
    member __.Dispose () = ()

/// Kafka API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Kafka =

  let [<Literal>] DefaultPort = 9092

  let connAsync (cfg:KafkaConnCfg) = async {
    let conn = new KafkaConn(cfg)
    do! conn.Connect ()
    return conn }

  let conn cfg =
    connAsync cfg |> Async.RunSynchronously

  let connHostAsync (host:string) =
    let ub = UriBuilder("kafka", host, DefaultPort)
    let cfg = KafkaConnCfg.ofBootstrapServers [ub.Uri]
    connAsync cfg

  let connHost host =
    connHostAsync host |> Async.RunSynchronously

  let metadata (c:KafkaConn) (req:MetadataRequest) : Async<MetadataResponse> =
    Api.metadata c.Chan req

  let fetch (c:KafkaConn) (req:FetchRequest) : Async<FetchResponse> =
    Api.fetch c.Chan req

  let produce (c:KafkaConn) (req:ProduceRequest) : Async<ProduceResponse> =
    Api.produce c.Chan req

  let offset (c:KafkaConn) (req:OffsetRequest) : Async<OffsetResponse> =
    Api.offset c.Chan req

  let groupCoordinator (c:KafkaConn) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> =
    Api.groupCoordinator c.Chan req

  let offsetCommit (c:KafkaConn) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> =
    Api.offsetCommit c.Chan req

  let offsetFetch (c:KafkaConn) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    Api.offsetFetch c.Chan req

  let joinGroup (c:KafkaConn) (req:JoinGroupRequest) : Async<JoinGroupResponse> =
    Api.joinGroup c.Chan req

  let syncGroup (c:KafkaConn) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    Api.syncGroup c.Chan req

  let heartbeat (c:KafkaConn) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    Api.heartbeat c.Chan req

  let leaveGroup (c:KafkaConn) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    Api.leaveGroup c.Chan req

  let listGroups (c:KafkaConn) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    Api.listGroups c.Chan req

  let describeGroups (c:KafkaConn) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    Api.describeGroups c.Chan req