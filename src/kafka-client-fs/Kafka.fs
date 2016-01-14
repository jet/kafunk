namespace KafkaFs

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.ExceptionServices

open KafkaFs


// -------------------------------------------------------------------------------------------------------------------------------------
// smart constructors

[<AutoOpen>]
module Constructors = 

  type Message with
  
    static member create (value:ArraySeg<byte>, ?key:ArraySeg<byte>, ?attrs:Attributes) =
      Message(0, 0y, (defaultArg attrs 0y), (defaultArg key (ArraySeg<_>())), value)
  
    static member ofBytes (data:ArraySeg<byte>, ?key:ArraySeg<byte>) =
      Message(0, 0y, 0y, (defaultArg  key (ArraySeg<_>())), data)

    static member ofBytes (value:byte[], ?key:byte[]) =
      let key = 
        match key with
        | Some key -> ArraySeg<_>(key, 0, key.Length)
        | None -> ArraySeg<_>()
      Message(0, 0y, 0y, key, ArraySeg<_>(value, 0, value.Length))

    static member ofString (value:string, ?key:string) =
      let value = Encoding.UTF8.GetBytes value |> ArraySeg.ofArray
      let key = 
        match key with
        | Some key -> Encoding.UTF8.GetBytes key |> ArraySeg.ofArray
        | None -> ArraySeg<_>()
      Message(0, 0y, 0y, key, value)

    static member valueString (m:Message) =
      m.value |> Encoding.UTF8.GetString

    static member keyString (m:Message) =
      if isNull m.value.Array then null
      else m.value |> Encoding.UTF8.GetString

  type MessageSet with
  
    static member ofMessage (m:Message) =
      MessageSet([| 0L, (size m), m |])
  
    static member ofMessages (ms:Message seq) =
      MessageSet(ms |> Seq.map (fun m -> 0L, size m, m) |> Seq.toArray)

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
        [| topic , [| partition, (size ms), ms |] |])

    static member ofMessageSets (topic:TopicName, ms:(Partition * MessageSet)[], ?requiredAcks:RequiredAcks, ?timeout:Protocol.Timeout) =
      ProduceRequest(
        (defaultArg requiredAcks RequiredAcks.Local), 
        (defaultArg timeout 1000), 
        [| topic , ms |> Array.map (fun (p,ms) -> p, (size ms), ms ) |])

    static member ofMessageSetTopics (ms:(TopicName * (Partition * MessageSet)[])[], ?requiredAcks:RequiredAcks, ?timeout:Protocol.Timeout) =
      ProduceRequest(
        (defaultArg requiredAcks RequiredAcks.Local), 
        (defaultArg timeout 1000), 
        ms |> Array.map (fun (t,ms) -> t , ms |> Array.map (fun (p,ms) -> p, (size ms), ms )))


  type FetchRequest with
    
    static member ofTopicPartition (topic:TopicName, partition:Partition, offset:FetchOffset, ?maxWaitTime:MaxWaitTime, ?minBytes:MinBytes, ?maxBytesPerPartition:MaxBytes) =
      FetchRequest(-1, (defaultArg maxWaitTime 0), (defaultArg minBytes 0), [| topic, [| partition,  offset, (defaultArg maxBytesPerPartition 1000) |] |])
    
    static member ofTopicPartitions (topic:TopicName, ps:(Partition * FetchOffset)[], ?maxWaitTime:MaxWaitTime, ?minBytes:MinBytes, ?maxBytesPerPartition:MaxBytes) =
      FetchRequest(-1, (defaultArg maxWaitTime 0), (defaultArg minBytes 0), [| topic, ps |> Array.map (fun (p,o) -> p, o, (defaultArg maxBytesPerPartition 1000)) |])


// -------------------------------------------------------------------------------------------------------------------------------------









// -------------------------------------------------------------------------------------------------------------------------------------
// Connection

module internal Conn =

  let private Log = Log.create "Marvel.Kafka.Conn"

  let ApiVersion : ApiVersion = 0s

  /// Establishes a fault-tolerance connection to the specified endpoint.
  let connect (ep:IPEndPoint, clientId:ClientId) = async {

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let req = Request(ApiVersion, correlationId, clientId, req)  
      let sessionData = toArraySeg req 
      sessionData,req.apiKey
      
    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, data:ArraySeg<byte>) =
      let res = ResponseMessage.readApiKey (data, apiKey)
      res   


    let connSocket =
      let s = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
      //s.ReceiveBufferSize <- 8192
      //s.SendBufferSize <- 8192
      s.NoDelay <- true
      s.ExclusiveAddressUse <- true
      s

    Log.info "connecting...|client_id=%s" clientId
    let! sendRcvSocket = Socket.connect connSocket ep
    Log.info "connected|remote_endpoint=%O" sendRcvSocket.RemoteEndPoint     



    /// An unframed input stream.  
    let inputStream =
      Socket.receiveStream sendRcvSocket
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send (data:ArraySeg<byte>) =
      let framed = data |> Framing.LengthPrefix.frame
      Socket.sendAll sendRcvSocket framed

    
       
    /// A framing sender.
    let rec send2 (data:ArraySeg<byte>) =
      let framed = data |> Framing.LengthPrefix.frame
      Socket.sendAll sendRcvSocket framed
      |> Async.Catch
      |> Async.bind (function
        | Success x -> async.Return x
        | Failure ex ->
          match ex with
          | :? SocketException as x -> 
            // TODO: reco
            send2 data
          | _ -> raise ex)


    let session = 
      Session.requestReply
        Session.corrId encode decode inputStream send

    return session }
  




[<AutoOpen>]
module internal ResponseEx =

  let wrongResponse () =
    failwith (sprintf "Wrong response!")

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
  
  let inline metadata (send:RequestMessage -> Async<ResponseMessage>) (req:MetadataRequest) =
    send (RequestMessage.Metadata req) |> Async.map (function MetadataResponse x -> x | _ -> wrongResponse ())

  let inline fetch (send:RequestMessage -> Async<ResponseMessage>) (req:FetchRequest) : Async<FetchResponse> = 
    send (RequestMessage.Fetch req) |> Async.map ResponseMessage.toFetch

  let inline produce (send:RequestMessage -> Async<ResponseMessage>) (req:ProduceRequest) : Async<ProduceResponse> = 
    send (RequestMessage.Produce req) |> Async.map ResponseMessage.toProduce

  let inline offset (send:RequestMessage -> Async<ResponseMessage>) (req:OffsetRequest) : Async<OffsetResponse> = 
    send (RequestMessage.Offset req) |> Async.map ResponseMessage.toOffset

  let inline groupCoordinator (send:RequestMessage -> Async<ResponseMessage>) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> = 
    send (RequestMessage.GroupCoordinator req) |> Async.map ResponseMessage.toGroupCoordinator

  let inline offsetCommit (send:RequestMessage -> Async<ResponseMessage>) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> = 
    send (RequestMessage.OffsetCommit req) |> Async.map ResponseMessage.toOffsetCommit

  let inline offsetFetch (send:RequestMessage -> Async<ResponseMessage>) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> = 
    send (RequestMessage.OffsetFetch req) |> Async.map ResponseMessage.toOffsetFetch

  let inline joinGroup (send:RequestMessage -> Async<ResponseMessage>) (req:JoinGroupRequest) : Async<JoinGroupResponse> = 
    send (RequestMessage.JoinGroup req) |> Async.map ResponseMessage.toJoinGroup

  let inline syncGroup (send:RequestMessage -> Async<ResponseMessage>) (req:SyncGroupRequest) : Async<SyncGroupResponse> = 
    send (RequestMessage.SyncGroup req) |> Async.map ResponseMessage.toSyncGroup

  let inline heartbeat (send:RequestMessage -> Async<ResponseMessage>) (req:HeartbeatRequest) : Async<HeartbeatResponse> = 
    send (RequestMessage.Heartbeat req) |> Async.map ResponseMessage.toHeartbeat

  let inline leaveGroup (send:RequestMessage -> Async<ResponseMessage>) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> = 
    send (RequestMessage.LeaveGroup req) |> Async.map ResponseMessage.toLeaveGroup

  let inline listGroups (send:RequestMessage -> Async<ResponseMessage>) (req:ListGroupsRequest) : Async<ListGroupsResponse> = 
    send (RequestMessage.ListGroups req) |> Async.map ResponseMessage.toListGroups

  let inline describeGroups (send:RequestMessage -> Async<ResponseMessage>) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> = 
    send (RequestMessage.DescribeGroups req) |> Async.map ResponseMessage.toDescribeGroups


/// Request routing to brokers.
module internal Route =

  let private hostEndpoint (host:string, port:int) =
    let ipv4 = Dns.GetHostAddresses host |> Seq.find (fun ip -> ip.AddressFamily = AddressFamily.InterNetwork)
    IPEndPoint(ipv4, port)
  
  let private concatFetchResponses (rs:FetchResponse[]) =
    new FetchResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.FetchResponse

  let private concatProduceResponses (rs:ProduceResponse[]) =
    new ProduceResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.ProduceResponse

  let private concatOffsetResponses (rs:OffsetResponse[]) =
    new OffsetResponse(rs |> Array.collect (fun r -> r.topics)) |> ResponseMessage.OffsetResponse

  /// Performs request routing based on cluster metadata.
  /// Fetch, produce and offset requests are routed to the broker which is the leader for that topic, partition.
  /// Group related requests are routed to the respective broker.
  let route (metadata:MetadataResponse) (conn:IPEndPoint -> (RequestMessage -> Async<ResponseMessage>)) =    
    
    let bootstrap : RequestMessage -> Async<ResponseMessage> =
      failwith ""

    let brokers = 
      metadata.brokers 
      |> Seq.toKeyValueMap (fun b -> b.nodeId) (fun b -> hostEndpoint (b.host, b.port) |> conn)
    
    let topicPartitions =
      metadata.topicMetadata
      |> Seq.collect (fun tmd ->
        tmd.partitionMetadata
        |> Seq.map (fun pmd -> (tmd.topicName, pmd.partitionId), (brokers |> Map.find pmd.leader))
      )
      |> dict

    let groupBrokers : IDictionary<GroupId, (RequestMessage -> Async<ResponseMessage>)> = 
      failwith ""

    fun (req:RequestMessage) -> async {
      match req with
      | Metadata _ ->
        return! bootstrap req

      | Fetch req ->
        return!
          req.topics
          |> Seq.collect (fun (tn,ps) -> ps |> Array.map (fun (p,o,mb) -> tn,p,o,mb))
          |> Seq.groupBy (fun (tn,ps,_,_) ->  tn,ps)
          |> Seq.map (fun (tp,reqs) ->
            match topicPartitions |> Dict.tryGet tp with
            | Some send -> 
              let topics = 
                reqs 
                |> Seq.groupBy (fun (t,_,_,_) -> t)
                |> Seq.map (fun (t,(ps)) -> t, ps |> Seq.map (fun (_,p,o,mb) -> p,o,mb) |> Seq.toArray)
                |> Seq.toArray
              let req = new FetchRequest(req.replicaId, req.maxWaitTime, req.minBytes, topics)
              send (RequestMessage.Fetch req)
            | None -> 
              failwith ""
          )
          |> Async.Parallel
          |> Async.map (Array.map ResponseMessage.toFetch >> concatFetchResponses)

      | Produce req ->
        return!
          req.topics
          |> Seq.collect (fun (t,ps) -> ps |> Array.map (fun (p,mss,ms) -> t,p,mss,ms))
          |> Seq.groupBy (fun (t,p,_,_) -> t,p)
          |> Seq.map (fun (tp,reqs) ->
            match topicPartitions |> Dict.tryGet tp with
            | Some send ->
              let topics =
                reqs
                |> Seq.groupBy (fun (t,_,_,_) -> t)
                |> Seq.map (fun (t,ps) -> t, ps |> Seq.map (fun (_,p,mss,ms) -> p,mss,ms) |> Seq.toArray)
                |> Seq.toArray
              let req = new ProduceRequest(req.requiredAcks, req.timeout, topics)
              send (RequestMessage.Produce req)
            | None ->
              failwith "")
          |> Async.Parallel
          |> Async.map (Array.map ResponseMessage.toProduce >> concatProduceResponses)
             
      | Offset req ->
        return!
          req.topics
          |> Seq.collect (fun (t,ps) -> ps |> Array.map (fun (p,tm,mo) -> t,p,tm,mo))
          |> Seq.groupBy (fun (t,p,_,_) -> t,p)
          |> Seq.map (fun (tp,reqs) ->
            match topicPartitions |> Dict.tryGet tp with
            | Some send ->
              let topics =
                reqs
                |> Seq.groupBy (fun (t,_,_,_) -> t)
                |> Seq.map (fun (t,ps) -> t, ps |> Seq.map (fun (_,p,mss,ms) -> p,mss,ms) |> Seq.toArray)
                |> Seq.toArray
              let req = new OffsetRequest(req.replicaId, topics)
              send (RequestMessage.Offset req)
            | None ->
              failwith "")
          |> Async.Parallel
          |> Async.map (Array.map ResponseMessage.toOffset >> concatOffsetResponses)
      
      | GroupCoordinator _ ->
        return! bootstrap req

      | OffsetCommit r ->
        match groupBrokers |> Dict.tryGet r.consumerGroup with
        | Some send -> return! send req
        | None -> return failwith ""

      | OffsetFetch r ->
        match groupBrokers |> Dict.tryGet r.consumerGroup with
        | Some send -> return! send req
        | None -> return failwith ""

      | JoinGroup r ->
        match groupBrokers |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""

      | SyncGroup r ->
        match groupBrokers |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""
      
      | Heartbeat r ->
        match groupBrokers |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""
       
      | LeaveGroup r ->
        match groupBrokers |> Dict.tryGet r.groupId with
        | Some send -> return! send req
        | None -> return failwith ""
       
      | DescribeGroups req ->
        return failwith ""
      
      | ListGroups req ->
        return failwith ""       
           
    }

  

// http://kafka.apache.org/documentation.html#connectconfigs

/// Kafka connection configuration.
type KafkaConnCfg = {
  
  /// The bootstrap brokers to attempt connection to.
  bootstrapServers : Uri list
  
  /// The client id.
  clientId : ClientId

//  /// A filter to apply to request/reply interactions.
//  filter : AsyncFilter<RequestMessage, ResponseMessage> option
//  
//  /// Receive buffer size on the socket.
//  /// This is the size of the chunk in which TCP will try to receive data in.
//  socketReceiveBufferSize : int option
//  
//  /// Send buffer size on the socket.
//  /// This is the size of the chunk in which TCP will try to transmit data in.
//  socketSendBufferSize : int option
//    
//  /// The maximum number of concurrent request/reply sessions.
//  maxSessions : int
//  
//  /// A possible handler to call when a connection is closed.
//  onConnectionClosed : (KafkaConnCfg -> Async<KafkaConn>) option

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
and KafkaConn internal (reqRepSession:ReqRepSession<_,_,_>) =
    
  static let ApiVersion : ApiVersion = 0s

  static let Log = Log.create "Marvel.Kafka"

  member internal x.Send (req:RequestMessage) : Async<ResponseMessage> =
    reqRepSession.Send req

  static member internal connect (ep:IPEndPoint) = async {

    let clientId : ClientId = Guid.NewGuid().ToString("N")

    let connSocket =
      let s = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
      //s.ReceiveBufferSize <- 8192
      //s.SendBufferSize <- 8192
      s.NoDelay <- true
      s.ExclusiveAddressUse <- true
      s

    Log.info "connecting...|client_id=%s" clientId
    let! sendRcvSocket = Socket.connect connSocket ep
    Log.info "connected|remote_endpoint=%O" sendRcvSocket.RemoteEndPoint     

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let req = Request(ApiVersion, correlationId, clientId, req)  
      let sessionData = toArraySeg req 
      sessionData,req.apiKey
      
    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, data:ArraySeg<byte>) =
      let res = ResponseMessage.readApiKey (data, apiKey)
      res   

    /// An unframed input stream.  
    let inputStream =
      Socket.receiveStream sendRcvSocket
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send (data:ArraySeg<byte>) =
      let framed = data |> Framing.LengthPrefix.frame
      Socket.sendAll sendRcvSocket framed

    let session = 
      Session.requestReply
        Session.corrId encode decode inputStream send

    return KafkaConn(session) }

// -------------------------------------------------------------------------------------------------------------------------------------




/// Kafka API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Kafka =
    
  let [<Literal>] DefaultPort = 9092

  let connHostAsync (host:string) =
    let ipv4 = Dns.GetHostAddresses host |> Seq.find (fun ip -> ip.AddressFamily = AddressFamily.InterNetwork)
    let ep = IPEndPoint(ipv4, DefaultPort)
    KafkaConn.connect ep

  let connHost host = 
    connHostAsync host |> Async.RunSynchronously

  let metadata (c:KafkaConn) (req:MetadataRequest) : Async<MetadataResponse> = 
    Api.metadata c.Send req

  let fetch (c:KafkaConn) (req:FetchRequest) : Async<FetchResponse> = 
    Api.fetch c.Send req

  let produce (c:KafkaConn) (req:ProduceRequest) : Async<ProduceResponse> = 
    Api.produce c.Send req

  let offset (c:KafkaConn) (req:OffsetRequest) : Async<OffsetResponse> = 
    Api.offset c.Send req

  let groupCoordinator (c:KafkaConn) (req:GroupCoordinatorRequest) : Async<GroupCoordinatorResponse> = 
    Api.groupCoordinator c.Send req

  let offsetCommit (c:KafkaConn) (req:OffsetCommitRequest) : Async<OffsetCommitResponse> = 
    Api.offsetCommit c.Send req

  let offsetFetch (c:KafkaConn) (req:OffsetFetchRequest) : Async<OffsetFetchResponse> =
    Api.offsetFetch c.Send req

  let joinGroup (c:KafkaConn) (req:JoinGroupRequest) : Async<JoinGroupResponse> = 
    Api.joinGroup c.Send req

  let syncGroup (c:KafkaConn) (req:SyncGroupRequest) : Async<SyncGroupResponse> =
    Api.syncGroup c.Send req

  let heartbeat (c:KafkaConn) (req:HeartbeatRequest) : Async<HeartbeatResponse> =
    Api.heartbeat c.Send req

  let leaveGroup (c:KafkaConn) (req:LeaveGroupRequest) : Async<LeaveGroupResponse> =
    Api.leaveGroup c.Send req

  let listGroups (c:KafkaConn) (req:ListGroupsRequest) : Async<ListGroupsResponse> =
    Api.listGroups c.Send req

  let describeGroups (c:KafkaConn) (req:DescribeGroupsRequest) : Async<DescribeGroupsResponse> =
    Api.describeGroups c.Send req



  // -------------------------------------------------------------------------------------------------------------------------
  // consumer groups

  type ConsumerConfig = {
    bootstrapServers : Uri[] // kafka://127.0.0.1:9092
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
      static member create (bootstrapServers:Uri[], groupId:GroupId, topics:TopicName[]) =
        {
          ConsumerConfig.bootstrapServers = bootstrapServers
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



//  type ConsumerState =
//    | Down
//    | Discover of GroupId
//    | GroupFollower of generationId:GenerationId * LeaderId * MemberId * GroupProtocol
//    | GroupLeader of generationId:GenerationId * LeaderId * MemberId * Members * GroupProtocol
//    | PartOfGroup
//    | RediscoverCoordinator
//    | StoppedConsumption
//    | Error



  
  /// Domain-specific message set.
  type ConsumerEvent =
            
    | GroopCoordResponse
    | JoinResponse
    | SyncResponse
    | FetchResponse

    /// GroupLoadInProgressCode	14	Yes	The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition), or in response to group membership requests (such as heartbeats) when group metadata is being loaded by the coordinator.
    | GroupLoadInProgress

    /// GroupCoordinatorNotAvailableCode	15	Yes	The broker returns this error code for group coordinator requests, offset commits, and most group management requests if the offsets topic has not yet been created, or if the group coordinator is not active.
    | GroupCoordinatorNotAvailable
    
    /// IllegalGenerationCode	22	 	Returned from group membership requests (such as heartbeats) when the generation id provided in the request is not the current generation.
    | IllegalGeneration
    
    /// InconsistentGroupProtocolCode	23	 	Returned in join group when the member provides a protocol type or set of protocols which is not compatible with the current group.
    | InconsistentGroupProtocol
    
    | InvalidGroupId    

    /// UnknownMemberIdCode	25	 	Returned from group requests (offset commits/fetches, heartbeats, etc) when the memberId is not in the current generation.
    | UnknownMemberId
    
    /// InvalidSessionTimeoutCode	26	 	Return in join group when the requested session timeout is outside of the allowed range on the broker
    | InvalidSessionTimeout
    
    /// RebalanceInProgressCode	27	 	Returned in heartbeat requests when the coordinator has begun rebalancing the group. This indicates to the client that it should rejoin the group.
    | RebalanceInProgress
        
    | SessionTimeout 

    | MetadataChanged

    /// 16	Yes	The broker returns this error code if it receives an offset fetch or commit request for a group that it is not a coordinator for.
    | NotCoordinatorForGroup
  


//  module AsyncSeq =
//    
//    let singletonAsync (a:Async<'a>) : AsyncSeq<'a> =
//      asyncSeq {
//        let! a = a
//        yield a }
//
//    let repeatAsync (a:Async<'a>) : AsyncSeq<'a> =
//      asyncSeq {
//        while true do
//          let! a = a
//          yield a }
//
//    let mergeChoice (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) : AsyncSeq<Choice<'a, 'b>> =
//      AsyncSeq.merge (s1 |> AsyncSeq.map Choice1Of2) (s2 |> AsyncSeq.map Choice2Of2)
      


  /// Given a consumer configuration, initiates the consumer group protocol.
  /// Returns an async sequence of states where each state corresponds to a generation in the group protocol.
  /// The state contains streams for the topics specified in the configuration.
  /// Whenever there is a change in the consumer group, or a failure, the protocol restarts and returns
  /// a new generation once successful.
  /// If there are failures surpassing configured thresholds, the resulting sequence throws an exception.
  let consume (conn:KafkaConn) (cfg:ConsumerConfig) : AsyncSeq<_> = async {
    
    // domain-specific api

    let groopCoord = 
      groupCoordinator conn (GroupCoordinatorRequest(cfg.groupId))
      |> Async.map (fun res ->
        match res.errorCode with
        | ErrorCode.NoError -> ConsumerEvent.GroopCoordResponse
        | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerEvent.GroupCoordinatorNotAvailable
        | ErrorCode.InvalidGroupIdCode -> ConsumerEvent.InvalidGroupId          
        | _ -> failwith "")
    
    // sent to group coordinator
    let joinGroup2 =
      let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, ArraySeg<_>())
      let assignmentStrategy : AssignmentStrategy = "range" //roundrobin
      let groupProtocols = GroupProtocols([| assignmentStrategy, (toArraySeg consumerProtocolMeta) |])
      let joinGroupReq = JoinGroupRequest(cfg.groupId, cfg.sessionTimeout, "" (* memberId *), ProtocolType.consumer, groupProtocols)
      joinGroup conn joinGroupReq
      |> Async.map (fun res ->
        match res.errorCode with
        | ErrorCode.NoError -> 
          //if res.members.members.Length > 0 then            
          ConsumerEvent.JoinResponse
        | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerEvent.GroupCoordinatorNotAvailable
        | ErrorCode.InconsistentGroupProtocolCode -> ConsumerEvent.InconsistentGroupProtocol
        | ErrorCode.InvalidSessionTimeoutCode -> ConsumerEvent.InvalidSessionTimeout
        | _ -> failwith "")

    // heartbeats: must be sent to group coordinator
    let rec hb (generationId,memberId) = async {
      let req = HeartbeatRequest(cfg.groupId, generationId, memberId)
      let! res = heartbeat conn req
      match res.errorCode with
      | ErrorCode.NoError -> 
        do! Async.Sleep (cfg.sessionTimeout / cfg.heartbeatFrequency)
        return! hb (generationId,memberId)
      | ErrorCode.IllegalGenerationCode -> 
        return ConsumerEvent.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode -> 
        return ConsumerEvent.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode -> 
        return ConsumerEvent.RebalanceInProgress
      | _ -> 
        return ConsumerEvent.SessionTimeout }
      
    // sent to group coordinator
    let leaderSyncGroup (generationId,memberId,members) = async {
      let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([||]))
      let members = [| "" (*memberId*), (toArraySeg assignment) |]
      let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment(members))
      let! res = syncGroup conn req
      match res.errorCode with
      | ErrorCode.NoError -> return ConsumerEvent.SyncResponse
      | ErrorCode.IllegalGenerationCode -> return ConsumerEvent.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode -> return ConsumerEvent.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode -> return ConsumerEvent.RebalanceInProgress
      | _ -> 
        return ConsumerEvent.SessionTimeout }

    // sent to group coordinator
    let followerSyncGroup (generationId,memberId) = async {
      let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([||]))
      let! res = syncGroup conn req
      match res.errorCode with
      | ErrorCode.NoError -> return ConsumerEvent.SyncResponse
      | ErrorCode.IllegalGenerationCode -> return ConsumerEvent.IllegalGeneration
      | ErrorCode.UnknownMemberIdCode -> return ConsumerEvent.UnknownMemberId
      | ErrorCode.RebalanceInProgressCode -> return ConsumerEvent.RebalanceInProgress
      | _ -> 
        return ConsumerEvent.SessionTimeout }


    // sent to group coordinator
    let commitOffset (generationId,memberId) (topic:TopicName, partition:Partition, offset:Offset) = async {        
      let req = OffsetCommitRequest(cfg.groupId, generationId, memberId, cfg.offsetRetentionTime, [| topic, [| partition, offset, null |] |])
      let! res = offsetCommit conn req
      // TODO: check error
      return () }

    let fetchOffset (topic:TopicName, partition:Partition) = async {
      let req = OffsetFetchRequest(cfg.groupId, [| topic, [| partition |] |])
      let! res = offsetFetch conn req
      let topic,ps = res.topics.[0]
      let (p,offset,metadata,ec) = ps.[0]
      return offset }

    // fetch sent to broker in metadata or coordinator?
    let stream (generationId,memberId) (topic:TopicName, partition:Partition) =
      let rec go (offset:FetchOffset) = asyncSeq {
        let req = FetchRequest(-1, cfg.fetchMaxWaitMs, cfg.fetchMinBytes, [| topic, [| partition, offset, cfg.fetchBuffer |] |])
        // TODO: wait for state change (kill) signal
        let! res = fetch conn req
        // TODO: check error
        let topic,partitions = res.topics.[0]
        let partition,ec,hmo,mss,ms = partitions.[0]
        let nextOffset = MessageSet.nextOffset ms
        let commit = commitOffset (generationId,memberId) (topic, partition, offset)
        yield ms,commit          
        yield! go nextOffset }
      // TODO: fetch offset
      go (0L)

    // TODO: period and watch for changes?
    let! metadata = metadata conn (MetadataRequest(cfg.topics))


    let rec go () : AsyncSeq<_> = async {

      // start of session
      let! groupCoord = groupCoordinator conn (GroupCoordinatorRequest(cfg.groupId))    
      // TODO: send commit/fetch requests to groop coord

      


      let consumerProtocolMeta = ConsumerGroupProtocolMetadata(0s, cfg.topics, ArraySeg<_>())
      let assignmentStrategy : AssignmentStrategy = "range" //roundrobin
      let groupProtocols = GroupProtocols([| assignmentStrategy, (toArraySeg consumerProtocolMeta) |])



      let memberId : MemberId = "" // assigned by coordinator
      let joinGroupReq = JoinGroupRequest(cfg.groupId, cfg.sessionTimeout, memberId, ProtocolType.consumer, groupProtocols)
      let! joinGroupRes = joinGroup conn joinGroupReq
      // TODO: or failure
      let generationId = joinGroupRes.generationId
      let memberId = joinGroupRes.memberId

      // is leader?
      if (joinGroupRes.leaderId = joinGroupRes.memberId) then
        // determine assignments
        // send sync request
        let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([||]))
        let syncReq = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([| "" (*memberId*), (toArraySeg assignment) |]))
        let! syncRes = syncGroup conn syncReq

        

        // TODO: get metadata?
                              
        return failwith ""
      else
            
        let syncReq = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([||]))
        let! syncRes = syncGroup conn syncReq
        let (memberAssignment:ConsumerGroupMemberAssignment,_) = read syncRes.memberAssignment       
      
        // the partitions assigned to this member
        let topicPartitions = memberAssignment.partitionAssignment.assignments


        // the topic,partition,stream combinations assigned to this member      
        let topicStreams =           
          topicPartitions
          |> Array.collect (fun (t,ps) -> ps |> Array.map (fun p -> t,p))
          |> Array.map (fun (t,p) -> t,p, stream (generationId,memberId) (t,p))           


        // return and wait for errors, which will stop all child streams
        // and return a new state

        return Cons ( (generationId,memberId,topicStreams), go ())
      
      }


    return! go ()

  }


    

// -------------------------------------------------------------------------------------------------------------------------------------



