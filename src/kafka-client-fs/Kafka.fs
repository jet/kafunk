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


/// A request/reply channel to Kafka.
type Chan = RequestMessage -> Async<ResponseMessage>

 
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





/// Request routing to brokers.
module internal Route =
  
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








module internal Conn =

  let private Log = Log.create "KafkaFunk.Conn"

  let ApiVersion : ApiVersion = 0s

  
  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  let rec connect (ep:IPEndPoint, clientId:ClientId) : Async<Chan> = async {
   
    let receiveBufferSize = 8192

    /// Builds and connects the socket.
    let conn () = async {
      use connSocket =
        new Socket(
          ep.AddressFamily, 
          SocketType.Stream, 
          ProtocolType.Tcp, 
          NoDelay=true, 
          ExclusiveAddressUse=true)
      Log.info "connecting...|remote_endpoint=%O" ep
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
      

    // --------

    /// Notify of an error and recovery.
    /// If a recovery is in progress, wait for it to complete and return.
    let reset () = async {
      // TODO: cleanup
      // if first, then recover
      if Monitor.TryEnter sendRcvSocket then
        try
          let! sendRcvSocket' = conn ()
          DVar.put sendRcvSocket' sendRcvSocket
        finally
          Monitor.Exit sendRcvSocket
      // if not first, wait for lock to be released, then assume recovery and proceed.
      else
        return lock sendRcvSocket id }
    

    let rec sendErr buf =
      send buf
      |> Async.Catch
      |> Async.bind (function
        | Success n -> async.Return n
        | Failure ex -> async {
          Log.error "error sending on socket|error=%O" ex                                
          do! reset ()
          return! sendErr buf })

    let rec receiveErr buf =
      receive buf
      |> Async.Catch
      |> Async.bind (function
        | Success received when received > 0 -> async.Return received
        | Success _ -> async {
          Log.warn "received 0 bytes. recovering..."
          do! reset ()
          return! receiveErr buf }
        | Failure ex -> async {
          Log.warn "error receiving on socket|error=%O" ex
          do! reset ()
          return! receiveErr buf })

    let send,receive = sendErr,receiveErr

    // --------

    

    /// An unframed input stream.  
    let inputStream =
      Socket.receiveStreamFrom receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send (data:ArraySeg<byte>) =
      let framed = data |> Framing.LengthPrefix.frame
      send framed


    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let req = Request(ApiVersion, correlationId, clientId, req)  
      let sessionData = toArraySeg req 
      sessionData,req.apiKey
      
    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, data:ArraySeg<byte>) =
      let res = ResponseMessage.readApiKey (data, apiKey)
      res   

    let session = 
      Session.requestReply
        Session.corrId encode decode inputStream send

    return session.Send }


  let anyCast (chs:Chan[]) : Chan =
    let i = ref 0
    let rec go req = async {
      let ch = chs.[!i]
      try
        return! ch req
      with ex ->
        Log.error "connection failure, trying another channel..."
        Interlocked.Increment i |> ignore
        return! go req }
    go



  

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
and KafkaConn internal (cfg:KafkaConnCfg, bootstrapChan:Chan) =
  
  static let Log = Log.create "KafkaFunc.Conn"    

  // routing tables

  // TODO: add bootstrap
  let chanByHost : DVar<Map<Host * Port, Chan>> =
    failwith ""
 
  
  let hostByNode : DVar<Map<NodeId, Host * Port>> =
    failwith ""

  let nodeByTopic : DVar<Map<TopicName * Partition, NodeId>> = 
    failwith ""

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



  let hostByGroup : DVar<Map<GroupId, Host * Port>> =
    failwith ""

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
  
  
  let routedChan = 
    DVar.combineLatestWith 
      (Route.route bootstrapChan)
      chanByTopic
      chanByGroupId
    |> DVar.toFun
          
    
  /// Connects to a Kafka cluster.
  static member internal connect (cfg:KafkaConnCfg) = async {
   
    let clientId : ClientId = Guid.NewGuid().ToString("N")
    Log.info "connecting...|client_id=%s" clientId

    Log.info "resolving bootstrap broker IP endpoints..."
    let! bootstrapIPs = 
      cfg.bootstrapServers
      |> Seq.map (fun uri -> Dns.IPv4.getEndpointAsync (uri.Host, uri.Port))
      |> Async.Parallel
    
    Log.info "creating fault-tolerant communication channels to bootstrap brokers..."      
    let! chs =
      bootstrapIPs
      |> Seq.map (fun ip -> Conn.connect (ip, clientId))
      |> Async.Parallel
      
    let ch = Conn.anyCast chs
  
    return KafkaConn(cfg, ch) }

  /// Gets the configuration used to create this connection.
  member __.Cfg = 
    cfg
  
  /// Gets the channel.
  member internal __.Chan : Chan = 
    routedChan

  /// Gets metadata from the bootstrap channel and updates internal routing tables.
  member internal __.GetMetadata (topics:TopicName[]) = async {
    let! metadata = Api.metadata bootstrapChan (MetadataRequest(topics))
    //nodeByHost |> DVar.update (Map.addMany (metadata.brokers |> Seq.map (fun b -> (b.host,b.port),b.nodeId)))    
    return () }
 
  member internal __.MetadataForTopic (t:TopicName) : TopicMetadata =
    failwith ""

  /// Gets the group coordinator for the specified group.
  /// Adds the group coordinator to the routing table, such that subsequent group requests
  /// are routed to the correct broker.
  member internal __.GetGroupCoordinator (groupId:GroupId) = async {    
    let! res = Api.groupCoordinator bootstrapChan (GroupCoordinatorRequest(groupId))
    
    hostByGroup |> DVar.update (Map.add groupId (res.coordinatorHost,res.coordinatorPort))
    //nodeByHost |> DVar.update (Map.add (res.coordinatorHost, res.coordinatorPort) res.coordinatorId)
    return () }


// -------------------------------------------------------------------------------------------------------------------------------------




/// Kafka API.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Kafka =
    
  let [<Literal>] DefaultPort = 9092

  let connAsync (cfg:KafkaConnCfg) =
    KafkaConn.connect cfg

  let conn cfg =
    connAsync cfg |> Async.RunSynchronously

  let connHostAsync (host:string) =
    let cfg = KafkaConnCfg.ofBootstrapServers ([Uri(host + ":" + string DefaultPort)])
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

  // -------------------------------------------------------------------------------------------------------------------------


  


  /// A producer message.
  type ProducerMessage =
    struct

      /// The message payload.
      val value : ArraySeg<byte>
      
      /// The optional message key.
      val key : ArraySeg<byte>

      /// The optional routing key.
      val routeKey : ProducerMessageRouteKey
                 
      new (value:ArraySeg<byte>, key:ArraySeg<byte>, routeKey:string) = 
        { value = value ; key = key ; routeKey = routeKey }
    end      
      with
        
        /// Creates a producer message.
        static member ofBytes (value:ArraySeg<byte>, ?key, ?routeKey) =
          ProducerMessage (value, defaultArg key (ArraySeg<_>()), defaultArg routeKey null)
      
        static member ofBytes (value:byte[], ?key, ?routeKey) =
          ProducerMessage (ArraySeg.ofArray value, defaultArg (key |> Option.map (ArraySeg.ofArray)) (ArraySeg<_>()), defaultArg routeKey null)

  /// A key used for routing to a partition.
  and ProducerMessageRouteKey = string

  /// A producer sends batches of topic and message set pairs to the appropriate Kafka brokers.
  type Producer = (TopicName * ProducerMessage[])[] -> Async<ProduceResponse>
  
  /// High-level producer API.
  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Producer =
    
    /// Producer configuration.
    type ProducerCfg = {
      
      /// The set of topics to produce to.
      /// Produce requests must be for a topic in this list.
      topics : TopicName[]
      
      /// The acks required.
      requiredAcks : RequiredAcks
      
      /// The compression method to use.
      compression : byte

      /// The maximum time to wait for acknowledgement.
      timeout : Timeout

      /// A partition function which given a topic name, cluster topic metadata and the message payload, returns the partition
      /// which the message should be written to.
      partition : TopicName * TopicMetadata[] * ProducerMessageRouteKey -> Partition

    }

    /// Creates a producer.
    let createAsync (conn:KafkaConn) (cfg:ProducerCfg) : Async<Producer> = async {
    
      do! conn.GetMetadata (cfg.topics)

      let! metadata = metadata conn (MetadataRequest(cfg.topics))
      
      /// (T * P) -> N
      let nodeByTopicPartition : Map<(TopicName * Partition), NodeId> =
        metadata.topicMetadata
        |> Seq.collect (fun tmd ->
          tmd.partitionMetadata
          |> Seq.map (fun pmd -> (tmd.topicName, pmd.partitionId), pmd.leader))
        |> Map.ofSeq

      /// N -> Chan
      let! (chanByBroker : Map<NodeId, Chan>) = 
        metadata.brokers 
        |> Seq.map (fun b -> async {
          let! ep = Dns.IPv4.getEndpointAsync (b.host, b.port)
          let! ch = Conn.connect (ep, conn.Cfg.clientId) // TODO: re-use connection...
          return b.nodeId,ch })
        |> Async.Parallel
        |> Async.map (Map.ofArray)
      


      let produce (ms:(TopicName * ProducerMessage[])[]) = async {
        return!
          ms
          |> Seq.collect (fun (tn,ms) ->
            ms
            |> Seq.map (fun m -> 
              let p = cfg.partition (tn,metadata.topicMetadata,m.routeKey)
              (tn,p),m))
          |> Seq.groupBy (fun ((tn,p),_) ->
            match nodeByTopicPartition |> Map.tryFind (tn,p) with
            | Some node -> node
            | None -> failwithf "unable to find broker for topic=%s partition=%i" tn p)
          |> Seq.map (fun (nodeId,ms) -> async {
            let ms = 
              ms
              |> Seq.groupBy (fun ((m,_),_) -> m)
              |> Seq.map (fun (tn,ms) -> 
                let ps =
                  ms
                  |> Seq.groupBy (fun ((_,p),_) -> p)
                  |> Seq.map (fun (p,ms) -> p, ms |> Seq.map (fun (_,m) -> Message.create (m.value, m.key)) |> MessageSet.ofMessages)
                  |> Seq.toArray
                tn,ps)
              |> Seq.toArray
            let req = ProduceRequest.ofMessageSetTopics (ms, cfg.requiredAcks, cfg.timeout) // TODO: compression
            match chanByBroker |> Map.tryFind nodeId with
            | Some ch -> return! Api.produce ch req
            | None -> return failwithf "unable to find channel for broker=%i" nodeId })
          |> Async.Parallel
          |> Async.map (fun rs -> new ProduceResponse(rs |> Array.collect (fun r -> r.topics))) }

      return produce }
     

  // -------------------------------------------------------------------------------------------------------------------------




  /// High-level consumer API.
  module Consumer =
    
    type ConsumerConfig = {
      //bootstrapServers : Uri[] // kafka://127.0.0.1:9092
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
        static member create (groupId:GroupId, topics:TopicName[]) =
          {
            //ConsumerConfig.bootstrapServers = bootstrapServers
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


  (*

  # Consumer Group Transitions


  ## Consumer

  - Coordinator heartbeat timeout -> close channel, then group coordinator re-discovery.


  ## Coordinator

  - Invalid generation heartbeat -> invalid generation heartbeat response


  *)


  
    /// Possible responses of a consumer group protocol.
    /// These may be received by several concurrent processes: heartbeating, fetching, committing.
    type ConsumerGroupResponse =
            
      | GroopCoordResponse // start protocol, or retry
      | JoinResponse // 
      | SyncResponse
      | FetchResponse

      // TODO: Choice<OK, Error> ?
      // TODO: on error, running consumers must be stopped (by closing stream).
      // Cleanup:
      // - stop heartbeating
      // - close fetched streams
      // - 

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
          | ErrorCode.NoError -> ConsumerGroupResponse.GroopCoordResponse
          | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerGroupResponse.GroupCoordinatorNotAvailable
          | ErrorCode.InvalidGroupIdCode -> ConsumerGroupResponse.InvalidGroupId          
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
            ConsumerGroupResponse.JoinResponse
          | ErrorCode.GroupCoordinatorNotAvailableCode -> ConsumerGroupResponse.GroupCoordinatorNotAvailable
          | ErrorCode.InconsistentGroupProtocolCode -> ConsumerGroupResponse.InconsistentGroupProtocol
          | ErrorCode.InvalidSessionTimeoutCode -> ConsumerGroupResponse.InvalidSessionTimeout
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
          return ConsumerGroupResponse.IllegalGeneration
        | ErrorCode.UnknownMemberIdCode -> 
          return ConsumerGroupResponse.UnknownMemberId
        | ErrorCode.RebalanceInProgressCode -> 
          return ConsumerGroupResponse.RebalanceInProgress
        | _ -> 
          return ConsumerGroupResponse.SessionTimeout }
      
      // sent to group coordinator
      let leaderSyncGroup (generationId,memberId,members) = async {
        let assignment = ConsumerGroupMemberAssignment(0s, PartitionAssignment([||]))
        let members = [| "" (*memberId*), (toArraySeg assignment) |]
        let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment(members))
        let! res = syncGroup conn req
        match res.errorCode with
        | ErrorCode.NoError -> return ConsumerGroupResponse.SyncResponse
        | ErrorCode.IllegalGenerationCode -> return ConsumerGroupResponse.IllegalGeneration
        | ErrorCode.UnknownMemberIdCode -> return ConsumerGroupResponse.UnknownMemberId
        | ErrorCode.RebalanceInProgressCode -> return ConsumerGroupResponse.RebalanceInProgress
        | _ -> 
          return ConsumerGroupResponse.SessionTimeout }

      // sent to group coordinator
      let followerSyncGroup (generationId,memberId) = async {
        let req = SyncGroupRequest(cfg.groupId, generationId, memberId, GroupAssignment([||]))
        let! res = syncGroup conn req
        match res.errorCode with
        | ErrorCode.NoError -> return ConsumerGroupResponse.SyncResponse
        | ErrorCode.IllegalGenerationCode -> return ConsumerGroupResponse.IllegalGeneration
        | ErrorCode.UnknownMemberIdCode -> return ConsumerGroupResponse.UnknownMemberId
        | ErrorCode.RebalanceInProgressCode -> return ConsumerGroupResponse.RebalanceInProgress
        | _ -> 
          return ConsumerGroupResponse.SessionTimeout }


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
        // TODO: send offset commit/fetch requests to groop coord

      


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



