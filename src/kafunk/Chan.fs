#nowarn "40"
namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks

open Kafunk

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


// -------------------------------------------------------------------------------------------------------------------------------------





// -------------------------------------------------------------------------------------------------------------------------------------

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
    static member internal toMetadata res = match res with MetadataResponse x -> x | _ -> wrongResponse ()      

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

  type ProduceRequest with
    static member Print (x:ProduceRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,mss,_) -> sprintf "partition=%i message_set_size=%i" p mss)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "ProduceRequest|required_acks=%i timeout=%i topics=[%s]" x.requiredAcks x.timeout ts

  type ProduceResponse with
    static member Print (x:ProduceResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec,o) -> sprintf "partition=%i offset=%i error_code=%i" p o ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "ProduceResponse|topics=[%s]" ts

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
            |> Seq.map (fun (p,ec,hwmo,mss,ms) ->
              let offsetInfo = ms.messages |> Seq.tryItem 0 |> Option.map (fun (o,_,_) -> sprintf " offset=%i lag=%i" o (hwmo - o)) |> Option.getOr ""
              sprintf "(partition=%i error_code=%i high_watermark_offset=%i message_set_size=%i%s)" p ec hwmo mss offsetInfo)
            |> String.concat ";"
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "FetchResponse|%s" ts

  type OffsetRequest with
    static member Print (x:OffsetRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,t,_) -> sprintf "partition=%i time=%i" p t)
            |> String.concat " ; "
          sprintf "topic=%s partitions=%s" tn ps)
        |> String.concat " ; "
      sprintf "OffsetRequest|topics=%s" ts

  type OffsetResponse with
    static member Print (x:OffsetResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun po -> 
              sprintf "partition=%i error_code=%i offsets=[%s]" 
                po.partition 
                po.errorCode 
                (po.offsets |> Seq.map (sprintf "offset=%i") |> String.concat " ; "))
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetResponse|topics=%s" ts

  type OffsetFetchRequest with
    static member Print (x:OffsetFetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun p -> sprintf "partition=%i" p)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
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
      | RequestMessage.Produce x -> ProduceRequest.Print x
      | RequestMessage.OffsetCommit x -> OffsetCommitRequest.Print x
      | RequestMessage.OffsetFetch x -> OffsetFetchRequest.Print x
      | RequestMessage.Offset x -> OffsetRequest.Print x
      | RequestMessage.Heartbeat x -> HeartbeatRequest.Print x
      | _ ->  sprintf "%A" x

  type ResponseMessage with
    static member Print (x:ResponseMessage) =
      match x with
      | ResponseMessage.MetadataResponse x -> MetadataResponse.Print x
      | ResponseMessage.FetchResponse x -> FetchResponse.Print x
      | ResponseMessage.ProduceResponse x -> ProduceResponse.Print x
      | ResponseMessage.OffsetCommitResponse x -> OffsetCommitResponse.Print x
      | ResponseMessage.OffsetFetchResponse x -> OffsetFetchResponse.Print x
      | ResponseMessage.OffsetResponse x -> OffsetResponse.Print x
      | ResponseMessage.HeartbeatResponse x -> HeartbeatResponse.Print x
      | x -> sprintf "%A" x

  // ------------------------------------------------------------------------------------------------------------------------------


/// A request/reply channel to a Kafka broker.
type Chan = 
  private 
  | Chan of (RequestMessage -> Async<ResponseMessage>)

/// API operations on a generic request/reply channel.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Chan =

  let private Log = Log.create "Kafunk.Chan"

  let send (Chan(send)) req = send req

  let metadata = AsyncFunc.dimap RequestMessage.Metadata ResponseMessage.toMetadata
  let fetch = AsyncFunc.dimap RequestMessage.Fetch ResponseMessage.toFetch
  let produce = AsyncFunc.dimap RequestMessage.Produce ResponseMessage.toProduce
  let offset = AsyncFunc.dimap RequestMessage.Offset ResponseMessage.toOffset
  let groupCoordinator = AsyncFunc.dimap RequestMessage.GroupCoordinator ResponseMessage.toGroupCoordinator
  let offsetCommit = AsyncFunc.dimap RequestMessage.OffsetCommit ResponseMessage.toOffsetCommit
  let offsetFetch = AsyncFunc.dimap RequestMessage.OffsetFetch ResponseMessage.toOffsetFetch
  let joinGroup = AsyncFunc.dimap RequestMessage.JoinGroup ResponseMessage.toJoinGroup
  let syncGroup = AsyncFunc.dimap RequestMessage.SyncGroup ResponseMessage.toSyncGroup
  let heartbeat = AsyncFunc.dimap RequestMessage.Heartbeat ResponseMessage.toHeartbeat
  let leaveGroup = AsyncFunc.dimap RequestMessage.LeaveGroup ResponseMessage.toLeaveGroup
  let listGroups = AsyncFunc.dimap RequestMessage.ListGroups ResponseMessage.toListGroups
  let describeGroups = AsyncFunc.dimap RequestMessage.DescribeGroups ResponseMessage.toDescribeGroups

  /// Configuration for an individual TCP channel.
  type Config = {
    
    useNagle : bool
    
    receiveBufferSize : int
    
    sendBufferSize : int
        
    connectTimeout : TimeSpan

    connectBackoff : Backoff

    requestTimeout : TimeSpan
    
    requestBackoff : Backoff

  } with
    
    static member create (?useNagle, ?receiveBufferSize, ?sendBufferSize, ?connectTimeout, ?connectBackoff, ?requestTimeout, ?requestBackoff) =
      {
        useNagle = defaultArg useNagle false
        receiveBufferSize = defaultArg receiveBufferSize 8192
        sendBufferSize = defaultArg sendBufferSize 8192
        connectTimeout = defaultArg connectTimeout (TimeSpan.FromSeconds 10.0)
        connectBackoff = defaultArg connectBackoff (Backoff.linear 500 50 |> Backoff.maxAttempts 1000)
        requestTimeout = defaultArg requestTimeout (TimeSpan.FromSeconds 10.0)
        requestBackoff = defaultArg requestBackoff (Backoff.linear 500 50 |> Backoff.maxAttempts 1000)
      }

  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  /// Only a single channel per endpoint is needed.
  let connect (config:Config) (clientId:ClientId) (ep:IPEndPoint) : Async<Chan> = async {

    /// Builds and connects the socket to the broker.
    let conn = async {
      let connSocket =
        new Socket(
          ep.AddressFamily,
          SocketType.Stream,
          ProtocolType.Tcp,
          NoDelay=not(config.useNagle),
          ExclusiveAddressUse=true,
          ReceiveBufferSize=config.receiveBufferSize,
          SendBufferSize=config.receiveBufferSize)
      Log.info "tcp_connecting|remote_endpoint=%O client_id=%s" ep clientId 
      let! sendRcvSocket = 
        Socket.connect connSocket ep
        |> Async.timeoutResult config.connectTimeout
        |> Async.map (Result.mapError (fun ex -> ex :> exn))
      match sendRcvSocket with
      | Success socket -> 
        Log.info "tcp_connected|remote_endpoint=%O local_endpoint=%O" socket.RemoteEndPoint socket.LocalEndPoint
        return sendRcvSocket 
      | Failure e ->
        Log.error "tcp_connection_failed|remote_endpoint=%O error=%O" ep e
        return sendRcvSocket }

    let conn =
      conn
      |> Faults.retryResultThrow id Exn.monoid config.connectBackoff

    let recovery (s:Socket, _req:obj, ex:exn) = async {
      Log.info "recovering_tcp_connection|client_id=%s remote_endpoint=%O error=%O" clientId ep ex
      tryDispose s
      // TODO: inspect error type
      return Resource.Recovery.Recreate }

    let! sendRcvSocket = 
      Resource.recoverableRecreate 
        conn
        recovery

    let! send =
      sendRcvSocket
      |> Resource.inject Socket.sendAll

    /// fault tolerant receive operation
    let! receive =
      let receive s b = 
        Socket.receive s b
        |> Async.map (fun received -> 
          if received = 0 then raise(SocketException(int SocketError.ConnectionAborted)) 
          else received)
      sendRcvSocket |> Resource.inject receive

    /// An unframed input stream.
    let inputStream =
      Socket.receiveStreamFrom config.receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send = Framing.LengthPrefix.frame >> send

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let req = Request(req.ApiVersion, correlationId, clientId, req)
      let sessionData = toArraySeg Request.size Request.write req
      sessionData, req.apiKey

    /// Decodes the session layer input and session state into a response.
    let decode (_, apiKey:ApiKey, buf:Binary.Segment) =
      ResponseMessage.readApiKey apiKey buf

    let session =
      Session.requestReply
        Session.corrId encode decode RequestMessage.awaitResponse inputStream send

    let send = 
      session.Send
      |> AsyncFunc.timeoutResult config.requestTimeout
      |> Faults.AsyncFunc.doAfterError
          (fun (_,e) -> Log.error "request_timed_out|timeout=%O error=%O" config.requestTimeout e)
      |> Faults.AsyncFunc.retryResultThrowList Exn.ofSeq config.requestBackoff
      |> AsyncFunc.doBeforeAfterExn 
          (fun a -> Log.trace "sending_request|request=%A" (RequestMessage.Print a))
          (fun (_,b) -> Log.trace "received_response|response=%A" (ResponseMessage.Print b))
          (fun (a,e) -> Log.error "request_errored|request=%A error=%O" (RequestMessage.Print a) e)

    return Chan send }

  let connectHost (config:Config) (clientId:ClientId) (host:Host, port:Port) = async {
    Log.info "discovering_dns_entries|host=%s" host
    let! ips = Dns.IPv4.getAllAsync host
    let ip = ips.[0]
    let ep = IPEndPoint(ip, port)
    let! ch = connect config clientId ep
    return ch }