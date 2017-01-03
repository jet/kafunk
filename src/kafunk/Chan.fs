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
    Message(0, 0y, (defaultArg attrs 0y), 0L, key, value)

  let ofBuffer data key =
    Message(0, 0y, 0y, 0L, (defaultArg  key (Binary.empty)), data)

  let ofBytes value key =
    let key =
      match key with
      | Some key -> Binary.ofArray key
      | None -> Binary.empty
    Message(0, 0y, 0y, 0L, key, Binary.ofArray value)

  let ofString (value:string) (key:string) =
    let value = Encoding.UTF8.GetBytes value |> Binary.ofArray
    let key = Encoding.UTF8.GetBytes key |> Binary.ofArray
    Message(0, 0y, 0y, 0L, key, value)

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

  /// Returns the frist offset in the message set.
  let firstOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      let (o,_,_) = ms.messages.[0] in o
    else
      0L

  /// Returns the last offset in the message set.
  let lastOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      //ms.messages |> Seq.map (fun (o,_,_) -> o) |> Seq.max
      let (o,_,_) = ms.messages.[ms.messages.Length - 1] in o
    else
      0L

  /// Returns the next offset to fetch, by taking the max offset in the
  /// message set and adding 1.
  /// Ensures the next offset is bellow high watermark offset.
  let nextOffset (ms:MessageSet) (hwm:HighwaterMarkOffset) : Offset =
    let lastOffset = lastOffset ms
    let nextOffset = lastOffset + 1L
    if nextOffset <= hwm then
      nextOffset
    else 
      failwithf "invalid offset computation last_offset=%i hwm=%i" lastOffset hwm

module FetchResponse =
  
  /// Returns the next set of offsets to fetch.
  let nextOffsets (res:FetchResponse) : (TopicName * (Partition * Offset)[])[] =
    res.topics
    |> Seq.map (fun (t,ps) ->
      let os =
        ps
        |> Seq.map (fun (p,_,hwmo,_,ms) ->
          let nextOffset = MessageSet.nextOffset ms hwmo
          p,nextOffset)
        |> Seq.toArray
      t,os)
    |> Seq.toArray
      



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

[<AutoOpen>]
module internal Printers =
  
  open System.Text

  let concatMapSbDo (sb:StringBuilder) (s:seq<'a>) (f:StringBuilder -> 'a -> _) (sep:string) =
    use en = s.GetEnumerator()
    if en.MoveNext () then
      f sb en.Current |> ignore
      while en.MoveNext () do
        sb.Append sep |> ignore
        f sb en.Current |> ignore

  let concatMapSb (s:seq<'a>) (f:StringBuilder -> 'a -> _) (sep:string) =
    let sb = StringBuilder()
    concatMapSbDo sb s f sep
    sb.ToString()
    
  let partitionOffsetPairs (os:seq<Partition * Offset>) =
    concatMapSb os (fun sb (p,o) -> sb.AppendFormat("[partition={0} offset={1}]", p, o)) " ; "
    
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
            |> Seq.map (fun (p,mss,ms) -> sprintf "partition=%i message_set_size=%i message_count=%i" p mss ms.messages.Length)
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

  type GroupCoordinatorResponse with
    static member Print (x:GroupCoordinatorResponse) =
      sprintf "GroupCoordinatorResponse|coordinator_id=%i host=%s port=%i error_code=%i" 
        x.coordinatorId x.coordinatorHost x.coordinatorPort x.errorCode

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
      | ResponseMessage.GroupCoordinatorResponse x -> GroupCoordinatorResponse.Print x
      | x -> sprintf "%A" x

  // ------------------------------------------------------------------------------------------------------------------------------


/// A broker endpoint.
[<CustomEquality;CustomComparison;StructuredFormatDisplay("{Display}")>]
type EndPoint = 
  private
  | EndPoint of IPEndPoint
  with
    static member endpoint (EndPoint ep) = ep
    static member parse (ipString:string, port:int) = EndPoint (IPEndPoint.parse (ipString, port))
    static member ofIPEndPoint (ep:IPEndPoint) = EndPoint ep
    static member ofIPAddressAndPort (ip:IPAddress, port:int) = EndPoint.ofIPEndPoint (IPEndPoint(ip,port))
    member this.Display = this.ToString()
    override this.Equals (o:obj) = 
      match o with 
      | :? EndPoint as ep -> (EndPoint.endpoint this).Equals(EndPoint.endpoint ep)
      | _ -> false
    override this.GetHashCode () = (EndPoint.endpoint this).GetHashCode()
    override this.ToString () = (EndPoint.endpoint this).ToString()
    interface IEquatable<EndPoint> with
      member this.Equals (other:EndPoint) =
        (EndPoint.endpoint this).Equals(EndPoint.endpoint other)
    interface IComparable with
      member this.CompareTo (other) =
        this.ToString().CompareTo(other.ToString())
      
  
/// A request/reply channel to a Kafka broker.
type Chan = 
  private 
  | Chan of ep:EndPoint * send:(RequestMessage -> Async<ResponseMessage>) * reconnect:Async<unit> * close:Async<unit>


/// Configuration for an individual TCP channel.
type ChanConfig = {
  
  /// Specifies whether the socket should use Nagle's algorithm.
  useNagle : bool
  
  /// The socket receive buffer size.
  receiveBufferSize : int
    
  /// The socket send buffer size.
  sendBufferSize : int
        
  /// The connection timeout.
  connectTimeout : TimeSpan

  /// The connection retry policy for timeouts and failures.
  connectRetryPolicy : RetryPolicy

  /// The request timeout.
  requestTimeout : TimeSpan
    
  /// The request retry polify for timeouts and failures.
  requestRetryPolicy : RetryPolicy

} with
  
  /// Creates a channel configuration.
  static member create (?useNagle, ?receiveBufferSize, ?sendBufferSize, ?connectTimeout, ?connectRetryPolicy, ?requestTimeout, ?requestRetryPolicy) =
    {
      useNagle = defaultArg useNagle false
      receiveBufferSize = defaultArg receiveBufferSize 8192
      sendBufferSize = defaultArg sendBufferSize 8192
      connectTimeout = defaultArg connectTimeout (TimeSpan.FromSeconds 10)
      connectRetryPolicy = defaultArg connectRetryPolicy (RetryPolicy.constantMs 2000 |> RetryPolicy.maxAttempts 50)
      requestTimeout = defaultArg requestTimeout (TimeSpan.FromSeconds 30)
      requestRetryPolicy = defaultArg requestRetryPolicy (RetryPolicy.constantMs 2000 |> RetryPolicy.maxAttempts 50)
    }


/// API operations on a generic request/reply channel.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Chan =

  let private Log = Log.create "Kafunk.Chan"

  /// Sends a request.
  let send (Chan(_,send,_,_)) req = send req
  
  /// Gets the endpoint.
  let endpoint (Chan(ep,_,_,_)) = ep

//  /// Re-established the connection to the socket.
//  let reconnect (Chan(_,reconnect,_)) = reconnect

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

  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  /// Only a single channel per endpoint is needed.
  let connect (version:System.Version, config:ChanConfig, clientId:ClientId) (ep:EndPoint) : Async<Chan> = async {
    
    let conn (ep:EndPoint) = async {
      let ipep = EndPoint.endpoint ep
      let connSocket =
        new Socket(
          ipep.AddressFamily,
          SocketType.Stream,
          ProtocolType.Tcp,
          NoDelay=not(config.useNagle),
          ExclusiveAddressUse=true,
          ReceiveBufferSize=config.receiveBufferSize,
          SendBufferSize=config.receiveBufferSize)
      return! Socket.connect connSocket ipep }
    
    let conn =
      conn
      |> AsyncFunc.timeoutResult config.connectTimeout
      |> AsyncFunc.catchResult
      |> AsyncFunc.doBeforeAfter
          (fun ep -> Log.info "tcp_connecting|remote_endpoint=%O client_id=%s" (EndPoint.endpoint ep) clientId)
          (fun (ep,res) ->
            let ipep = EndPoint.endpoint ep
            match res with
            | Success s ->
              Log.info "tcp_connected|remote_endpoint=%O local_endpoint=%O" s.RemoteEndPoint s.LocalEndPoint
            | Failure (Choice1Of2 _) ->
              Log.error "tcp_connection_timed_out|remote_endpoint=%O timeout=%O" ipep config.connectTimeout
            | Failure (Choice2Of2 e) ->
              Log.error "tcp_connection_failed|remote_endpoint=%O error=%O" ipep e)
      |> AsyncFunc.mapOut (snd >> Result.codiagExn)
      |> Faults.AsyncFunc.retryResultThrow id Exn.monoid config.connectRetryPolicy

    let recovery (s:Socket, ver:int, _req:obj, ex:exn) = async {
      Log.warn "recovering_tcp_connection|client_id=%s remote_endpoint=%O version=%i error=%O" clientId (EndPoint.endpoint ep) ver ex
      tryDispose s
      return Resource.Recovery.Recreate }

    let! socketAgent = 
      Resource.recoverableRecreate 
        (conn ep)
        recovery

    let! send =
      socketAgent
      |> Resource.inject Socket.sendAll

    let! receive =
      let receive s buf = async {
        let! received = Socket.receive s buf
        if received = 0 then return raise(SocketException(int SocketError.ConnectionAborted)) 
        else return received }
      socketAgent 
      |> Resource.inject receive

    /// An unframed input stream.
    let receiveStream =
      Socket.receiveStreamFrom config.receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    /// A framing sender.
    let send = Framing.LengthPrefix.frame >> send

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let apiKey = req.ApiKey
      let apiVer = Versions.byKey version apiKey
      let req = Request(apiVer, correlationId, clientId, req)
      let sessionData = toArraySeg Request.size (fun a -> Request.write (apiVer,a)) req
      sessionData,(apiKey,apiVer)

    /// Decodes the session layer input and session state into a response.
    let decode (_, (apiKey:ApiKey,apiVer:ApiVersion), buf:Binary.Segment) =
      ResponseMessage.readApiKey (apiKey,apiVer,buf)

    let session =
      Session.requestReply
        Session.corrId encode decode RequestMessage.awaitResponse receiveStream send

    let send =
      Session.send session

    let send = 
      send
      |> AsyncFunc.timeoutOption config.requestTimeout
      |> Resource.timeoutIndep socketAgent
      |> AsyncFunc.catch
      |> AsyncFunc.mapOut (fun (_,res) ->
        match res with
        | Success (Some (Some res)) -> Success res
        | Success _ -> Failure (Choice1Of2 ())
        | Failure e -> Failure (Choice2Of2 e))
      |> AsyncFunc.doBeforeAfter
          (fun req -> Log.trace "sending_request|request=%A" (RequestMessage.Print req))
          (fun (req,res) -> 
            match res with
            | Success res -> 
              Log.trace "received_response|response=%A" (ResponseMessage.Print res)
            | Failure (Choice1Of2 ()) ->
              Log.warn "request_timed_out|ep=%O request=%s timeout=%O" ep (RequestMessage.Print req) config.requestTimeout
            | Failure (Choice2Of2 e) ->
              Log.warn "request_exception|ep=%O request=%s error=%O" ep (RequestMessage.Print req) e)
      |> Faults.AsyncFunc.retryResultThrowList 
          (fun errors -> TimeoutException(sprintf "Broker request retries terminated after %i errors." errors.Length)) 
          config.requestRetryPolicy

    return Chan (ep, send, Async.empty, Async.empty) }

  /// Discovers brokers via DNS and connects to the first IPv4.
  let discoverConnect (version:System.Version, config:ChanConfig, clientId:ClientId) (host:Host, port:Port) = async {
    let! ip = async {
      match IPAddress.tryParse host with
      | None ->
        Log.info "discovering_dns_entries|host=%s" host
        let! ips = Dns.IPv4.getAllAsync host
        Log.info "discovered_dns_entries|host=%s ips=%A" host ips
        let ip = ips.[0]
        return ip
      | Some ip ->
        return ip }
    let ep = EndPoint.ofIPAddressAndPort (ip, port)
    return! connect (version, config, clientId) ep }