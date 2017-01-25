#nowarn "40"
namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks


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


/// A broker endpoint.
[<CustomEquality;CustomComparison;StructuredFormatDisplay("{Display}")>]
type internal EndPoint = 
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

/// The result of a request on a channel.
type ChanResult = Result<ResponseMessage, ChanError list>

/// A channel error result.
and ChanError =
  | ChanTimeout
  | ChanFailure of exn

/// A request/reply TCP channel to a Kafka broker.
type internal Chan =
  | Chan of ep:EndPoint * send:(RequestMessage -> Async<ChanResult>) * reconnect:Async<unit> * close:Async<unit>

/// API operations on a generic request/reply channel.
[<Compile(Module)>]
module internal Chan =

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
          SendBufferSize=config.sendBufferSize)
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
      tryDispose s }

    let! socketAgent = 
      Resource.recoverableRecreate 
        (fun _ -> conn ep)
        recovery

    let! send =
      socketAgent
      |> Resource.inject Socket.sendAll

    let! receive =
      let receive s buf = async {
        try
          let! received = Socket.receive s buf
          if received = 0 then 
            Log.warn "received_empty_buffer|remote_endpoint=%O" ep
            return Failure (ResourceErrorAction.RecoverResume (exn("received_empty_buffer"),0))
          else 
            return Success received
        with ex ->
          Log.error "receive_failure|remote_endpoint=%O error=%O" ep ex
          return Failure (ResourceErrorAction.RecoverResume (ex,0)) }
      socketAgent 
      |> Resource.injectResult receive

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
          (fun req -> Log.trace "sending_request|request=%s" (RequestMessage.Print req))
          (fun (req,res) -> 
            match res with
            | Success res -> 
              Log.trace "received_response|response=%s" (ResponseMessage.Print res)
            | Failure (Choice1Of2 ()) ->
              Log.warn "request_timed_out|ep=%O request=%s timeout=%O" ep (RequestMessage.Print req) config.requestTimeout
            | Failure (Choice2Of2 e) ->
              Log.warn "request_exception|ep=%O request=%s error=%O" ep (RequestMessage.Print req) e)
      |> Faults.AsyncFunc.retryResultList config.requestRetryPolicy
      |> AsyncFunc.mapOut (snd >> Result.mapError (List.map (Choice.fold (konst ChanTimeout) (ChanFailure))))

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