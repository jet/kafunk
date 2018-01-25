#nowarn "40"
namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open FSharp.Control

/// Configuration for a TCP channel to an individual broker.
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
    
  /// The request retry policy for timeouts and failures.
  requestRetryPolicy : RetryPolicy

  /// The buffer pool to use for requests.
  bufferPool : IBufferPool

} with
  
  /// The default TCP receive buffer size = 65536.
  static member DefaultReceiveBufferSize = 65536
  
  /// The default TCP send buffer size = 65536.
  static member DefaultSendBufferSize = 65536
  
  /// The default TCP connection timeout = 10s.
  static member DefaultConnectTimeout = TimeSpan.FromSeconds 10
  
  /// The default TCP connection retry policy = RetryPolicy.constantBoundedMs 3000 2.
  static member DefaultConnectRetryPolicy = RetryPolicy.constantBoundedMs 3000 2

  ///// The default TCP connection retry policy = RetryPolicy.expRandLimitBoundedMs 50 1.5 0.2 1000 2.
  //static member DefaultConnectRetryPolicy = 
  //  RetryPolicy.expRandLimitBoundedMs 50 1.5 0.2 1000 2
  
  /// The default TCP request timeout = 30s.
  static member DefaultRequestTimeout = TimeSpan.FromSeconds 30
  
  /// The default TCP request retry policy = RetryPolicy.constantBoundedMs 1000 2.
  static member DefaultRequestRetryPolicy = RetryPolicy.constantBoundedMs 1000 2
  
  ///// The default TCP request retry policy = RetryPolicy.expRandLimitBoundedMs 100 1.5 0.2 1000 2.
  //static member DefaultRequestRetryPolicy = 
  //  RetryPolicy.expRandLimitBoundedMs 100 1.5 0.2 1000 2

  /// The default TCP Nagle setting = false.
  static member DefaultUseNagle = false

  /// The default buffer pool = BufferPool.GC.
  static member DefaultBufferPool = BufferPool.GC

  /// Creates a broker TCP channel configuration.
  static member create (?useNagle, ?receiveBufferSize, ?sendBufferSize, ?connectTimeout, 
                        ?connectRetryPolicy, ?requestTimeout, ?requestRetryPolicy, ?bufferPool) =
    {
      useNagle = defaultArg useNagle ChanConfig.DefaultUseNagle
      receiveBufferSize = defaultArg receiveBufferSize ChanConfig.DefaultReceiveBufferSize
      sendBufferSize = defaultArg sendBufferSize ChanConfig.DefaultSendBufferSize
      connectTimeout = defaultArg connectTimeout ChanConfig.DefaultConnectTimeout
      connectRetryPolicy = defaultArg connectRetryPolicy ChanConfig.DefaultConnectRetryPolicy
      requestTimeout = defaultArg requestTimeout ChanConfig.DefaultRequestTimeout
      requestRetryPolicy = defaultArg requestRetryPolicy ChanConfig.DefaultRequestRetryPolicy
      bufferPool = defaultArg bufferPool ChanConfig.DefaultBufferPool
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
      | :? EndPoint as ep -> this.Equals (ep)
      | _ -> false
    member this.Equals (other:EndPoint) =
      (EndPoint.endpoint this).Equals(EndPoint.endpoint other)
    override this.GetHashCode () = (EndPoint.endpoint this).GetHashCode()
    override this.ToString () = (EndPoint.endpoint this).ToString()
    interface IEquatable<EndPoint> with
      member this.Equals (other:EndPoint) =
        this.Equals (other)
    interface IComparable with
      member this.CompareTo (other) =
        this.ToString().CompareTo(other.ToString())

/// The result of a request on a channel.
type internal ChanResult = Result<ResponseMessage, ChanError list>

/// A broker channel error.
and internal ChanError =
  | ChanTimeout
  | ChanFailure of exn
  with
    static member internal printErrors (xs:ChanError seq) =
      xs
      |> Seq.map (function ChanError.ChanFailure ex -> sprintf "[chan_exn|error=\"%O\"]" ex | ChanError.ChanTimeout -> "[chan_timeout]")
      |> String.concat " ; "
    static member internal printErrorsShort (xs:ChanError seq) =
      xs
      |> Seq.map (function ChanError.ChanFailure ex -> sprintf "[chan_exn|error=\"%O\"]" ex.Message | ChanError.ChanTimeout -> "[chan_timeout]")
      |> String.concat " ; "

/// A request/reply TCP channel to a Kafka broker.
[<CustomEquality;NoComparison;AutoSerializable(false)>]
type internal Chan = private {
  ep : EndPoint
  send : RequestMessage -> Async<ChanResult>
  socket : Resource<Socket>
}
  with 
    override this.GetHashCode () = this.ep.GetHashCode ()
    member this.Equals (o:Chan) = this.ep.Equals (o.ep)
    override this.Equals (o:obj) =
      match o with
      | :? Chan as o -> this.Equals o
      | _ -> false
    interface IEquatable<Chan> with
      member this.Equals (o:Chan) = this.Equals o
        
/// API operations on a generic request/reply channel.
[<Compile(Module)>]
module internal Chan =

  let private Log = Log.create "Kafunk.Chan"

  /// Sends a request.
  let inline send (ch:Chan) req = ch.send req
  
  /// Gets the endpoint.
  let endpoint (ch:Chan) = ch.ep

  /// Ensures the channel is open.
  let ensureOpen (ch:Chan) = Resource.ensureOpen ch.socket

  /// Closes the channel.
  let close (ch:Chan) = Resource.close ch.socket

  /// Creates a fault-tolerant channel to the specified endpoint.
  /// Recoverable failures are retried, otherwise escalated.
  /// Only a single channel per endpoint is needed.
  let connect (connId:string, apiVersion:ApiKey -> ApiVersion, config:ChanConfig, clientId:ClientId) (ep:EndPoint) : Async<Chan> = async {
    
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
          (fun ep -> Log.info "tcp_connecting|remote_endpoint=%O conn_id=%s" (EndPoint.endpoint ep) connId)
          (fun (ep,res) ->
            let ipep = EndPoint.endpoint ep
            match res with
            | Success s ->
              Log.info "tcp_connected|remote_endpoint=%O local_endpoint=%O conn_id=%s" s.RemoteEndPoint s.LocalEndPoint connId
            | Failure (Choice1Of2 _) ->
              Log.warn "tcp_connection_timed_out|remote_endpoint=%O timeout=%O conn_id=%s" ipep config.connectTimeout connId
            | Failure (Choice2Of2 e) ->
              Log.warn "tcp_connection_failed|remote_endpoint=%O conn_id=%s error=\"%O\"" ipep connId e)
      |> AsyncFunc.mapOut (snd >> Result.codiagExn)
      |> Faults.AsyncFunc.retryResultThrow id Exn.monoid config.connectRetryPolicy

    let close (s:ResourceEpoch<Socket>, ex:exn option) = async {
      try 
        s.resource.Close () 
        match ex with
        | Some ex ->
          Log.warn "tcp_connection_closed|remote_endpoint=%O version=%i error=\"%O\" conn_id=%s" (EndPoint.endpoint ep) s.version ex connId
        | None ->
          Log.info "tcp_connection_closed|remote_endpoint=%O version=%i conn_id=%s" (EndPoint.endpoint ep) s.version connId
      with ex -> 
        Log.error "tcp_connection_close_error|remote_endpoint=%O version=%i error=\"%O\" conn_id=%s" (EndPoint.endpoint ep) s.version ex connId }

    let! socketAgent = 
      Resource.create 
        (ep.ToString())
        (fun _ _ -> conn ep)
        close

    let send =
      socketAgent
      |> Resource.injectWithRecovery
          RetryPolicy.none
          (fun s buf -> Socket.sendAll s buf |> Async.Catch |> Async.map (Result.mapError (ResourceErrorAction.CloseRetry)))

    let receive =
      let receive s buf = async {
        try
          let! received = Socket.receive s buf
          if received = 0 then
            return Failure (ResourceErrorAction.CloseResume (None,0))
          else 
            return Success received
        with ex ->
          Log.warn "receive_failure|remote_endpoint=%O error=\"%O\" conn_id=%s" ep ex connId
          return Failure (ResourceErrorAction.CloseResume (Some ex,0)) }
      socketAgent
      |> Resource.injectWithRecovery RetryPolicy.none receive

    /// An unframed input stream.
    let receiveStream =
      Socket.receiveStreamFrom config.receiveBufferSize receive
      |> Framing.LengthPrefix.unframe

    let bufferPool = config.bufferPool

    /// A framing sender.
    let send = 
      Framing.LengthPrefix.frame >> send
      |> AsyncFunc.tryFinally bufferPool.Free

    /// Encodes the request into a session layer request, keeping ApiKey as state.
    let encode (req:RequestMessage, correlationId:CorrelationId) =
      let apiKey = req.ApiKey
      let apiVer = apiVersion apiKey
      let req = Request(apiVer, correlationId, clientId, req)
      let size = Request.size (apiVer, req)
      let buf = bufferPool.Alloc size
      Request.Write (apiVer, req, BinaryZipper(buf))
      buf,(apiKey,apiVer)

    /// Decodes the session layer input and session state into a response.
    let decode (_, (apiKey:ApiKey,apiVer:ApiVersion), buf:Binary.Segment) =
      ResponseMessage.Read (apiKey,apiVer,BinaryZipper(buf))

    let session =
      Session.requestReply
        (EndPoint.endpoint ep) Session.corrId encode decode RequestMessage.awaitResponse receiveStream send config.requestTimeout

    let _task = 
      socketAgent
      |> Resource.states
      |> AsyncSeq.iterAsync (fun state -> async {
        try
          match state with
          | ResourceState.Open (_,st) -> 
            let! _ = Async.cancelWithTask st (session.Start ())
            return ()
          | ResourceState.Closed -> 
            do! session.Close ()
          | ResourceState.Faulted _ ->
            do! session.Close ()
        with ex ->
          Log.error "session_exception|error=\"%O\"" ex
          do! Resource.fault socketAgent ex })
      |> Async.StartAsTask

    let send = 
      Session.send session
      |> Faults.AsyncFunc.retryAsyncConditional config.requestRetryPolicy
          (fun (_,res) -> not <| Option.isSome res)
          (fun (_,res) -> 
            match res with
            | Some res -> Success res
            | None -> Failure [ChanTimeout])
          (fun (_,ress) -> Failure (ress |> List.map (konst ChanTimeout)))

    return  { ep = ep ; send = send ; socket = socketAgent } }