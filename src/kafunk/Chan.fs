#nowarn "40"
namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Text
open System.Threading
open System.Threading.Tasks


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
    
  /// The request retry polify for timeouts and failures.
  requestRetryPolicy : RetryPolicy

} with
  
  /// The default TCP receive buffer size = 65536.
  static member DefaultReceiveBufferSize = 65536
  
  /// The default TCP send buffer size = 65536.
  static member DefaultSendBufferSize = 65536
  
  /// The default TCP connection timeout = 10s.
  static member DefaultConnectTimeout = TimeSpan.FromSeconds 10
  
  /// The default TCP connection retry policy = RetryPolicy.constantBoundedMs 2000 50.
  static member DefaultConnectRetryPolicy = RetryPolicy.constantBoundedMs 2000 50
  
  /// The default TCP request timeout = 30s.
  static member DefaultRequestTimeout = TimeSpan.FromSeconds 30
  
  /// The default TCP request retry policy = RetryPolicy.constantBoundedMs 2000 50.
  static member DefaultRequestRetryPolicy = RetryPolicy.constantBoundedMs 2000 50
  
  /// The default TCP Nagle setting = false.
  static member DefaultUseNagle = false

  /// Creates a broker TCP channel configuration.
  static member create (?useNagle, ?receiveBufferSize, ?sendBufferSize, ?connectTimeout, ?connectRetryPolicy, ?requestTimeout, ?requestRetryPolicy) =
    {
      useNagle = defaultArg useNagle ChanConfig.DefaultUseNagle
      receiveBufferSize = defaultArg receiveBufferSize ChanConfig.DefaultReceiveBufferSize
      sendBufferSize = defaultArg sendBufferSize ChanConfig.DefaultSendBufferSize
      connectTimeout = defaultArg connectTimeout ChanConfig.DefaultConnectTimeout
      connectRetryPolicy = defaultArg connectRetryPolicy ChanConfig.DefaultConnectRetryPolicy
      requestTimeout = defaultArg requestTimeout ChanConfig.DefaultRequestTimeout
      requestRetryPolicy = defaultArg requestRetryPolicy ChanConfig.DefaultRequestRetryPolicy
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
type ChanResult = Result<ResponseMessage, ChanError list>

/// A channel error result.
and ChanError =
  | ChanTimeout
  | ChanFailure of exn

/// A request/reply TCP channel to a Kafka broker.
[<CustomEquality;NoComparison>]
type internal Chan = private {
  ep : EndPoint
  send : RequestMessage -> Async<ChanResult>
  task : Task<unit>
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
  let send (ch:Chan) req = ch.send req
  
  /// Gets the endpoint.
  let endpoint (ch:Chan) = ch.ep

  let internal task (ch:Chan) = ch.task

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
          (fun ep -> Log.info "tcp_connecting|client_id=%s remote_endpoint=%O" clientId (EndPoint.endpoint ep))
          (fun (ep,res) ->
            let ipep = EndPoint.endpoint ep
            match res with
            | Success s ->
              Log.info "tcp_connected|client_id=%s remote_endpoint=%O local_endpoint=%O" clientId s.RemoteEndPoint s.LocalEndPoint
            | Failure (Choice1Of2 _) ->
              Log.warn "tcp_connection_timed_out|client_id=%s remote_endpoint=%O timeout=%O" clientId ipep config.connectTimeout
            | Failure (Choice2Of2 e) ->
              Log.error "tcp_connection_failed|client_id=%s remote_endpoint=%O error=%O" clientId ipep e)
      |> AsyncFunc.mapOut (snd >> Result.codiagExn)
      |> Faults.AsyncFunc.retryResultThrow id Exn.monoid config.connectRetryPolicy

    let recovery (s:Socket, ver:int, _req:obj, ex:exn) = async {
      Log.warn "recovering_tcp_connection|client_id=%s remote_endpoint=%O version=%i error=%O" clientId (EndPoint.endpoint ep) ver ex
      tryDispose s }

    let! socketAgent = 
      Resource.recoverableRecreate 
        (fun _ _ -> conn ep)
        recovery

    let! send =
      socketAgent
      |> Resource.inject Socket.sendAll

    let! receive =
      let receive s buf = async {
        try
          let! received = Socket.receive s buf
          if received = 0 then 
            Log.warn "received_empty_buffer|client_id=%s remote_endpoint=%O" clientId ep
            return Failure (ResourceErrorAction.RecoverResume (exn("received_empty_buffer"),0))
          else 
            return Success received
        with ex ->
          Log.error "receive_failure|client_id=%s remote_endpoint=%O error=%O" clientId ep ex
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
      //|> Resource.timeoutIndep socketAgent
      |> AsyncFunc.mapOut (snd >> Some)
      |> AsyncFunc.catch
      |> AsyncFunc.mapOut (fun (_,res) ->
        match res with
        | Success (Some (Some res)) -> Success res
        | Success _ -> Failure (Choice1Of2 ())
        | Failure e -> Failure (Choice2Of2 e))
      |> AsyncFunc.doBeforeAfter
          //(fun _req -> (*Log.trace "sending_request|request=%s" (RequestMessage.Print req)*) ())
          (ignore)
          (fun (req,res) -> 
            match res with
            | Success _res -> 
              ()
              //Log.trace "received_response|response=%s" (ResponseMessage.Print res)
            | Failure (Choice1Of2 ()) ->
              Log.warn "request_timed_out|client_id=%s ep=%O request=%s timeout=%O" 
                clientId ep (RequestMessage.Print req) config.requestTimeout
            | Failure (Choice2Of2 e) ->
              Log.warn "request_exception|ep=%O request=%s error=%O" ep (RequestMessage.Print req) e)
      |> Faults.AsyncFunc.retryResultList config.requestRetryPolicy
      |> AsyncFunc.mapOut (snd >> Result.mapError (List.map (Choice.fold (konst ChanTimeout) (ChanFailure))))

    return  { ep = ep ; send = send ; task = session.Task } }