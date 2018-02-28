[<AutoOpen>]
module internal Kafunk.Tcp

#nowarn "40"

open FSharp.Control
open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Kafunk.AsyncEx.IVar
  
type IPAddress with
  static member tryParse (ipString:string) =
    let mutable ip = Unchecked.defaultof<_>
    if IPAddress.TryParse (ipString, &ip) then Some ip
    else None

type IPEndPoint with
  static member tryParse (ipString:string, port:int) =
    IPAddress.tryParse ipString |> Option.map (fun ip -> IPEndPoint(ip, port))
  static member parse (ipString:string, port:int) =
    IPEndPoint.tryParse (ipString, port) |> Option.get


[<Compile(Module)>]
module Dns =

  module IPv4 =

    let getAllAsync (hostOrAddress:string) =
      Dns.GetHostAddressesAsync(hostOrAddress)
      |> Async.awaitTaskCancellationAsError
      |> Async.map (Array.filter (fun ip -> ip.AddressFamily = AddressFamily.InterNetwork))

    let getAsync (host:string) =
      getAllAsync host |> Async.map (Array.item 0)

    /// Gets an IPv4 IPEndPoint given a host and port.
    let getEndpointAsync (hostOrAddress:string, port:int) = async {
      let! ipv4 = getAsync hostOrAddress
      return IPEndPoint(ipv4, port) }

    /// Gets an IPv4 IPEndPoint given a host and port.
    let getEndpointsAsync (hostOrAddress:string, port:int) = async {
      let! ipv4s = getAllAsync hostOrAddress
      return ipv4s |> Array.map (fun ipv4 -> IPEndPoint(ipv4, port)) }


/// Operations on Berkley sockets.
[<Compile(Module)>]
module Socket =

  /// Executes an async socket operation.
  let exec (alloc:unit -> SocketAsyncEventArgs, free:SocketAsyncEventArgs -> unit) (config:SocketAsyncEventArgs -> unit) (op:SocketAsyncEventArgs -> bool) (map:SocketAsyncEventArgs -> 'a) =
    Async.FromContinuations <| fun (ok, error, _) ->
      try
        let args = alloc ()
        config args
        let rec k (_:obj) (args:SocketAsyncEventArgs) =
          try
            args.remove_Completed(k')
            match args.SocketError with
            | SocketError.Success -> ok (map args)
            | e -> error (SocketException(int e))
          finally
            free args
        and k' = EventHandler<SocketAsyncEventArgs>(k)
        args.add_Completed(k')
        if not (op args) then
          k null args
      with ex ->
        error ex

  let argsAlloc =
    let pool = new ObjectPool<_>(10000, fun () -> new SocketAsyncEventArgs())
    let pop = pool.Pop
    let push (args:SocketAsyncEventArgs) =
      args.AcceptSocket <- null
      args.UserToken <- null
      args.RemoteEndPoint <- null
      args.SetBuffer(null, 0, 0) |> ignore
      if not (isNull args.BufferList) then
        args.BufferList <- null
      pool.Push(args)
    pop,push

  let inline setBuffer (buf:Binary.Segment) (args:SocketAsyncEventArgs) =
    args.SetBuffer(buf.Array, buf.Offset, buf.Count)

  let inline setBuffers (bufs:Binary.Segment[]) (args:SocketAsyncEventArgs) =
    args.BufferList <- bufs

  /// Accepts an incoming connection and pass connection info to a separate receive/send socket.
  let accept (socket:Socket) =
    exec argsAlloc ignore socket.AcceptAsync (fun a -> a.AcceptSocket)

  /// Creates an async sequence of accepted socket connections.
  let acceptStream (socket:Socket) = asyncSeq {
    while true do
      let! socket = accept socket
      yield socket }

  /// Receives data from the socket into the specified Binary. Returns the
  /// number of bytes transferred.
  let receive (socket:Socket) (buf:Binary.Segment) =
    exec argsAlloc (setBuffer buf) socket.ReceiveAsync (fun a -> a.BytesTransferred)

  let receiveFrom (socket:Socket) (remoteEp:IPEndPoint) (buf:Binary.Segment) =
    exec argsAlloc (fun args -> setBuffer buf args ; args.RemoteEndPoint <- remoteEp) socket.ReceiveAsync (fun a -> a.BytesTransferred)

  /// Sends the specified buffer to the socket. Returns the number of bytes
  /// sent.
  let send (socket:Socket) (buf:Binary.Segment) =
    exec argsAlloc (setBuffer buf) socket.SendAsync (fun a -> a.BytesTransferred)

  /// Sends the specified array of buffers to the socket. Returns the number of
  /// bytes sent.
  let sendAll (socket:Socket) (bufs:Binary.Segment[]) =
    exec argsAlloc (setBuffers bufs) socket.SendAsync (fun a -> a.BytesTransferred)

  let sendAllTo (socket:Socket) (remoteEp:IPEndPoint) (bufs:Binary.Segment[]) =
    exec argsAlloc (fun args -> setBuffers bufs args ; args.RemoteEndPoint <- remoteEp) socket.SendAsync (fun a -> a.BytesTransferred)

  /// Connects to a remote host.
  let connect (socket:Socket) (ep:IPEndPoint) =
    exec argsAlloc (fun a -> a.RemoteEndPoint <- ep) socket.ConnectAsync (fun a -> a.ConnectSocket)

  let disconnect (socket:Socket) (reuse:bool) =
    exec argsAlloc (fun a -> a.DisconnectReuseSocket <- reuse) socket.DisconnectAsync (ignore)

  /// Returns an async sequence where each element corresponds to a receive
  /// operation. The sequence finishes when a receive operation completes
  /// transferring 0 bytes. Socket errors are propagated as exceptions.
  let receiveStreamFrom (bufferSize:int) (receive:Binary.Segment -> Async<int>) : AsyncSeq<Binary.Segment> =
    AsyncSeq.unfoldAsync
      (fun remBuff -> async {
        let buff =
          match remBuff with
          | Some rem -> rem
          | None -> bufferSize |> Binary.zeros
        let! received = receive buff
        if received = 0 then 
          return None
        else
          let remainder = buff.Count - received
          let remBuff =
            if remainder > 0 then
              Some (Binary.shiftOffset remainder buff)
            else
              None
          return Some (Binary.resize received buff, remBuff) })
      (None)

  /// Returns an async sequence each item of which corresponds to a receive on
  /// the specified socket. The sequence finishes when a receive operation
  /// completes transferring 0 bytes. Socket errors are propagated as
  /// exceptions.
  let receiveStream (socket:Socket) : AsyncSeq<Binary.Segment> =
    receiveStreamFrom socket.ReceiveBufferSize (receive socket)


/// An exception thrown when an error occurs during framing.
type FramingException (msg) =
  inherit Exception (msg)
  new () = FramingException(null)

/// Stream framing
module Framing =

  /// 32 bit length prefix framer
  module LengthPrefix =

    open FSharp.Core.Operators.Checked

    [<Literal>]
    let HeaderLength = 4

    let inline allocBuffer size =
      try Binary.zeros size 
      with ex -> raise (exn(sprintf "Error trying to allocate a buffer of size=%i" size, ex))

    [<Struct>]
    [<NoEquality;NoComparison;AutoSerializable(false)>]
    type private ReadResult =
      val public remainder : Binary.Segment
      val public length : int
      val public headerBytes : int
      new (remainder:Binary.Segment, length:int) = { remainder = remainder ; length = length ; headerBytes = 0 }
      new (remainder:Binary.Segment, length:int, headerBytes:int) = { remainder = remainder ; length = length ; headerBytes = headerBytes }

    let frame (data:Binary.Segment) : Binary.Segment[] = [|
      yield Binary.zeros HeaderLength |> Binary.pokeInt32 data.Count
      yield data |]

    let frameSeq (data:Binary.Segment seq) : Binary.Segment[] = [|
      let length = data |> Seq.sumBy (fun d -> d.Count)
      yield Binary.zeros HeaderLength |> Binary.pokeInt32 length
      yield! data |]

    let private tryReadLength (headerBytes:int) (length:int) (data:Binary.Segment) : ReadResult =
      let rec loop (headerBytes:int) (length:int) (i:int) =
        if i = data.Offset + data.Count then
          new ReadResult(Binary.empty, length, headerBytes)
        else
          let length = ((int data.Array.[i]) <<< ((HeaderLength - headerBytes - 1) * 8)) ||| length // big endian
          let headerBytes = headerBytes + 1
          if headerBytes = HeaderLength then ReadResult(Binary.offset (i + 1) data, length)
          else loop headerBytes length (i + 1)
      loop headerBytes length data.Offset

    type State =
      struct
        val remainder : Binary.Segment
        val enumerator : Lazy<IAsyncEnumerator<Binary.Segment>>
        new (e,r) = { enumerator = e ; remainder = r }
      end

    /// Reads 32 bytes of the length prefix, then the the message of the former length, then yields, then continues.
    let unframe (s:AsyncSeq<Binary.Segment>) : AsyncSeq<Binary.Segment> =
          
      let rec readLength (headerBytes:int) (length:int) (s:State) = async {
        let! next = async {
          if s.remainder.Count > 0 then
            return Some s.remainder
          else
            return! s.enumerator.Value.MoveNext() }
        match next with
        | None ->
          s.enumerator.Value.Dispose() 
          return None
        | Some data ->
          let readResult = tryReadLength headerBytes length data
          let s' = State(s.enumerator, readResult.remainder) 
          if readResult.headerBytes = 0 then
            let buffer = allocBuffer readResult.length
            return! readData readResult.length buffer s'
          else
            return! readLength readResult.headerBytes readResult.length s' }

      and readData (length:int) (buffer:Binary.Segment) (s:State) = async {
        let! next = async {
          if s.remainder.Count > 0 then
            return Some s.remainder
          else
            return! s.enumerator.Value.MoveNext() }
        match next with
        | None ->
          s.enumerator.Value.Dispose()
          return None
        | Some data ->
          let copy = min data.Count (length - buffer.Offset)
          Binary.copy data buffer copy
          let s' = State(s.enumerator, data |> Binary.shiftOffset copy)
          let bufferIndex = buffer.Offset + copy
          if bufferIndex = length then
            let buffer = buffer |> Binary.offset 0
            return Some (buffer, s')
          else
            let buffer = buffer |> Binary.offset bufferIndex
            return! readData length buffer s' }

      AsyncSeq.unfoldAsync (readLength 0 0) (State(lazy (s.GetEnumerator()), Binary.Segment()))


// ----------------------------------------------------------------------------------------------
// session layer (layer 5)

/// A correlation id.
type CorrelationId = int32

/// A client creates a session message, assigns a unique tx id and waits for a response from the server.
/// A server receives a session message, and sends a response with the same tx id.
[<Struct>]
[<NoEquality;NoComparison;AutoSerializable(false)>]
type SessionMessage =
  val public tx_id : CorrelationId
  val public payload : Binary.Segment
  new (txId, payload) = { tx_id = txId ; payload = payload }
with
  static member decode (buf:Binary.Segment) = 
    if buf.Count < 4 then raise (FramingException("Insufficient data to decode SessionMessage."))
    let txId = Binary.peekInt32 buf
    let payload = Binary.shiftOffset 4 buf
    SessionMessage (txId,payload)
    
/// An exception raised on failure to decode a raw TCP response.
/// This is a fatal exception and should be escalated.
type ResponseDecodeException (ex:exn) = inherit Exception ("There was an error decoding the raw TCP response.", ex)

/// A multiplexed request/reply session.
/// Maintains state between requests and responses and contains a process reading the input stream.
/// Send failures are propagated to the caller who is responsible for recreating the session.
[<NoEquality;NoComparison;AutoSerializable(false)>]
type ReqRepSession<'a, 'b, 's> internal
  (    
    /// The remote endpoint.
    remoteEndpoint:IPEndPoint,

    /// A correlation id generator.
    correlationId:unit -> CorrelationId,

    /// Encodes a request with a correlation id into a byte array and a state maintained for the duration of the request.
    encode:'a * CorrelationId -> Binary.Segment * 's,

    /// Decodes a response given the correlatio id, the maintained state and the response byte array.
    decode:CorrelationId * 's * Binary.Segment -> 'b,

    /// If a request 'a does not expect a response, return Some with the default response.
    awaitResponse:'a -> 'b option,

    /// A stream of messages corresponding to the stream received from the remote host.
    receive:AsyncSeq<Binary.Segment>,

    /// Sends a message to the remote host.
    send:Binary.Segment -> Async<int>,
    
    requestTimeout : TimeSpan) =

  static let Log = Log.create "Kafunk.TcpSession"

  // these fields define the state of the session
  let txs = new ConcurrentDictionary<CorrelationId, DateTime * 's * TaskCompletionSource<'b option>>()
  let [<VolatileField>] mutable sessionTask : Task<unit> = null

  member private __.Demux (data:Binary.Segment) =
    let sessionData = SessionMessage.decode data
    let correlationId = sessionData.tx_id
    let mutable token = Unchecked.defaultof<_>
    if txs.TryRemove(correlationId, &token) then
      //Log.trace "received_response|correlation_id=%i size=%i" correlationId sessionData.payload.Count
      let _dt,state,reply = token
      try
        let res = decode (correlationId,state,sessionData.payload)
        if not (reply.TrySetResult (Some res)) then
          Log.warn "received_response_was_already_cancelled|correlation_id=%i size=%i" correlationId sessionData.payload.Count
      with ex ->
        Log.error "response_decode_exception|correlation_id=%i error=\"%O\"" correlationId ex
        reply.TrySetException (ResponseDecodeException(ex)) |> ignore
    else
      Log.trace "received_orphaned_response|correlation_id=%i in_flight_requests=%i" correlationId txs.Count

  member private __.Mux (sessionTask,req) =
    let startTime = DateTime.UtcNow
    let correlationId = correlationId ()
    let rep = TaskCompletionSource<_>()    
    let sessionReq,state = encode (req,correlationId)
    let timeout = Task.Delay(requestTimeout)
    Task.WhenAny (timeout, sessionTask, rep.Task)
    |> Task.extend (fun t ->
      if obj.ReferenceEquals (t.Result, sessionTask) then __.Cancel (correlationId,true)
      elif obj.ReferenceEquals (t.Result, timeout) then  __.Cancel (correlationId,false)
      else ())
    |> ignore      
    match awaitResponse req with
    | None ->
      if not (txs.TryAdd(correlationId, (startTime,state,rep))) then
        Log.error "clash_of_the_sessions"
        invalidOp (sprintf "clash_of_the_sessions|correlation_id=%i" correlationId)
    | Some res ->
      rep.SetResult (Some res)
    correlationId,sessionReq,rep

  member private __.Cancel (correlationId,error) = 
    let mutable token = Unchecked.defaultof<_>
    if txs.TryRemove(correlationId, &token) then
      let startTime,state,rep = token
      let inProgress = 
        if error then rep.TrySetException (OperationCanceledException())
        else rep.TrySetResult None 
      if inProgress then
        let endTime = DateTime.UtcNow
        let elapsed = endTime - startTime
        Log.trace "request_cancelled|remote_endpoint=%O correlation_id=%i in_flight_requests=%i state=%A start_time=%s end_time=%s elapsed_sec=%f" 
          remoteEndpoint correlationId txs.Count state (startTime.ToString("s")) (endTime.ToString("s")) elapsed.TotalSeconds

  /// Starts the session.
  member internal __.Start (task:Task<unit>) = async {
    sessionTask <- task
    try
      Log.trace "starting_session|remote_endpoint=%O" remoteEndpoint
      do! receive |> AsyncSeq.iter __.Demux
      //Log.info "session_closed2|remote_endpoint=%O" remoteEndpoint
    with ex ->
      Log.error "session_exception|remote_endpoint=%O error=\"%O\"" remoteEndpoint ex
      return raise ex }

  member internal __.Close () = async {
    Log.trace "session_closed|remote_endpoint=%O" remoteEndpoint
    sessionTask <- Task.never
    return () }

  member internal __.Send (req:'a) = async {    
    let _correlationId,sessionData,rep = __.Mux (sessionTask,req)
    let! _sent = send sessionData
    return! rep.Task |> Async.awaitTaskCancellationAsError }


/// Operations on network sessions (layer 5).
[<Compile(Module)>]
module Session =

  let [<Literal>] HeaderSize = 4

  /// A correlation id factory.
  let corrId : unit -> CorrelationId =
    let id = ref 0
    fun () -> Interlocked.Increment id

  /// Creates a request/reply session listening to the specified input stream and
  /// sending to the specified output.
  let requestReply
    (remoteEndpoint:IPEndPoint)
    (correlationId:unit -> CorrelationId)
    (encode:'a * CorrelationId -> Binary.Segment * 's)
    (decode:CorrelationId * 's * Binary.Segment -> 'b)
    (awaitResponse:'a -> 'b option)
    (receive:AsyncSeq<Binary.Segment>)
    (send:Binary.Segment -> Async<int>)
    (requestTimeout:TimeSpan)=
      new ReqRepSession<'a, 'b, 's>(remoteEndpoint, correlationId, encode, decode, awaitResponse, receive, send, requestTimeout)

  /// Sends a request on the session and awaits the response.
  let send (session:ReqRepSession<_, _, _>) req = session.Send req