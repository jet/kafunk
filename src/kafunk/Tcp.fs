#nowarn "40"
namespace Kafunk

open FSharp.Control
open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module internal NetEx =
  
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


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module internal Dns =

  module IPv4 =

    let getAllAsync (hostOrAddress:string) =
      Dns.GetHostAddressesAsync(hostOrAddress)
      |> Async.AwaitTask
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
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Socket =

  /// Executes an async socket operation.
  let exec (alloc:unit -> SocketAsyncEventArgs, free:SocketAsyncEventArgs -> unit) (config:SocketAsyncEventArgs -> unit) (op:SocketAsyncEventArgs -> bool) (map:SocketAsyncEventArgs -> 'a) =
    Async.FromContinuations <| fun (ok, error, _) ->
      let args = alloc ()
      config args
      let rec k (_:obj) (args:SocketAsyncEventArgs) =
        args.remove_Completed(k')
        try
          match args.SocketError with
          | SocketError.Success -> ok (map args)
          | e -> error (SocketException(int e))
        finally
          free args
      and k' = EventHandler<SocketAsyncEventArgs>(k)
      args.add_Completed(k')
      if not (op args) then
          k null args

  let argsAlloc =
    let pool = new ObjectPool<_>(10000, fun () -> new SocketAsyncEventArgs())
    let pop = pool.Pop
    let push (args:SocketAsyncEventArgs) =
      args.AcceptSocket <- null
      args.RemoteEndPoint <- null
      args.SetBuffer(null, 0, 0) |> ignore
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
    let remBuff = ref None
    let inline getBuffer () =
      match !remBuff with
      | Some rem -> rem
      | None -> bufferSize |> Binary.zeros
    let rec go () = asyncSeq {
      let buff = getBuffer ()
      let! received = receive buff
      if received = 0 then ()
      else
        let remainder = buff.Count - received
        if remainder > 0 then
          remBuff := Some (Binary.shiftOffset remainder buff)
        else
          remBuff := None
        yield buff |> Binary.resize received
        yield! go () }
    go ()

  /// Returns an async sequence each item of which corresponds to a receive on
  /// the specified socket. The sequence finishes when a receive operation
  /// completes transferring 0 bytes. Socket errors are propagated as
  /// exceptions.
  let receiveStream (socket:Socket) : AsyncSeq<Binary.Segment> =
    receiveStreamFrom socket.ReceiveBufferSize (receive socket)

/// Stream framing
module Framing =

  /// 32 bit length prefix framer
  module LengthPrefix =

    [<Literal>]
    let HeaderLength = 4

    let inline allocBuffer length = Binary.zeros length

    [<Struct>]
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
//      if data.Count >= (HeaderLength - headerBytes) then
//        let length = Binary.peekInt32 data
//        let remainder = Binary.offset (data.Offset + HeaderLength) data
//        ReadResult(remainder, length)
//      else
        let rec loop (headerBytes:int) (length:int) (i:int) =
          if i = data.Offset + data.Count then
            new ReadResult(Binary.empty, length, headerBytes)
          else
            let length = ((int data.Array.[i]) <<< ((HeaderLength - headerBytes - 1) * 8)) ||| length // big endian
            let headerBytes = headerBytes + 1
            if headerBytes = HeaderLength then ReadResult(Binary.offset (i + 1) data, length)
            else loop headerBytes length (i + 1)
        loop headerBytes length data.Offset


//    type FramerState =
//      | Await of (Binary.Segment option -> FramerState)
//      | Yield of Binary.Segment * FramerState
//      | Halt
//
//    let framer = 
//
//      let awaitHalt f = Await (function Some data -> f data | None -> Halt)
//
//      let rec awaitLength (headerBytes:int) (length:int) (rem:Binary.Segment) =
//        awaitHalt (fun data' ->
//          let data =
//            if rem.Count > 0 then rem
//            else data'
//          let readResult = tryReadLength headerBytes length data
//          if readResult.headerBytes = 0 then
//            let buffer = allocBuffer readResult.length
//            awaitMessage readResult.length buffer
//          else
//            awaitLength readResult.headerBytes readResult.length)
//            
//      and awaitMessage (length:int) (buffer:Binary.Segment) =
//        awaitHalt (fun data ->
//          let copy = min data.Count (length - buffer.Offset)
//          Binary.copy data buffer copy
//          let bufferIndex = buffer.Offset + copy
//          if bufferIndex = length then
//            let buffer = buffer |> Binary.offset 0
//            Yield (buffer, awaitLength 0 0)
//          else
//            let buffer = buffer |> Binary.offset bufferIndex
//            awaitMessage length buffer)
//        
//      awaitLength 0 0 (Binary.Segment())
//
//
//    let unframe2 (f:FramerState) (input:AsyncSeq<Binary.Segment>) : AsyncSeq<Binary.Segment> =      
//      let rec go (en:IAsyncEnumerator<_>) (s:FramerState) = async {
//        match s with
//        | Halt ->
//          return None
//        | Yield (m,s') -> 
//          return (Some (m,s'))
//        | Await recv ->
//          let! next = en.MoveNext ()
//          let s' = recv next
//          return! go en s' }      
//      AsyncSeq.unfoldAsync (go (input.GetEnumerator())) f


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

// session layer (layer 5)

/// A correlation id.
type CorrelationId = int32

/// A client creates a session message, assigns a unique tx id and waits for a response from the server.
/// A server receives a session message, and sends a response with the same tx id.
[<Struct>]
type SessionMessage =
  val public tx_id : CorrelationId
  val public payload : Binary.Segment
  new (data:Binary.Segment) = { tx_id = Binary.peekInt32 data; payload = Binary.shiftOffset 4 data }
  new (txId, payload) = { tx_id = txId ; payload = payload }

/// A multiplexed request/reply session.
/// Note a session is stateful in that it maintains state between requests and responses
/// and in that starts a process to read the input stream.
/// Send failures are propagated to the caller who is responsible for recreating the session.
type ReqRepSession<'a, 'b, 's> internal
  (
    /// A correlation id generator.
    correlationId:unit -> CorrelationId,

    /// Encodes a request with a correlation id into a byte array and a state maintained for the duration of the request.
    encode:'a * CorrelationId -> Binary.Segment * 's,

    /// Decodes a response given the correlatio id, the maintained state and the response byte array.
    decode:CorrelationId * 's * Binary.Segment -> 'b,

    /// If a request 'a does not expect a response, return Some with the default response.
    awaitResponse:'a -> 'b option,

    /// A stream of bytes corresponding to the stream received from the remote host.
    receive:AsyncSeq<Binary.Segment>,

    /// Sends a byte array to the remote host.
    send:Binary.Segment -> Async<int>) =

  static let Log = Log.create "Kafunk.TcpSession"

  let txs = new ConcurrentDictionary<int, DateTime * 's * TaskCompletionSource<'b>>()
  let cts = new CancellationTokenSource()

  let demux (data:Binary.Segment) =
    let sessionData = SessionMessage(data)
    let correlationId = sessionData.tx_id
    let mutable token = Unchecked.defaultof<_>
    if txs.TryRemove(correlationId, &token) then
      //Log.trace "received_response|correlation_id=%i size=%i" correlationId sessionData.payload.Count
      let _dt,state,reply = token
      try
        let res = decode (correlationId,state,sessionData.payload)
        if not (reply.TrySetResult res) then
          Log.warn "received_response_was_already_cancelled|correlation_id=%i size=%i" correlationId sessionData.payload.Count
      with ex ->
        Log.error "response_decode_exception|correlation_id=%i error=%O payload=%s" correlationId ex (Binary.toString sessionData.payload)
        reply.SetException ex
    else
      Log.error "received_orphaned_response|correlation_id=%i in_flight_requests=%i" correlationId txs.Count

  let mux (ct:CancellationToken) (req:'a) =
    let startTime = DateTime.UtcNow
    let correlationId = correlationId ()
    let rep = TaskCompletionSource<_>()
    let sessionReq,state = encode (req,correlationId)    
    let cancel () =
      if rep.TrySetException (TimeoutException("The timeout expired before a response was received from the TCP stream.")) then
        let endTime = DateTime.UtcNow
        let elapsed = endTime - startTime
        Log.warn "request_timed_out|correlation_id=%i in_flight_requests=%i state=%A start_time=%s end_time=%s elapsed_sec=%f" correlationId txs.Count state (startTime.ToString("s")) (endTime.ToString("s")) elapsed.TotalSeconds
        let mutable token = Unchecked.defaultof<_>
        txs.TryRemove(correlationId, &token) |> ignore
    ct.Register (Action(cancel)) |> ignore
    match awaitResponse req with
    | None ->
      if not (txs.TryAdd(correlationId, (startTime,state,rep))) then
        Log.error "clash_of_the_sessions"
        invalidOp (sprintf "clash_of_the_sessions|correlation_id=%i" correlationId)
    | Some res ->
      rep.SetResult res
    correlationId,sessionReq,rep

  let rec receiveLoop = async {
    try
      do!
        receive
        |> AsyncSeq.iter demux
        |> Async.tryFinally (fun () -> Log.warn "session_disconnected|in_flight_requests=%i" txs.Count)
      Log.warn "restarting_receive_loop" 
      return! receiveLoop
    with ex ->
      Log.error "receive_loop_faiure|error=%O" ex
      return! receiveLoop }

  do Async.Start(receiveLoop, cts.Token)

  member x.Send (req:'a) = async {
    let! ct = Async.CancellationToken
    let _correlationId,sessionData,rep = mux ct req
    //Log.trace "sending_request|correlation_id=%i bytes=%i" correlationId sessionData.Count
    let! _sent = send sessionData
    //Log.trace "request_sent|correlation_id=%i bytes=%i" correlationId sent
    return! rep.Task |> Async.AwaitTask }

  interface IDisposable with
    member x.Dispose() = cts.Dispose()


/// Operations on network sessions (layer 5).
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Session =

  let [<Literal>] HeaderSize = 4

  let inline reply (sm:SessionMessage) (payload) =
    SessionMessage(sm.tx_id, payload)

  let builder () =
    let tx_id = ref 0
    fun m -> SessionMessage(Interlocked.Increment tx_id, m)

  let encodeSession (m:SessionMessage) : Binary.Segment[] = [|
    yield BitConverter.GetBytes m.tx_id |> Binary.ofArray
    yield m.payload |]

  /// A correlation id factory.
  let corrId : unit -> CorrelationId =
    let id = ref 0
    fun () -> Interlocked.Increment id

  /// Creates a request/reply session listening to the specified input stream and
  /// sending to the specified output.
  let requestReply
    (correlationId:unit -> int)
    (encode:'a * CorrelationId -> Binary.Segment * 's)
    (decode:CorrelationId * 's * Binary.Segment -> 'b)
    (awaitResponse:'a -> 'b option)
    (receive:AsyncSeq<Binary.Segment>)
    (send:Binary.Segment -> Async<int>) =
      new ReqRepSession<'a, 'b, 's>(correlationId, encode, decode, awaitResponse, receive, send)
