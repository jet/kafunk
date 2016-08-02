#nowarn "40"
namespace Kafunk

open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Kafunk.Logging

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


/// Operations on Berkley sockets.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Socket =

  /// Executes an async socket operation.
  let exec (alloc:unit -> SocketAsyncEventArgs, free:SocketAsyncEventArgs -> unit) (config:SocketAsyncEventArgs -> unit) (op:SocketAsyncEventArgs -> bool) (map:SocketAsyncEventArgs -> 'a) =
    Async.FromContinuations <| fun (ok, error, _) ->
      let args = alloc ()
      config args
      let rec k (_:obj) (args:SocketAsyncEventArgs) =
        match args.SocketError with
        | SocketError.Success ->
          args.remove_Completed(k')
          let result = map args
          free args
          ok result
        | e ->
          args.remove_Completed(k')
          free args
          error (SocketException(int e))
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
      yield Binary.zeros 4 |> Binary.pokeInt32 data.Count
      yield data |]

    let frameSeq (data:Binary.Segment seq) : Binary.Segment[] = [|
      let length = data |> Seq.sumBy (fun d -> d.Count)
      yield Binary.zeros 4 |> Binary.pokeInt32 length
      yield! data |]

    let private tryReadLength (headerBytes:int) (length:int) (data:Binary.Segment) : ReadResult =
      if data.Count >= HeaderLength then
        let length = Binary.peekInt32 data
        let remainder = Binary.offset (data.Offset + HeaderLength) data
        ReadResult(remainder, length)
      else
        let rec loop  (headerBytes:int) (length:int) (i:int) =
          if i = data.Offset + data.Count then
            new ReadResult(Binary.empty, length, headerBytes)
          else
            let length = (int data.Array.[i]) <<< (headerBytes * 8) ||| length // little endian
            let headerBytes = headerBytes + 1
            if headerBytes = HeaderLength then ReadResult(Binary.offset (i + 1) data, length)
            else loop headerBytes length (i + 1)
        loop headerBytes length data.Offset

    /// Reads 32 bytes of the length prefix, then the length of the message, then yields, then continues.
    let unframe (s:AsyncSeq<Binary.Segment>) : AsyncSeq<Binary.Segment> =
      let rec go (offset:int) (s:AsyncSeq<Binary.Segment>) =
        readLength 0 0 offset s
      and readLength (headerBytes:int) (length:int) (offset:int) (s:AsyncSeq<Binary.Segment>) =
        s |> Async.bind (function
          | Nil -> async.Return Nil
          | Cons (data, tl) as s ->
            let data =
              if offset > 0 then data |> Binary.shiftOffset (data.Count - offset)
              else data
            let readResult = tryReadLength headerBytes length data
            let tl, offset =
              if readResult.remainder.Count = 0 then tl,0
              else (async.Return s),readResult.remainder.Count
            if readResult.headerBytes = 0 then
              let buffer = allocBuffer readResult.length
              readData readResult.length buffer offset tl
            else
              readLength readResult.headerBytes readResult.length offset tl)
      and readData (length:int) (buffer:Binary.Segment) (offset:int) (s:AsyncSeq<Binary.Segment>) =
        s |> Async.bind (function
          | Nil -> async.Return Nil
          | Cons (data, tl) as s ->
            let data =
              if offset > 0 then data |> Binary.shiftOffset (data.Count - offset)
              else data
            let copy = min data.Count (length - buffer.Offset)
            Binary.copy data buffer copy
            let tl, offset =
              if copy = data.Count then tl, 0
              else (async.Return s, data.Count - copy)
            let bufferIndex = buffer.Offset + copy
            if bufferIndex = length then
              let buffer = buffer |> Binary.offset 0
              Cons (buffer, go offset tl) |> async.Return
            else
              let buffer = buffer |> Binary.offset bufferIndex
              readData length buffer offset tl)
      go 0 s

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

  let txs = new ConcurrentDictionary<int, 's * TaskCompletionSource<'b>>()
  let cts = new CancellationTokenSource()

  let demux (data:Binary.Segment) =
    let sessionData = SessionMessage(data)
    let correlationId = sessionData.tx_id
    let mutable token = Unchecked.defaultof<_>
    if txs.TryRemove(correlationId, &token) then
      Log.log Verbose (
        Message.eventX "Received message"
        >> Message.setFieldValue "correlationId" correlationId
        >> Message.setFieldValue "size" sessionData.payload.Count)
      let state, reply = token
      let res = decode (correlationId,state,sessionData.payload)
      reply.SetResult res
    else
      Log.logSimple (
        Message.event Error "Received message but unabled to find session for {correlationId}"
        |> Message.setFieldValue "correlationId" correlationId)

  let mux (req:'a) =
    let correlationId = correlationId ()
    let rep = TaskCompletionSource<_>()
    let sessionReq, state = encode (req,correlationId)
    match awaitResponse req with
    | None ->
      if not (txs.TryAdd(correlationId, (state,rep))) then
        Log.logSimple (Message.event Error "Clash of the sessions!")
    | Some res ->
      rep.SetResult res
    correlationId,sessionReq,rep

  do
    receive
    |> AsyncSeq.iter demux
    |> Async.tryFinally (fun () -> Log.log Info (Message.eventX "Session disconnected"))
    |> (fun t -> Async.Start(t, cts.Token))

  member x.Send (req:'a) = async {
    let correlationId,sessionData,rep = mux req
    Log.log Verbose (
      Message.eventX "Sending request..."
      >> Message.setFieldValue "correlationId" correlationId
      >> Message.setFieldValue "size" sessionData.Count)
    let! sent = send sessionData
    Log.log Verbose (
      Message.eventX "Request sent"
      >> Message.setFieldValue "correlationId" correlationId
      >> Message.setFieldValue "bytes" sent)
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
    (encode:'a * int -> Binary.Segment * 's)
    (decode:int * 's * Binary.Segment -> 'b)
    (awaitResponse:'a -> 'b option)
    (receive:AsyncSeq<Binary.Segment>)
    (send:Binary.Segment -> Async<int>) =
      new ReqRepSession<'a, 'b, 's>(correlationId, encode, decode, awaitResponse, receive, send)
