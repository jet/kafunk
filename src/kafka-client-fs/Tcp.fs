#nowarn "40"
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


/// Operations on Berkley sockets.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Socket =

  /// Executes an async socket operation.
  let inline exec (alloc:unit -> SocketAsyncEventArgs, free:SocketAsyncEventArgs -> unit) (prep:SocketAsyncEventArgs -> unit) (op:SocketAsyncEventArgs -> bool) (map:SocketAsyncEventArgs -> 'a) =
    Async.FromContinuations <| fun (ok, error, _) ->

      let args = alloc ()
      prep args

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


//  let argsAlloc =
//    (fun () -> new SocketAsyncEventArgs()),(fun (args:SocketAsyncEventArgs) -> args.Dispose())

  let argsAlloc =
    let pool = new ObjectPool<_>(10000, fun () -> new SocketAsyncEventArgs())
    let pop = pool.Pop
    let push (args:SocketAsyncEventArgs) =
      args.AcceptSocket <- null
      args.RemoteEndPoint <- null
      args.SetBuffer(null, 0, 0) |> ignore
      args.BufferList <- null
      //args.UserToken <- null
      pool.Push(args)
    pop,push

  let inline setBuffer (buf:ArraySegment<byte>) (args:SocketAsyncEventArgs) =
    args.SetBuffer(buf.Array, buf.Offset, buf.Count)

  let inline setBuffers (bufs:ArraySegment<byte>[]) (args:SocketAsyncEventArgs) =
    args.BufferList <- bufs

  /// Accepts an incoming connection and pass connection info to a separate receive/send socket.
  let accept (socket:Socket) =
    exec argsAlloc ignore socket.AcceptAsync (fun a -> a.AcceptSocket)

  /// Creates an async sequence of accepted socket connections.
  let acceptStream (socket:Socket) = asyncSeq {
    while true do
      let! socket = accept socket
      yield socket }

  /// Receives data from the socket into the specified buffer.
  /// Returns the number of bytes transferred.
  let receive (socket:Socket) (buf:ArraySegment<byte>) =
    exec argsAlloc (setBuffer buf) socket.ReceiveAsync (fun a -> a.BytesTransferred)

  let receiveFrom (socket:Socket) (remoteEp:IPEndPoint) (buf:ArraySegment<byte>) =
    exec argsAlloc (fun args -> setBuffer buf args ; args.RemoteEndPoint <- remoteEp) socket.ReceiveAsync (fun a -> a.BytesTransferred)

  /// Sends the specified buffer to the socket.
  /// Returns the number of bytes sent.
  let send (socket:Socket) (buf:ArraySegment<byte>) =
    exec argsAlloc (setBuffer buf) socket.SendAsync (fun a -> a.BytesTransferred)

  /// Sends the specified array of buffers to the socket.
  /// Returns the number of bytes sent.
  let sendAll (socket:Socket) (bufs:ArraySegment<byte>[]) =
    exec argsAlloc (setBuffers bufs) socket.SendAsync (fun a -> a.BytesTransferred)

  let sendAllTo (socket:Socket) (remoteEp:IPEndPoint) (bufs:ArraySegment<byte>[]) =
    exec argsAlloc (fun args -> setBuffers bufs args ; args.RemoteEndPoint <- remoteEp) socket.SendAsync (fun a -> a.BytesTransferred)

  /// Connects to a remote host.
  let connect (socket:Socket) (ep:IPEndPoint) =
    exec argsAlloc (fun a -> a.RemoteEndPoint <- ep) socket.ConnectAsync (fun a -> a.ConnectSocket)

  let disconnect (socket:Socket) (reuse:bool) =
    exec argsAlloc (fun a -> a.DisconnectReuseSocket <- reuse) socket.DisconnectAsync (ignore)

  let receiveStream (socket:Socket) : AsyncSeq<ArraySeg<byte>> =

    let remBuff = ref None

    let inline getBuffer () =
      match !remBuff with
      | Some rem -> rem
      | None -> socket.ReceiveBufferSize |> ArraySeg.ofCount

    let rec go () = asyncSeq {
      let buff = getBuffer ()
      let! received = receive socket buff
      if received = 0 then ()
      else
        let remainder = buff.Count - received
        if remainder > 0 then
          remBuff := Some (ArraySeg.shiftOffset remainder buff)
        else
          remBuff := None
        yield buff |> ArraySeg.setCount received
        yield! go () }

    go ()






// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// framing

/// Stream framing.
module Framing =

  /// 32 bit length prefix framer.
  module LengthPrefix =

    let [<Literal>] HeaderLength = 4

    let inline allocBuffer length : ArraySeg<byte> = ArraySeg.ofCount length

    [<Struct>]
    type private ReadResult =
      val public remainder : ArraySeg<byte>
      val public length : int
      val public headerBytes : int
      new (remainder:ArraySeg<byte>, length:int) = { remainder = remainder ; length = length ; headerBytes = 0 }
      new (remainder:ArraySeg<byte>, length:int, headerBytes:int) = { remainder = remainder ; length = length ; headerBytes = headerBytes }

    let frame (data:ArraySeg<byte>) : ArraySeg<byte>[] = [|
      yield ArraySeg.ofArray (BitConverter.GetBytesBigEndian data.Count)
      yield data |]

    let frameSeq (data:ArraySeg<byte> seq) : ArraySeg<byte>[] = [|
      let length = data |> Seq.sumBy (fun d -> d.Count)
      yield ArraySeg.ofArray (BitConverter.GetBytesBigEndian length)
      yield! data |]

    let private tryReadLength (headerBytes:int) (length:int) (data:ArraySeg<byte>) : ReadResult =
      //printfn "tryReadLength|data.Count=%i data.Offset=%i" data.Count data.Offset
      if data.Count >= HeaderLength then
        let length = BitConverter.ToInt32BigEndian(data.Array, data.Offset)
        let remainder = ArraySeg.setOffset (data.Offset + HeaderLength) data
        ReadResult(remainder, length)
      else
        //printfn "SLOW FRAME!"
        let rec loop  (headerBytes:int) (length:int) (i:int) =
          //printfn "tryReadLength|loop=%i headerBytes=%i length=%i littleEndian=%b" i headerBytes length BitConverter.IsLittleEndian
          if i = data.Offset + data.Count then
            //printfn "tryReadLength|end of data"
            new ReadResult(ArraySeg<_>(), length, headerBytes)
          else
            let length = (int data.Array.[i]) <<< (headerBytes * 8) ||| length // little endian
            let headerBytes = headerBytes + 1
            if headerBytes = HeaderLength then ReadResult(ArraySeg.setOffset (i + 1) data, length)
            else loop headerBytes length (i + 1)
        loop headerBytes length data.Offset

    /// Reads 32 bytes of the length prefix, then the length of the message, then yields, then continues.
    let unframe (s:AsyncSeq<ArraySeg<byte>>) : AsyncSeq<ArraySeg<byte>> =

      let rec go (offset:int) (s:AsyncSeq<ArraySeg<byte>>) =
        readLength 0 0 offset s

      and readLength (headerBytes:int) (length:int) (offset:int) (s:AsyncSeq<ArraySeg<byte>>) =
        s |> Async.bind (function
          | Nil -> async.Return Nil
          | Cons (data,tl) as s ->
            let data =
              if offset > 0 then data |> ArraySeg.shiftOffset (data.Count - offset)
              else data
            let readResult = tryReadLength headerBytes length data
            let tl,offset =
              if readResult.remainder.Count = 0 then tl,0
              else (async.Return s),readResult.remainder.Count
            //printfn "readLength|readResult.length=%i ; remainder=%i headerBytes=%i ; offset=%i" readResult.length readResult.remainder.Count readResult.headerBytes offset
            if readResult.headerBytes = 0 then
              let buffer = allocBuffer readResult.length
              readData readResult.length buffer offset tl
            else
              readLength readResult.headerBytes readResult.length offset tl)

      and readData (length:int) (buffer:ArraySeg<byte>) (offset:int) (s:AsyncSeq<ArraySeg<byte>>) =
        s |> Async.bind (function
          | Nil -> async.Return Nil
          | Cons (data,tl) as s ->
            let data =
              if offset > 0 then data |> ArraySeg.shiftOffset (data.Count - offset)
              else data
            let copy = min data.Count (length - buffer.Offset)
            ArraySeg.copy data buffer copy
            let tl,offset =
              if copy = data.Count then tl,0
              else async.Return s,(data.Count - copy)
            let bufferIndex = buffer.Offset + copy
            //printfn "readData|length=%i data.Count=%i offset=%i copy=%i buffer.Offset=%i bufferIndex=%i" length data.Count offset copy buffer.Offset bufferIndex
            //printfn "readData|buffer=%s" (Encoding.UTF8.GetString buffer)
            if bufferIndex = length then
              let buffer = buffer |> ArraySeg.setOffset 0
              Cons (buffer, go offset tl) |> async.Return
            else
              let buffer = buffer |> ArraySeg.setOffset bufferIndex
              readData length buffer offset tl)

      go 0 s

// -----------------------------------------------------------------------------------------------------------------------------------------------------------






// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// session layer (layer 5)

/// A correlation id.
type CorrelationId = int32

/// A client creates a session message, assigns a unique tx id and waits for a response from the server.
/// A server receives a session message, and sends a response with the same tx id.
[<Struct>]
type SessionMessage =
  val public tx_id : CorrelationId
  val public payload : ArraySeg<byte>
  new (data:ArraySeg<byte>) = { tx_id = BitConverter.ToInt32BigEndian (data.Array, data.Offset) ; payload = ArraySeg.shiftOffset 4 data }
  new (txId, payload) = { tx_id = txId ; payload = payload }

/// A multiplexed request/reply session.
type ReqRepSession<'a, 'b, 's> internal 
  (
    /// A correlation id generator.
    correlationId:unit -> CorrelationId, 
     
    /// Encodes a request with a correlation id into a byte array and a state maintained for the duration of the request.
    encode:'a * CorrelationId -> ArraySeg<byte> * 's, 
     
    /// Decodes a response given the correlatio id, the maintained state and the response byte array.
    decode:CorrelationId * 's * ArraySeg<byte> -> 'b, 
     
    /// A stream of bytes corresponding to the stream received from the remote host.
    receive:AsyncSeq<ArraySeg<byte>>, 
     
    /// Sends a byte array to the remote host.
    send:ArraySeg<byte> -> Async<int>) =

  static let Log = Log.create "KafkaFs.Session"

  let txs = new ConcurrentDictionary<int, 's * TaskCompletionSource<'b>>()
  let cts = new CancellationTokenSource()

  let demux (data:ArraySeg<byte>) =
    let sessionData = SessionMessage(data)
    let correlationId = sessionData.tx_id
    let mutable token = Unchecked.defaultof<_>
    if txs.TryRemove(correlationId, &token) then
      Log.trace "received message. correlation_id=%i size=%i" correlationId sessionData.payload.Count
      let state,reply = token
      let res = decode (correlationId,state,sessionData.payload)
      reply.SetResult res
    else
      Log.error "received message but unabled to find session for correlation_id=%i" correlationId
    
  let mux (req:'a) =
    let correlationId = correlationId ()
    let sessionReq,state = encode (req,correlationId)
    let rep = TaskCompletionSource<_>()
    if not (txs.TryAdd(correlationId, (state,rep))) then
      Log.error "clash of the sessions!"
    correlationId,sessionReq,rep
    
  do
    receive
    |> AsyncSeq.iter demux
    |> Async.tryFinally (fun () -> Log.info "session disconnected.")
    |> (fun t -> Async.Start(t, cts.Token))
    
  member x.Send (req:'a) = async {
    let correlationId,sessionData,rep = mux req
    Log.trace "sending request... correlation_id=%i bytes=%i" correlationId sessionData.Count
    let! sent = send sessionData
    Log.trace "request sent. correlation_id=%i bytes=%i" correlationId sent
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

  let encodeSession (m:SessionMessage) : ArraySeg<byte>[] = [|
    yield BitConverter.GetBytes m.tx_id |> ArraySeg.ofArray
    yield m.payload |]
  
  /// A correlation id factory.
  let corrId : unit -> CorrelationId =
    let id = ref 0
    fun () -> Interlocked.Increment id

  /// Creates a request/reply session listening to the specified input stream and
  /// sending to the specified output.
  let requestReply 
    (correlationId:unit -> int) 
    (encode:'a * int -> ArraySeg<byte> * 's) 
    (decode:int * 's * ArraySeg<byte> -> 'b) 
    (receive:AsyncSeq<ArraySeg<byte>>)
    (send:ArraySeg<byte> -> Async<int>) =
      new ReqRepSession<'a, 'b, 's>(correlationId, encode, decode, receive, send)

// -----------------------------------------------------------------------------------------------------------------------------------------------------------