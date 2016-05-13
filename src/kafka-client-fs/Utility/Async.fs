#nowarn "40"
namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

open Kafunk.Prelude

type internal MbReq<'a> =
  | Put of 'a
  | Take of AsyncReplyChannel<'a>

/// An unbounded FIFO mailbox.
type Mb<'a> internal () =

  let agent = MailboxProcessor.Start <| fun agent ->

    let queue = new Queue<_>()

    let rec loop () = async {
      match queue.Count with
      | 0 -> do! tryReceive ()
      | _ -> do! trySendOrReceive ()
      return! loop () }

    and tryReceive () =
      agent.Scan (function
        | Put (a) -> Some (receive(a))
        | _ -> None)

    and receive (a:'a) = async {
      queue.Enqueue a }

    and send (rep:AsyncReplyChannel<'a>) = async {
      let a = queue.Dequeue ()
      rep.Reply a }

    and trySendOrReceive () = async {
      let! msg = agent.Receive ()
      match msg with
      | Put a -> return! receive a
      | Take rep -> return! send rep }

    loop ()

  member __.Put (a:'a) =
    agent.Post (Put a)

  member __.Take =
    agent.PostAndAsyncReply (fun ch -> Take ch)

  interface IDisposable with
    member __.Dispose () = (agent :> IDisposable).Dispose()


/// Operations on unbounded FIFO mailboxes.
module Mb =

  /// Creates a new unbounded mailbox.
  let create () = new Mb<'a> ()

  /// Puts a message into a mailbox, no waiting.
  let inline put (a:'a) (mb:Mb<'a>) = mb.Put a

  /// Creates an async computation that completes when a message is available in a mailbox.
  let inline take (mb:Mb<'a>) = mb.Take

type AsyncCh<'a> () =

  let readers = ConcurrentQueue<'a -> unit>()
  let writers = ConcurrentQueue<'a * (unit -> unit)>()
  let spawn k = ThreadPool.QueueUserWorkItem(fun _ -> k ()) |> ignore

  member __.Take readerOk =
    let w = ref Unchecked.defaultof<_>
    if writers.TryDequeue w then
      let (value,writerOk) = !w
      spawn writerOk
      readerOk value
    else
      readers.Enqueue readerOk

  member __.Fill (x:'a, writerOk:unit -> unit) =
    let r = ref Unchecked.defaultof<_>
    if readers.TryDequeue r then
      let readerOk = !r
      spawn writerOk
      readerOk x
    else
      writers.Enqueue(x, writerOk)

  /// Creates an async computation which completes when a value is available in the channel.
  member inline this.Take() =
    Async.FromContinuations <| fun (ok, _, _) -> this.Take ok

  /// Creates an async computation which completes when the provided value is read from the channel.
  member inline this.Fill x =
    Async.FromContinuations <| fun (ok, _, _) -> this.Fill(x, ok)

  /// Queues a write to the channel.
  member this.EnqueueFill x =
    writers.Enqueue(x, id)


/// Operations on channels.
module AsyncCh =

  /// Creates an empty channel.
  let inline create() = AsyncCh<'a>()

  /// Creates a channel initialized with a value.
  let inline createFull a =
    let ch = create()
    ch.EnqueueFill a
    ch

  /// Creates an async computation which completes when a value is available in the channel.
  let inline take (ch:AsyncCh<'a>) = ch.Take()

  /// Blocks until a value is available in the channel.
  let inline takeNow (ch:AsyncCh<'a>) = take ch |> Async.RunSynchronously

  /// Creates an async computation which completes when the provided value is read from the channel.
  let inline fill a (ch:AsyncCh<'a>) = ch.Fill a

  /// Starts an async computation which writes a value to a channel.
  let inline fillNow a (ch:AsyncCh<'a>) = fill a ch |> Async.Start

  /// Creates an async computation which completes when the provided value is read from the channel.
  let inline enqueueFill a (ch:AsyncCh<'a>) = ch.EnqueueFill a







[<AutoOpen>]
module AsyncEx =

  let empty : Async<unit> = async.Return()

  let never : Async<unit> = Async.Sleep Timeout.Infinite

  let awaitTaskUnit (t:Task) =
    Async.FromContinuations <| fun (ok,err,cnc) ->
      t.ContinueWith(fun t ->
        if t.IsFaulted then err(t.Exception)
        elif t.IsCanceled then cnc(OperationCanceledException("Task wrapped with Async.AwaitTask has been cancelled.",  t.Exception))
        elif t.IsCompleted then ok()
        else failwith "invalid Task state!") |> ignore

  let awaitTaskCancellationAsError (t:Task<'a>) : Async<'a> =
    Async.FromContinuations <| fun (ok,err,_) ->
      t.ContinueWith (fun (t:Task<'a>) ->
        if t.IsFaulted then err t.Exception
        elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
        elif t.IsCompleted then ok t.Result
        else failwith "invalid Task state!") |> ignore

  let awaitTaskUnitCancellationAsError (t:Task) : Async<unit> =
    Async.FromContinuations <| fun (ok,err,_) ->
      t.ContinueWith (fun (t:Task) ->
        if t.IsFaulted then err t.Exception
        elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
        elif t.IsCompleted then ok ()
        else failwith "invalid Task state!") |> ignore


  type Async with

    /// An async computation which does nothing and completes immediately.
    static member inline empty = empty

    /// An async computation which does nothing and never completes.
    static member inline never = never

    static member map (f:'a -> 'b) (a:Async<'a>) : Async<'b> = async.Bind(a, f >> async.Return)

    static member inline bind (f:'a -> Async<'b>) (a:Async<'a>) : Async<'b> = async.Bind(a, f)

    static member inline join (a:Async<Async<'a>>) : Async<'a> = Async.bind id a

    static member map2 (a:Async<'a>) (b:Async<'b>) (f:'a * 'b -> 'c) = Async.Parallel (a,b) |> Async.map f

    static member inline tryFinally (compensation:unit -> unit) (a:Async<'a>) : Async<'a> =
      async.TryFinally(a, compensation)

    static member inline tryFinallyDispose (d:#IDisposable) (a:Async<'a>) : Async<'a> =
      Async.tryFinally (fun () -> d.Dispose()) a

    static member inline tryFinallyDisposeAll (ds:#IDisposable seq) (a:Async<'a>) : Async<'a> =
      Async.tryFinally (fun () -> ds |> Seq.iter (fun d -> d.Dispose())) a

    static member inline tryCancelled comp a = Async.TryCancelled(a, comp)

    static member inline tryWith h a = async.TryWith(a, h)

    /// Returns an async computation which will wait for the given task to complete.
    static member inline AwaitTask (t:Task) = awaitTaskUnit t

    /// Returns an async computation which will wait for the given task to complete and returns its result.
    /// Task cancellations are propagated as exceptions so that they can be trapped.
    static member inline AwaitTaskCancellationAsError (t:Task<'a>) : Async<'a> = awaitTaskCancellationAsError t

    /// Returns an async computation which will wait for the given task to complete and returns its result.
    /// Task cancellations are propagated as exceptions so that they can be trapped.
    static member inline AwaitTaskCancellationAsError (t:Task) : Async<unit> = awaitTaskUnitCancellationAsError t

    /// Like Async.StartWithContinuations but starts the computation on a ThreadPool thread.
    static member StartThreadPoolWithContinuations (a:Async<'a>, ok:'a -> unit, err:exn -> unit, cnc:OperationCanceledException -> unit, ?ct:CancellationToken) =
      let a = Async.SwitchToThreadPool () |> Async.bind (fun _ -> a)
      Async.StartWithContinuations (a, ok, err, cnc, defaultArg ct CancellationToken.None)

    static member Parallel (c1, c2) : Async<'a * 'b> = async {
        let! c1 = c1 |> Async.StartChild
        let! c2 = c2 |> Async.StartChild
        let! c1 = c1
        let! c2 = c2
        return c1,c2 }

    static member Parallel (c1:Async<unit>, c2:Async<unit>) : Async<unit> = async {
        let! c1 = c1 |> Async.StartChild
        let! c2 = c2 |> Async.StartChild
        do! c1
        do! c2 }

    static member Parallel (c1, c2, c3) : Async<'a * 'b * 'c> = async {
        let! c1 = c1 |> Async.StartChild
        let! c2 = c2 |> Async.StartChild
        let! c3 = c3 |> Async.StartChild
        let! c1 = c1
        let! c2 = c2
        let! c3 = c3
        return c1,c2,c3 }

    static member Parallel (c1, c2, c3, c4) : Async<'a * 'b * 'c * 'd> = async {
        let! c1 = c1 |> Async.StartChild
        let! c2 = c2 |> Async.StartChild
        let! c3 = c3 |> Async.StartChild
        let! c4 = c4 |> Async.StartChild
        let! c1 = c1
        let! c2 = c2
        let! c3 = c3
        let! c4 = c4
        return c1,c2,c3,c4 }

    /// Creates an async computation which runs the provided sequence of computations and completes
    /// when all computations in the sequence complete. Up to parallelism computations will
    /// be in-flight at any given point in time. Error or cancellation of any computation in
    /// the sequence causes the resulting computation to error or cancel, respectively.
    static member ParallelIgnoreCT (ct:CancellationToken) (parallelism:int) (xs:seq<Async<_>>) = async {
      let sm = new SemaphoreSlim(parallelism)
      let cde = new CountdownEvent(1)
      let tcs = new TaskCompletionSource<unit>()
      ct.Register(Action(fun () -> tcs.TrySetCanceled() |> ignore)) |> ignore
      let inline tryComplete () =
        if cde.Signal() then
          tcs.SetResult(())
      let inline ok _ =
        sm.Release() |> ignore
        tryComplete ()
      let inline err (ex:exn) =
        tcs.TrySetException ex |> ignore
        sm.Release() |> ignore
      let inline cnc (_:OperationCanceledException) =
        tcs.TrySetCanceled() |> ignore
        sm.Release() |> ignore
      try
        use en = xs.GetEnumerator()
        while not (tcs.Task.IsCompleted) && en.MoveNext() do
          sm.Wait()
          cde.AddCount(1)
          Async.StartWithContinuations (en.Current, ok, err, cnc, ct)
        tryComplete ()
        do! tcs.Task |> Async.AwaitTask
      finally
        cde.Dispose()
        sm.Dispose() }

    /// Creates an async computation which runs the provided sequence of computations and completes
    /// when all computations in the sequence complete. Up to parallelism computations will
    /// be in-flight at any given point in time. Error or cancellation of any computation in
    /// the sequence causes the resulting computation to error or cancel, respectively.
    static member ParallelIgnore (parallelism:int) (xs:seq<Async<_>>) =
      Async.ParallelIgnoreCT CancellationToken.None parallelism xs

    /// Creates an async computation which runs the provided sequence of computations and completes
    /// when all computations in the sequence complete. Up to parallelism computations will
    /// be in-flight at any given point in time. Error or cancellation of any computation in
    /// the sequence causes the resulting computation to error or cancel, respectively.
    /// Like Async.Parallel but with support for throttling.
    /// Note that an array is allocated to contain the results of all computations.
    static member ParallelThrottled (parallelism:int) (xs:seq<Async<'a>>) : Async<'a[]> = async {
      let rec comps  = xs |> Seq.toArray |> Array.mapi (fun i -> Async.map (fun a -> Array.set results i a))
      and results = Array.zeroCreate comps.Length
      do! Async.ParallelIgnore parallelism comps
      return results }

    /// Retries an async computation.
    static member retryBackoff (attempts:int) (filter:exn -> bool) (backoff:int -> int option) (a:Async<'a>) =
      let rec go i = async {
        try
          let! res = a
          return res
        with ex ->
          if (filter ex = false) then return raise (new Exception("Retry attempt exception filtered.", ex))
          elif (i = attempts) then return raise (new Exception(sprintf "Retry failed after %i attempts." i, ex))
          else
            match backoff i with
            | Some timeoutMs when timeoutMs > 0 -> do! Async.Sleep timeoutMs
            | _ -> ()
            return! go (i + 1) }
      go 1


    /// Retries an async computation.
    static member retryAllBackoff (attempts:int) (backoff:int -> int option) (a:Async<'a>) =
      Async.retryBackoff attempts (konst true) backoff a


    /// Retries an async computation.
    static member retryTimeout (attempts:int) (filter:exn -> bool) (timeoutMs:int) (a:Async<'a>) = async {
        try
            let! res = a
            return res
        with ex ->
            if (filter ex = false) then return raise (new Exception("Retry attempt exception filtered.", ex))
            elif attempts = 0 then return raise (new Exception("Retry failed after several attempts.", ex))
            else
                if timeoutMs > 0 then do! Async.Sleep timeoutMs
                return! Async.retryTimeout (attempts - 1) filter timeoutMs a
    }

    /// Retries an async computation when exceptions match the specified filter.
    static member retry (attempts:int) (filter:exn -> bool) (a:Async<'a>) = Async.retryTimeout attempts filter 0 a

    /// Retries an async computation given any exception.
    static member retryAll (attempts:int) (a:Async<'a>) = Async.retry attempts (fun _ -> true) a

    /// Starts the specified operation using a new CancellationToken and returns
    /// IDisposable object that cancels the computation. This method can be used
    /// when implementing the Subscribe method of IObservable interface.
    static member StartDisposable (op:Async<unit>) =
        let ct = new System.Threading.CancellationTokenSource()
        Async.Start(op, ct.Token)
        { new IDisposable with member x.Dispose() = ct.Cancel() }

    /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
    /// by the specified timeout.
    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) = async {
        let! r = Async.StartChild(c, (int)timeout.TotalMilliseconds)
        return! r }

    /// Creates a computation which returns the result of the first computation that
    /// produces a value as well as a handle to the other computation. The other
    /// computation will be memoized.
    static member chooseBoth (a:Async<'a>) (b:Async<'a>) : Async<'a * Async<'a>> =
      Async.FromContinuations <| fun (ok,err,cnc) ->
        let state = ref 0
        let iv = new TaskCompletionSource<_>()
        let inline ok a =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then
            ok (a, iv.Task |> Async.AwaitTask)
          else
            iv.SetResult a
        let inline err (ex:exn) =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then err ex
          else iv.SetException ex
        let inline cnc ex =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then cnc ex
          else iv.SetCanceled ()
        Async.StartThreadPoolWithContinuations (a, ok, err, cnc)
        Async.StartThreadPoolWithContinuations (b, ok, err, cnc)

    static member chooseTasks (a:Task<'a>) (b:Task<'a>) : Async<'a * Task<'a>> = async {
      let! ct = Async.CancellationToken
      let i = Task.WaitAny([| (a :> Task) ; (b :> Task) |], ct)
      if i = 0 then return (a.Result, b)
      elif i = 1 then return (b.Result, a)
      else return! failwith (sprintf "unreachable, i = %d" i) }

    /// Creates a computation which produces a tuple consiting of the value produces by the first
    /// argument computation to complete and a handle to the other computation. The second computation
    /// to complete is memoized.
    static member internal chooseBothAny (a:Async<'a>) (b:Async<'b>) : Async<Choice<'a * Async<'b>, 'b * Async<'a>>> =
      Async.chooseBoth (a |> Async.map Choice1Of2) (b |> Async.map Choice2Of2)
      |> Async.map (fun (first,second) ->
        match first with
        | Choice1Of2 a -> (a,(second |> Async.map (function Choice2Of2 b -> b | _ -> failwith "invalid state"))) |> Choice1Of2
        | Choice2Of2 b -> (b,(second |> Async.map (function Choice1Of2 a -> a | _ -> failwith "invalid state"))) |> Choice2Of2
      )

    /// Creates an async computation which completes when any of the argument computations completes.
    /// The other argument computation is cancelled.
    static member choose (a:Async<'a>) (b:Async<'a>) : Async<'a> =
      Async.FromContinuations <| fun (ok,err,cnc) ->
        let state = ref 0
        let cts = new CancellationTokenSource()
        let inline cancel () =
          cts.Cancel()
          cts.Dispose()
        let inline ok a =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then ok a
          else cancel ()
        let inline err (ex:exn) =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then err ex
          else cancel ()
        let inline cnc ex =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then cnc ex
          else cancel ()
        Async.StartThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
        Async.StartThreadPoolWithContinuations (b, ok, err, cnc, cts.Token)

    /// Converts an async computation returning a Choice where Choice1Of2 represents Success
    /// and Choice2Of2 represents failure such that failures are raised as exceptions.
    static member throwMap (f:'e -> exn) (a:Async<Choice<'a, 'e>>) : Async<'a> = async {
      let! r = a
      match r with
      | Choice1Of2 a -> return a
      | Choice2Of2 ex -> return raise (f ex) }


module AsyncChoice =

  let mapSuccessAsync (f:'a -> Async<'c>) (c:Choice<'a, 'b>) : Async<Choice<'c, 'b>> =
    match c with
    | Choice1Of2 a -> f a |> Async.map Choice1Of2
    | Choice2Of2 b -> async.Return (Choice2Of2 b)

  let mapSuccess (f:'a -> 'c) (a:Async<Choice<'a, 'b>>) : Async<Choice<'c, 'b>> =
    a |> Async.map (Choice.mapSuccess f)

  let bindSuccess (f:'a -> Async<'c>) (a:Async<Choice<'a, 'b>>) : Async<Choice<'c, 'b>> =
    a |> Async.bind (function
      | Choice1Of2 a -> f a |> Async.map Choice1Of2
      | Choice2Of2 b -> async.Return (Choice2Of2 b))

  let mapError (f:'b -> 'c) (a:Async<Choice<'a, 'b>>) : Async<Choice<'a, 'c>> =
    a |> Async.map (Choice.mapError f)

  let bindError (f:'b -> Async<'c>) (a:Async<Choice<'a, 'b>>) : Async<Choice<'a, 'c>> =
    a |> Async.bind (function
      | Choice1Of2 a -> async.Return (Choice1Of2 a)
      | Choice2Of2 b -> f b |> Async.map Choice2Of2)




type IVar<'a> = TaskCompletionSource<'a>

module IVar =

  let create<'a> = IVar<'a>()

  let createFull (a:'a) : IVar<'a> =
    let iv = create<'a>
    iv.SetResult(a)
    iv

  let put (a:'a) (iv:IVar<'a>) =
    iv.SetResult a |> ignore

  let error (e:exn) (iv:IVar<'a>) =
    iv.SetException e |> ignore

  let get (iv:IVar<'a>) : Async<'a> =
    iv.Task |> Async.AwaitTask





type AsyncObs<'a> = 'a option -> Async<unit>

module AsyncObs =

  let mapIn (_f:'b option -> 'a) : AsyncObs<'a> -> AsyncObs<'b> =
    failwith ""

  let tryFinallyAsync (f:Async<unit>) (o:AsyncObs<'a>) : AsyncObs<'a> =
    function
    | Some a -> o (Some a)
    | None -> o (None) |> Async.bind (fun _ -> f)

  let tryFinally (comp:unit -> unit) (o:AsyncObs<'a>) : AsyncObs<'a> =
    tryFinallyAsync (async { do comp () }) o




type AsyncEvt<'a> = AsyncObs<'a> -> Async<unit>

module AsyncEvt =

  open System.Threading

  let internal create (f:AsyncObs<'a> -> Async<unit>) : AsyncEvt<'a> =
    f

  let internal apply (xs:AsyncEvt<'a>) (obs:AsyncObs<'a>) : Async<unit> =
    xs obs

  let internal delayAsync (xs:Async<AsyncEvt<'a>>) : AsyncEvt<'a> =
    create <| fun obs -> async {
      let! xs = xs
      do! apply xs obs }

  let foldAsync (f:'b -> 'a -> Async<'b>) (z:'b) (xs:AsyncEvt<'a>) : Async<'b> = async {
    let tcs = TaskCompletionSource<'b>()
    let mutable b = z
    do! xs (function
      | Some a -> f b a |> Async.map (fun b' -> Interlocked.Exchange(&b, b') |> ignore)
      | None -> tcs.SetResult(b) ; Async.empty)
    return! tcs.Task |> Async.AwaitTask }

  let unfoldAsync (f:'s -> Async<('a * 's) option>) (s:'s) : AsyncEvt<'a> =
    create (fun obs -> async {
      let rec go (s:'s) =
        f s
        |> Async.bind (function
          | Some (a,s') ->
            Async.Parallel (obs (Some a), go s')
          | None ->
            obs None)
      return! go s })

  let mapObs (f:AsyncObs<'b> -> AsyncObs<'a>) (xs:AsyncEvt<'a>) : AsyncEvt<'b> =
    create (fun obsA -> f obsA |> xs)

  let append (xs1:AsyncEvt<'a>) (xs2:AsyncEvt<'a>) : AsyncEvt<'a> =
    create (fun obs -> apply xs1 (function Some a -> obs (Some a) | None -> apply xs2 obs))

  let empty<'a> : AsyncEvt<'a> =
    create ((|>) None)

  let tryPick (f:'a -> Async<'b option>) (xs:AsyncEvt<'a>) : Async<'b option> = async {
    let tcs = IVar.create
    do! apply xs (function
      | Some a -> async {
        let! b = f a
        match b with
        | Some b -> IVar.put (Some b) tcs
        | None -> ()
        return () }
      | None -> async { IVar.put None tcs })
    return! tcs.Task |> Async.AwaitTask }

  let take (count:int) (xs:AsyncEvt<'a>) : AsyncEvt<'a> =
    create <| fun obs ->
      let mutable i = 0
      apply xs (function
        | Some a -> async {
          do! obs (Some a)
          if Interlocked.Increment &i = count then
            do! obs None }
        | None -> async {
          do! obs None })

  let singleton (a:'a) : AsyncEvt<'a> =
    create (fun obs -> obs (Some a))

  let singletonAsync (a:Async<'a>) : AsyncEvt<'a> =
    delayAsync (a |> Async.map singleton)

  let cons (a:'a) (xs:AsyncEvt<'a>) =
    append (singleton a) (xs)

  let interval (periodMs:int) : AsyncEvt<DateTime> =
    create <| fun obs -> async {
      while true do
        do! Async.Sleep periodMs
        do! obs (Some (DateTime.UtcNow)) }





type AsyncObs<'a, 'b> = 'a option -> Async<'b option>

type AsyncPipe<'a, 'b> = AsyncObs<'a, 'b> -> Async<'b option>

type AsyncPipe<'a> = AsyncPipe<'a, unit>

module AysncPipe =

  let internal create (f:AsyncObs<'a, 'b> -> Async<'b option>) : AsyncPipe<'a, 'b> =
    f

  let internal apply (xs:AsyncPipe<'a, 'b>) (obs:AsyncObs<'a, 'b>) : Async<'b option> =
    xs obs

  let unfoldAsync (f:'s -> Async<('a * 's) option>) (s:'s) : AsyncPipe<'a, 'b> =
    create (fun obs -> async {
      let rec go (s:'s) =
        f s
        |> Async.bind (function
          | Some (a,s') -> async {
            let! r = obs (Some a)
            match r with
            | Some b -> return Some b
            | None -> return! go s' }
          | None ->
            obs None)
      return! go s })









type AsyncEvtSrc<'a> = AsyncEvtSrc of IVar<('a * AsyncEvtSrc<'a>) option>

module AsyncEvtSrc =

  let inline un (AsyncEvtSrc(iv)) = iv

  let create<'a> : AsyncEvtSrc<'a> =
    AsyncEvtSrc (IVar.create)

  let createFull (a:'a) : AsyncEvtSrc<'a> =
    AsyncEvtSrc (IVar.createFull (Some (a, create)))

  let put (a:'a) (xs:AsyncEvtSrc<'a>) : unit =
    (un xs) |> IVar.put (Some (a, create))

  let error (e:exn) (xs:AsyncEvtSrc<'a>) : unit =
    (un xs) |> IVar.error e

  let close (xs:AsyncEvtSrc<'a>) : unit =
    (un xs) |> IVar.put None

  let asAsyncEvt (xs:AsyncEvtSrc<'a>) : AsyncEvt<'a> =
    AsyncEvt.create (fun obs ->
      let rec go xs =
        xs
        |> un
        |> IVar.get
        |> Async.bind (function
          | Some (hd,tl) ->
            obs (Some hd) |> Async.bind (fun _ -> go tl)
          | None -> obs None)
      go xs)

  let groupByAsync (f:'a -> Async<'k>) (xs:AsyncEvt<'a>) : AsyncEvt<'k * AsyncEvt<'a>> =
    AsyncEvt.create <| fun obs -> async {
      let groups = ConcurrentDictionary<'k, AsyncEvtSrc<'a>>()
      do! AsyncEvt.apply xs (function
        | Some a -> async {
          let! k = f a
          let mutable evtSrc : AsyncEvtSrc<'a> = Unchecked.defaultof<_>
          if groups.TryGetValue(k, &evtSrc) then
            put a evtSrc
          else
            evtSrc <- createFull a
            if groups.TryAdd (k, evtSrc) then
              do! obs (Some (k, asAsyncEvt evtSrc)) }
        | None ->
          groups.Values |> Seq.iter close
          Async.empty) }

  let bufferByCountAndTime (_count:int) (_timeMs:int) (_xs:AsyncEvt<'a>) : AsyncEvt<'a[]> =
    AsyncEvt.create <| fun _obs -> async {
        return ()
      }

  let tryFirstAsync (xs:AsyncEvt<'a>) : Async<'a option> = async {
    let iv = IVar.create
    do! AsyncEvt.apply xs (function
      | Some a ->  IVar.put (Some a) iv |> async.Return
      | None -> IVar.put None iv |> async.Return)
    return! IVar.get iv }

  let tryLastAsync (xs:AsyncEvt<'a>) : Async<'a option> = async {
    let mutable last = None
    let iv = IVar.create
    do! AsyncEvt.apply xs (function
      | Some a -> Interlocked.Exchange(&last, Some a) |> ignore |> async.Return
      | None ->
        match last with
        | None -> IVar.put None iv |> async.Return
        | Some a -> IVar.put (Some a) iv |> async.Return)
    return! IVar.get iv }

  let scanAsync (f:'b -> 'a -> Async<'b>) (z:'b) (xs:AsyncEvt<'a>) : AsyncEvt<'b> =
    AsyncEvt.delayAsync <| async {
      let src = create
      let mutable b = z
      do! AsyncEvt.apply xs (function
        | Some a -> async {
          let! b' = f b a
          b <- b'
          put b' src }
        | None -> async {
          close src })
      return asAsyncEvt src }

  let tryFinally (comp:unit -> unit) (xs:AsyncEvt<'a>) : AsyncEvt<'a> =
    xs |> AsyncEvt.mapObs (AsyncObs.tryFinally comp)

  let iterAsync (f:'a -> Async<unit>) (xs:AsyncEvt<'a>) : Async<unit> =
    xs (function Some a -> f a | None -> Async.empty)





  type AsyncStream<'a> = Async<AsyncStreamCons<'a>>

  and AsyncStream<'a, 'b> = Async<AsyncStreamCons<'a, 'b>>

  and AsyncStreamCons<'a> = AsyncStreamCons of 'a * AsyncStream<'a>

  and AsyncStreamCons<'a, 'b> = AsyncStreamCons2 of 'a * AsyncStream<'b>

  module AsyncStreamCons =

    let inline head (AsyncStreamCons (a,_)) = a

    let inline tail (AsyncStreamCons (_,tl)) = tl

    let rec repeat a =
      AsyncStreamCons (a, async.Delay (fun () -> async.Return (repeat a)))

    let rec repeatAsync a : AsyncStream<'a> =
      a |> Async.map (fun a' -> AsyncStreamCons (a', repeatAsync a))

    let rec map (f:'a -> 'b) (AsyncStreamCons (a,tl)) =
      AsyncStreamCons (f a, tl |> Async.map (map f))

    let rec mapAsync (f:'a -> Async<'b>) (AsyncStreamCons (a,tl)) =
      f a |> Async.map (fun b -> AsyncStreamCons (b, tl |> Async.bind (mapAsync f)))

    let rec unfold (f:'s -> ('a * 's)) (s:'s) : AsyncStreamCons<'a> =
      let a,s' = f s in
      AsyncStreamCons (a, async.Delay (fun () -> async.Return (unfold f s')))

    let rec unfoldAsync (f:'s -> Async<('a * 's)>) (s:'s) : AsyncStream<'a> =
      f s |> Async.map (fun (a,s') -> AsyncStreamCons (a, async.Delay (fun () -> unfoldAsync f s')))

    let rec iterAsync (f:'a -> Async<unit>) (AsyncStreamCons (a,tl)) : Async<unit> =
      f a |> Async.bind (fun _ -> tl |> Async.bind (iterAsync f))

    let rec chooseAsync (f:'a -> Async<'b option>) (AsyncStreamCons (a,tl)) : AsyncStream<'b> =
      f a |> Async.bind (function
        | Some b -> AsyncStreamCons (b, tl |> Async.bind (chooseAsync f)) |> async.Return
        | None -> tl |> Async.bind (chooseAsync f))

    let rec pickAsync (f:'a -> Async<'b option>) (AsyncStreamCons (a,tl)) : Async<'b * AsyncStream<'a>> =
      f a |> Async.bind (function
        | Some b -> async.Return (b, tl)
        | None -> tl |> Async.bind (pickAsync f))

    let rec zapAsync (AsyncStreamCons (f,tlf)) (AsyncStreamCons (a,tla)) : AsyncStreamCons<'b> =
      AsyncStreamCons (f a, Async.Parallel (tlf,tla) |> Async.map ((<||) zapAsync))

    let rec zipAsync (AsyncStreamCons (a,tla)) (AsyncStreamCons (b,tlb)) : AsyncStreamCons<'a * 'b> =
      AsyncStreamCons ((a,b), Async.Parallel (tla,tlb) |> Async.map ((<||) zipAsync))

//
//module AsyncStream =
//
//  let whenever (a:AsyncStream<'a>) (tf:AsyncStream<bool>) : AsyncStream<'a> =
//    failwith ""





  type AsyncAlt<'a> =
    private
    | Now of 'a
    | Never
    | AwaitComp of Async<'a> * (unit -> unit)
    | Await of Async<'a>

  module AsyncAlt =

    [<GeneralizableValue>]
    let never<'a> : AsyncAlt<'a> =
      // activate all / commit one / undo others
      Never

    let now (a:'a) : AsyncAlt<'a> =
      Now a

    /// Creates an async alternative which evaluates the async computation
    /// produced by a function which accepts a nack promise as an argument
    /// which completes when a different alternative is committed to in a
    /// synchronization.
    let withNackAsync (f:Async<unit> -> Async<'a>) : AsyncAlt<'a> =
      let tcs = TaskCompletionSource<unit>()
      let a = f (tcs.Task |> Async.AwaitTask)
      let comp () = tcs.SetResult()
      AwaitComp (a, comp)

    ///
    let ofAsync (a:Async<'a>) : AsyncAlt<'a> =
      withNackAsync <| fun (nack:Async<unit>) ->
        let tcs = TaskCompletionSource<'a>()
        let cts = new CancellationTokenSource()
        let op = async {
          try
            let! a = a
            tcs.SetResult a
          with ex ->
            tcs.SetException ex }
        Async.Start (op, cts.Token)
        nack
        |> Async.map (fun _ -> cts.Cancel() ; cts.Dispose())
        |> Async.StartChild
        |> Async.bind (fun _ ->
          tcs.Task
          |> Async.AwaitTask
          |> (fun a -> async.TryFinally(a, fun () -> cts.Dispose())))

    type ChMsg<'a> =
      | Take of AsyncReplyChannel<'a>
      | Give of 'a * AsyncReplyChannel<'a>



    let distAsyncOpt (ac:Async<'a option>) : Async<'a> option =
      match ac |> Async.RunSynchronously with
      | Some a -> Some (async.Return a)
      | None -> None

    let distOptAsync (oa:Async<'a> option) : Async<'a option> = async {
      match oa with
      | Some oa -> return! oa |> Async.map Some
      | None -> return None }

    let distChoiceAsync (ca:Choice<Async<'a>, Async<'b>>) : Async<Choice<'a, 'b>> = async {
      match ca with
      | Choice1Of2 a -> return! a |> Async.map Choice1Of2
      | Choice2Of2 b -> return! b |> Async.map Choice2Of2 }

    let distTupleAsync (a:Async<'a>, b:Async<'b>) : Async<'a * 'b> =
      Async.Parallel (a,b)








    // prepare/commit
    let commit (_a:Async<Async<unit> option>) =
      failwith ""

//    let ch<'a> =
//      // Take : Ch<'a> -> AsyncAlt<'a> / Give : Ch<'a> -> 'a -> AsyncAlt<unit>
//      let agent =
//        MailboxProcessor.Start <| fun agent -> async {
//
//            let rec loop () = async {
//              let! msg = agent.Receive()
//              match msg with
//              | Take reply -> }
//
//            let rec full (a:'a) =
//              agent.Scan (function
//                | Take reply -> Some (async { reply.Reply a })
//                | _ -> None)
//
//            and empty (repply:) =
//              agent.Scan (function
//                | Give (a,reply) ->
//
//                )
//
//            //
//            return () }
//      ()



//    let choose (a:AsyncAlt<'a>) (b:AsyncAlt<'a>) : AsyncAlt<'a> =
//      match a,b with
//      | Now a, _ -> AsyncAlt.Now a
//      | _, Now b -> AsyncAlt.Now b
//      | Never, Never -> AsyncAlt.Never
//      | Never, b -> b
//      | a, Never -> a
//      | Await a, Await b -> AsyncAlt.Await (Async.choose a b)
//      | Await a, AwaitComp (b,comp) ->
//        let await () = async {
//          let! chosen : Async<'a> = Async.choose a b }






//        failwith ""
//      | AwaitComp (a,comp), Await b ->
//        failwith ""
//      | AwaitComp (a,comp1), AwaitComp (b,comp2) ->
//        failwith ""
