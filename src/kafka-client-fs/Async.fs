#nowarn "40"
namespace KafkaFs

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic

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
        else failwith "invalid Task state!"
      )
      |> ignore

  let awaitTaskCancellationAsError (t:Task<'a>) : Async<'a> =
    Async.FromContinuations <| fun (ok,err,_) ->
      t.ContinueWith (fun (t:Task<'a>) ->
        if t.IsFaulted then err t.Exception
        elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
        elif t.IsCompleted then ok t.Result
        else failwith "invalid Task state!"
      )
      |> ignore

  let awaitTaskUnitCancellationAsError (t:Task) : Async<unit> =
    Async.FromContinuations <| fun (ok,err,_) ->
      t.ContinueWith (fun (t:Task) ->
        if t.IsFaulted then err t.Exception
        elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
        elif t.IsCompleted then ok ()
        else failwith "invalid Task state!"
      )
      |> ignore


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