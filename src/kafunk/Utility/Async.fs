#nowarn "40"
namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

open Kafunk


[<AutoOpen>]
module AsyncEx =

  let empty : Async<unit> = async.Return()

  let never : Async<unit> = 
    Async.Sleep Timeout.Infinite

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
    static member inline AwaitTaskCancellationAsError (t:Task<'a>) : Async<'a> = 
      awaitTaskCancellationAsError t

    /// Returns an async computation which will wait for the given task to complete and returns its result.
    /// Task cancellations are propagated as exceptions so that they can be trapped.
    static member inline AwaitTaskCancellationAsError (t:Task) : Async<unit> = 
      awaitTaskUnitCancellationAsError t

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
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            cancel ()
            ok a
        let inline err (ex:exn) =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            cancel ()
            err ex
        let inline cnc ex =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            cancel ()
            cnc ex
        Async.StartThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
        Async.StartThreadPoolWithContinuations (b, ok, err, cnc, cts.Token)

    /// Associates an async computation to a cancellation token.
    static member withCancellationToken (ct:CancellationToken) (a:Async<'a>) : Async<'a> =
      Async.FromContinuations (fun (ok,err,cnc) -> Async.StartThreadPoolWithContinuations(a, ok, err, cnc, ct))

    static member Throw (a:Async<Choice<'a, exn>>) : Async<'a> =
      async {
        let! r = a
        match r with
        | Choice1Of2 a -> return a
        | Choice2Of2 e -> return raise e }

    static member timeoutWith (f:unit -> 'a) (timeout:TimeSpan) (c:Async<'a>) : Async<'a> =
      let timeout = async {
        do! Async.Sleep (int timeout.TotalMilliseconds)
        return f () }
      Async.choose c timeout

    static member timeoutResultWith (f:unit -> 'e) (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, 'e>> =
      Async.timeoutWith (f >> Failure) timeout (c |> Async.map Success)

    static member timeoutResult (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, TimeoutException>> =
      Async.timeoutResultWith (fun () -> TimeoutException(sprintf "The operation timed out after %fsec" timeout.TotalSeconds)) timeout c

    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) =
      Async.timeoutResult timeout c 
      |> Async.map (Result.throw)



module AsyncFunc =
  
  let catch (f:'a -> Async<'b>) : 'a -> Async<Result<'b, exn>> =
    f >> Async.Catch

  let dimap (g:'c -> 'a) (h:'b -> 'd) (f:'a -> Async<'b>) : 'c -> Async<'d> =
    g >> f >> Async.map h

  let mapInput (g:'c -> 'a) (f:'a -> Async<'b>) : 'c -> Async<'b> =
    g >> f

  let mapOut (h:'a * 'b -> 'c) (f:'a -> Async<'b>) : 'a -> Async<'c> =
    fun a -> Async.map (fun b -> h (a,b)) (f a)

  let mapOutAsync (h:'a * 'b -> Async<'c>) (f:'a -> Async<'b>) : 'a -> Async<'c> =
    fun a -> Async.bind (fun b -> h (a,b)) (f a)

  let doBeforeAfter (before:'a -> unit) (after:'a * 'b -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      do before a
      let! b = f a
      do after (a,b)
      return b }

  let doBeforeAfterExn (before:'a -> unit) (after:'a * 'b -> unit) (error:'a * exn -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      do before a
      try
        let! b = f a
        do after (a,b)
        return b
      with ex ->
        error (a,ex)
        return raise ex }

  let doExn (error:'a * exn -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      try return! f a
      with ex ->
        error (a,ex)
        return raise ex }

  let timeout (t:TimeSpan) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async.Delay (fun () -> f a) |> Async.timeoutAfter t

  let timeoutResult (t:TimeSpan) (f:'a -> Async<'b>) : 'a -> Async<Result<'b, TimeoutException>> =
    fun a -> async.Delay (fun () -> f a) |> Async.timeoutResult t 
  







type Mb<'a> = MailboxProcessor<'a>

/// Operations on unbounded FIFO mailboxes.
module Mb =

  /// Creates a new unbounded mailbox.
  let create () : Mb<'a> = 
    MailboxProcessor.Start (fun _ -> async.Return())

  /// Puts a message into a mailbox, no waiting.
  let inline put (a:'a) (mb:Mb<'a>) = mb.Post a

  /// Creates an async computation that completes when a message is available in a mailbox.
  let inline take (mb:Mb<'a>) = mb.Receive()