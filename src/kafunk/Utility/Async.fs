[<AutoOpen>]
module Kafunk.Async

#nowarn "40"

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent
open Kafunk


/// A write-once concurrent variable.
type IVar<'a> = TaskCompletionSource<'a>

/// Operations on write-once variables.
module IVar =

  /// Creates an empty IVar structure.
  let inline create () = new IVar<'a>()

  /// Creates a IVar structure and initializes it with a value.
  let inline createFull a =
    let ivar = create()
    ivar.SetResult(a)
    ivar

  /// Writes a value to an IVar.
  /// A value can only be written once, after which the behavior is undefined and may throw.
  let inline put a (i:IVar<'a>) = 
    i.SetResult(a)

  let inline tryPut a (i:IVar<'a>) = 
    i.TrySetResult (a)

  /// Writes an error to an IVar to be propagated to readers.
  let inline error (ex:exn) (i:IVar<'a>) = 
    i.SetException(ex)

  let inline tryError (ex:exn) (i:IVar<'a>) = 
    i.TrySetException(ex)

  /// Writes a cancellation to an IVar to be propagated to readers.
  let inline cancel (i:IVar<'a>) = 
    i.SetCanceled()

  let inline tryCancel (i:IVar<'a>) = 
    i.TrySetCanceled()

  /// Creates an async computation which returns the value contained in an IVar.
  let inline get (i:IVar<'a>) : Async<'a> = 
    i.Task |> Async.AwaitTask




let private empty : Async<unit> = async.Return()

let private never : Async<unit> = Async.Sleep Timeout.Infinite

let private awaitTaskUnit (t:Task) =
  Async.FromContinuations <| fun (ok,err,cnc) ->
    t.ContinueWith(fun t ->
      if t.IsFaulted then err(t.Exception)
      elif t.IsCanceled then cnc(OperationCanceledException("Task wrapped with Async.AwaitTask has been cancelled.",  t.Exception))
      elif t.IsCompleted then ok()
      else failwith "invalid Task state!") |> ignore

let private awaitTaskCancellationAsError (t:Task<'a>) : Async<'a> =
  Async.FromContinuations <| fun (ok,err,_) ->
    t.ContinueWith (fun (t:Task<'a>) ->
      if t.IsFaulted then err t.Exception
      elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
      elif t.IsCompleted then ok t.Result
      else failwith "invalid Task state!") |> ignore

let private awaitTaskUnitCancellationAsError (t:Task) : Async<unit> =
  Async.FromContinuations <| fun (ok,err,_) ->
    t.ContinueWith (fun (t:Task) ->
      if t.IsFaulted then err t.Exception
      elif t.IsCanceled then err (OperationCanceledException("Task wrapped with Async has been cancelled."))
      elif t.IsCompleted then ok ()
      else failwith "invalid Task state!") |> ignore

type Async with

  /// An async computation which does nothing and completes immediately.
  static member empty = empty

  /// An async computation which does nothing and never completes.
  static member never = never

  static member map (f:'a -> 'b) (a:Async<'a>) : Async<'b> = async.Bind(a, f >> async.Return)

  static member inline bind (f:'a -> Async<'b>) (a:Async<'a>) : Async<'b> = async.Bind(a, f)

  static member inline join (a:Async<Async<'a>>) : Async<'a> = Async.bind id a
   
  static member tryFinnallyAsync (a:Async<'a>) (comp:Async<unit>) : Async<'a> =
    async {
      try
        let! a = a
        do! comp
        return a
      with ex ->
        do! comp
        return raise ex }

  static member tryFinally (compensation:unit -> unit) (a:Async<'a>) : Async<'a> =
    async.TryFinally(a, compensation)

  static member tryFinallyDispose (d:#IDisposable) (a:Async<'a>) : Async<'a> =
    Async.tryFinally (fun () -> d.Dispose()) a

  static member tryFinallyDisposeAll (ds:#IDisposable seq) (a:Async<'a>) : Async<'a> =
    Async.tryFinally (fun () -> ds |> Seq.iter (fun d -> d.Dispose())) a

  static member tryCancelled comp a = Async.TryCancelled(a, comp)

  static member tryWith h a = async.TryWith(a, h)

  /// Returns an async computation which will wait for the given task to complete.
  static member AwaitTask (t:Task) = awaitTaskUnit t

  /// Returns an async computation which will wait for the given task to complete and returns its result.
  /// Task cancellations are propagated as exceptions so that they can be trapped.
  static member AwaitTaskCancellationAsError (t:Task<'a>) : Async<'a> = 
    awaitTaskCancellationAsError t

  /// Returns an async computation which will wait for the given task to complete and returns its result.
  /// Task cancellations are propagated as exceptions so that they can be trapped.
  static member AwaitTaskCancellationAsError (t:Task) : Async<unit> = 
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
  static member ParallelThrottledIgnore (parallelism:int) (xs:seq<Async<_>>) = async {
    let! ct = Async.CancellationToken
    use sm = new SemaphoreSlim(parallelism)
    use cde = new CountdownEvent(1)
    let tcs = new TaskCompletionSource<unit>()
    ct.Register(Action(fun () -> tcs.TrySetCanceled() |> ignore)) |> ignore
    let tryComplete () =
      if not (tcs.Task.IsCompleted) then
        if cde.Signal() then
          tcs.SetResult(())
        true
      else
        false
    let ok _ =
      if tryComplete () then
        sm.Release() |> ignore
    let err (ex:exn) =
      if tcs.TrySetException ex then
        sm.Release() |> ignore
    let cnc (_:OperationCanceledException) =
      if tcs.TrySetCanceled () then
        sm.Release() |> ignore
    try
      use en = xs.GetEnumerator()
      while not (tcs.Task.IsCompleted) && en.MoveNext() do
        sm.Wait()
        cde.AddCount(1)
        Async.StartThreadPoolWithContinuations (en.Current, ok, err, cnc, ct)
      tryComplete () |> ignore
      do! tcs.Task |> Async.AwaitTaskCancellationAsError
    with ex ->
      printfn "Async.ParallelThrottledIgnore|error=%O" ex
      return raise ex }

  /// Creates an async computation which completes when any of the argument computations completes.
  /// The other computation is cancelled.
  static member choose (a:Async<'a>) (b:Async<'a>) : Async<'a> = async {
    let! ct = Async.CancellationToken
    return!
      Async.FromContinuations <| fun (ok,err,cnc) ->
        let state = ref 0
        let cts = CancellationTokenSource.CreateLinkedTokenSource ct
        let cancel () =
          cts.Cancel()
          // cts.Dispose()
        let ok a =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            ok a
            cancel ()
        let err (ex:exn) =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            cancel ()
            err ex
        let cnc ex =
          if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
            cancel ()
            cnc ex
        Async.StartThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
        Async.StartThreadPoolWithContinuations (b, ok, err, cnc, cts.Token) }

  static member chooseChoice (a:Async<'a>) (b:Async<'b>) : Async<Choice<'a, 'b>> =
    Async.choose (a |> Async.map Choice1Of2) (b |> Async.map Choice2Of2)

  /// Cancels a computation and returns None if the CancellationToken is cancelled before the 
  /// computation completes.
  static member cancelWithToken (ct:CancellationToken) (a:Async<'a>) : Async<'a option> = async {
    let! ct2 = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
    let tcs = new TaskCompletionSource<'a option>()
    use _reg = cts.Token.Register (fun () -> tcs.SetResult None)
    let a = async {
      try
        let! a = a
        tcs.SetResult (Some a)
      with ex ->
        tcs.SetException ex }
    Async.Start (a, cts.Token)
    return! tcs.Task |> Async.AwaitTask }
        
  static member Sleep (s:TimeSpan) : Async<unit> =
    Async.Sleep (int s.TotalMilliseconds)
      
  static member timeoutWith (g:'a -> 'b) (f:unit -> 'b) (timeout:TimeSpan) (c:Async<'a>) : Async<'b> =
    let timeout = async {
      do! Async.Sleep timeout
      return f () }
    Async.choose (Async.map g c) timeout

  static member timeoutOption (timeout:TimeSpan) (c:Async<'a>) : Async<'a option> =
    Async.timeoutWith Some (fun () -> None) timeout c

  static member timeoutResultWith (f:unit -> 'e) (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, 'e>> =
    Async.timeoutWith Success (f >> Failure) timeout c

  static member timeoutResult (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, TimeoutException>> =
    Async.timeoutResultWith (fun () -> TimeoutException(sprintf "The operation timed out after %fsec" timeout.TotalSeconds)) timeout c


/// Operations on functions of the form 'a -> Async<'b>.
module AsyncFunc =
  
  let catch (f:'a -> Async<'b>) : 'a -> Async<Result<'b, exn>> =
    f >> Async.Catch

  let catchWith (e:exn -> 'b) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    f >> Async.Catch >> Async.map (Result.fold id e)

  let catchResult (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, Choice<'e, exn>>> =
    f >> Async.Catch >> Async.map Result.join

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

  let timeoutOption (t:TimeSpan) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    fun a -> Async.timeoutOption t (async.Delay (fun () -> f a))

  let timeoutResult (t:TimeSpan) (f:'a -> Async<'b>) : 'a -> Async<Result<'b, TimeoutException>> =
    fun a -> Async.timeoutResult t (async.Delay (fun () -> f a))
  






/// A mailbox processor.
type Mb<'a> = MailboxProcessor<'a>

/// Operations on unbounded FIFO mailboxes.
module internal Mb =

  /// Creates a new unbounded mailbox.
  let create () : Mb<'a> = 
    MailboxProcessor.Start (fun _ -> async.Return())

  /// Puts a message into a mailbox, no waiting.
  let inline put (a:'a) (mb:Mb<'a>) = mb.Post a

  /// Creates an async computation that completes when a message is available in a mailbox.
  let inline take (mb:Mb<'a>) = async.Delay mb.Receive