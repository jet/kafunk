[<AutoOpen>]
module internal Kafunk.AsyncEx
#nowarn "40"

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent
open Kafunk

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
    i.Task |> awaitTaskCancellationAsError

//  /// Creates an async computation which returns the value contained in an IVar.
//  let inline getWithTimeout (timeout:TimeSpan) (timeoutResult:unit -> 'a) (i:IVar<'a>) : Async<'a> = async {
//    let! ct = Async.CancellationToken
//    (Task.Delay (timeout, ct)).ContinueWith (Func<_,_>(fun _ -> tryPut (timeoutResult ()) i))
//    |> ignore
//    return! i.Task |> awaitTaskCancellationAsError }

  /// Creates an async computation which returns the value contained in an IVar.
  let inline getWithTimeout (timeout:TimeSpan) (timeoutResult:unit -> 'a) (i:IVar<'a>) : Async<'a> = async {
    use _timer = new Timer((fun _ -> tryPut (timeoutResult ()) i |> ignore), null, (int timeout.TotalMilliseconds), Timeout.Infinite)
    return! i.Task |> awaitTaskCancellationAsError }

  /// Returns a cancellation token which is cancelled when the IVar is set.
  let inline intoCancellationToken (cts:CancellationTokenSource) (i:IVar<_>) =
    i.Task.ContinueWith (fun (t:Task<_>) -> cts.Cancel ()) |> ignore

  /// Returns a cancellation token which is cancelled when the IVar is set.
  let inline asCancellationToken (i:IVar<_>) =
    let cts = new CancellationTokenSource ()
    intoCancellationToken cts i
    cts.Token
    

/// Operations on System.Threading.Tasks.Task<_>.
module Task =
  
  let never<'a> : Task<'a> =
    let ivar = IVar.create ()
    ivar.Task

  let inline create (a:'a) : Task<'a> =
    Task.FromResult a

  let inline join (t:Task<Task<'a>>) : Task<'a> =
    t.Unwrap()

  let inline extend (f:Task<'a> -> 'b) (t:Task<'a>) : Task<'b> =
    t.ContinueWith f

  let inline map (f:'a -> 'b) (t:Task<'a>) : Task<'b> =
    extend (fun t -> f t.Result) t

  let inline bind (f:'a -> Task<'b>) (t:Task<'a>) : Task<'b> =
    extend (fun t -> f t.Result) t |> join

  /// Returns a Task that completes only if the argument Task faults.
  let taskFault (t:Task<'a>) : Task<'b> =
    t 
    |> extend (fun t -> 
      let ivar = IVar.create ()
      if t.IsFaulted then
        IVar.error t.Exception ivar
      ivar.Task)
    |> join

  /// Returns a cancellation token which is cancelled when the IVar is set.
  let intoCancellationToken (cts:CancellationTokenSource) (t:Task<_>) =
    t.ContinueWith (fun (t:Task<_>) -> cts.Cancel ()) |> ignore

  /// Returns a cancellation token which is cancelled when the IVar is set.
  let asCancellationToken (t:Task<_>) =
    let cts = new CancellationTokenSource ()
    intoCancellationToken cts t
    cts.Token

[<Compile(Module)>]
module Async =

  /// An async computation which does nothing and completes immediately.
  let empty = async.Return()

  /// An async computation which does nothing and never completes.
  let never = Async.Sleep Timeout.Infinite

  let map (f:'a -> 'b) (a:Async<'a>) : Async<'b> = async.Bind(a, f >> async.Return)

  let bind (f:'a -> Async<'b>) (a:Async<'a>) : Async<'b> = async.Bind(a, f)

  let join (a:Async<Async<'a>>) : Async<'a> = bind id a
   
  let tryFinnallyAsync (a:Async<'a>) (comp:Async<unit>) : Async<'a> =
    async {
      try
        let! a = a
        do! comp
        return a
      with ex ->
        do! comp
        return raise ex }

  let tryFinnallyWithAsync (a:Async<'a>) (comp:Async<unit>) (err:exn -> Async<'a>) : Async<'a> =
    async {
      try
        let! a = a
        do! comp
        return a
      with ex ->
        return! err ex }

  let tryFinally (compensation:unit -> unit) (a:Async<'a>) : Async<'a> =
    async.TryFinally(a, compensation)

  let tryFinallyDispose (d:#IDisposable) (a:Async<'a>) : Async<'a> =
    tryFinally (fun () -> d.Dispose()) a

  let tryFinallyDisposeAll (ds:#IDisposable seq) (a:Async<'a>) : Async<'a> =
    tryFinally (fun () -> ds |> Seq.iter (fun d -> d.Dispose())) a

  let tryCancelled comp a = Async.TryCancelled(a, comp)

  let tryWith h a = async.TryWith(a, h)

  /// Returns an async computation which will wait for the given task to complete.
  let awaitTaskUnit (t:Task) = awaitTaskUnit t

  /// Returns an async computation which will wait for the given task to complete and returns its result.
  /// Task cancellations are propagated as exceptions so that they can be trapped.
  let awaitTaskCancellationAsError (t:Task<'a>) : Async<'a> = 
    awaitTaskCancellationAsError t

  /// Returns an async computation which will wait for the given task to complete and returns its result.
  /// Task cancellations are propagated as exceptions so that they can be trapped.
  let awaitTaskUnitCancellationAsError (t:Task) : Async<unit> = 
    awaitTaskUnitCancellationAsError t

  /// Like Async.StartWithContinuations but starts the computation on a ThreadPool thread.
  let startThreadPoolWithContinuations (a:Async<'a>, ok:'a -> unit, err:exn -> unit, cnc:OperationCanceledException -> unit, ct:CancellationToken) =
    let a = Async.SwitchToThreadPool () |> bind (fun _ -> a)
    Async.StartWithContinuations (a, ok, err, cnc, ct)

  let parallel2 (c1, c2) : Async<'a * 'b> = async {
    let! c1 = c1 |> Async.StartChild
    let! c2 = c2 |> Async.StartChild
    let! c1 = c1
    let! c2 = c2
    return c1,c2 }

  let parallel3 (c1, c2, c3) : Async<'a * 'b * 'c> = async {
    let! c1 = c1 |> Async.StartChild
    let! c2 = c2 |> Async.StartChild
    let! c3 = c3 |> Async.StartChild
    let! c1 = c1
    let! c2 = c2
    let! c3 = c3
    return c1,c2,c3 }

  let parallel4 (c1, c2, c3, c4) : Async<'a * 'b * 'c * 'd> = async {
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
  let parallelThrottledIgnoreThread (startOnCallingThread:bool) (parallelism:int) (xs:seq<Async<_>>) = async {
    let! ct = Async.CancellationToken
    use sm = new SemaphoreSlim(parallelism)
    let count = ref 1
    let res = IVar.create ()
    let tryComplete () =
      if Interlocked.Decrement count = 0 then
        IVar.tryPut () res
      else
        not res.Task.IsCompleted
    let ok _ =
      if tryComplete () then
        sm.Release() |> ignore      
    let err (ex:exn) =
      if IVar.tryError ex res then
        sm.Release() |> ignore
    let cnc (_:OperationCanceledException) =
      if IVar.tryCancel res then
        sm.Release() |> ignore
    let start = async {
      use en = xs.GetEnumerator()
      while not (res.Task.IsCompleted) && en.MoveNext() do
        sm.Wait()
        Interlocked.Increment count |> ignore
        if startOnCallingThread then Async.StartWithContinuations (en.Current, ok, err, cnc, ct)
        else startThreadPoolWithContinuations (en.Current, ok, err, cnc, ct)
      tryComplete () |> ignore }
    Async.Start (tryWith (err >> async.Return) start, ct)
    return! res.Task |> awaitTaskCancellationAsError }

  let parallelThrottledIgnore (parallelism:int) (xs:seq<Async<_>>) =
    parallelThrottledIgnoreThread true parallelism xs

  /// Creates an async computation which completes when any of the argument computations completes.
  /// The other computation is cancelled.
  let choose (a:Async<'a>) (b:Async<'a>) : Async<'a> = async {
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
        startThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
        startThreadPoolWithContinuations (b, ok, err, cnc, cts.Token) }

  let choose2 (a:Async<'a>) (b:Async<'a>) : Async<'a> = async {
    let! ct = Async.CancellationToken
    let cts = CancellationTokenSource.CreateLinkedTokenSource ct
    let res = IVar.create ()
    IVar.intoCancellationToken cts res
    let inline ok a = IVar.tryPut a res |> ignore
    let inline err e = IVar.tryError e res |> ignore
    let inline cnc (_:OperationCanceledException) = IVar.tryCancel res |> ignore
    startThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
    startThreadPoolWithContinuations (b, ok, err, cnc, cts.Token)
    return! IVar.get res }

  let chooseChoice (a:Async<'a>) (b:Async<'b>) : Async<Choice<'a, 'b>> =
    choose (a |> map Choice1Of2) (b |> map Choice2Of2)

  /// Cancels a computation and returns None if the CancellationToken is cancelled before the 
  /// computation completes.
  let withCancellation (ct:CancellationToken) (a:Async<'a>) : Async<'a> = async {
    let! ct2 = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
    let tcs = new TaskCompletionSource<'a>()
    use _reg = cts.Token.Register (fun () -> tcs.TrySetCanceled() |> ignore)
    let a = async {
      try
        let! a = a
        tcs.TrySetResult a |> ignore
      with ex ->
        tcs.TrySetException ex |> ignore }
    Async.Start (a, cts.Token)
    return! tcs.Task |> awaitTaskCancellationAsError }

  /// Cancels a computation and returns None if the CancellationToken is cancelled before the 
  /// computation completes.
  let cancelTokenWith (ct:CancellationToken) (f:unit -> 'a) (a:Async<'a>) : Async<'a> = async {
    let! ct2 = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
    let tcs = new TaskCompletionSource<'a>()
    use _reg = cts.Token.Register (fun () -> tcs.TrySetResult (f ()) |> ignore)
    let a = async {
      try
        let! a = a
        tcs.TrySetResult a |> ignore
      with ex ->
        tcs.TrySetException ex |> ignore }
    Async.Start (a, cts.Token)
    return! tcs.Task |> awaitTaskCancellationAsError }

  /// Cancels a computation and returns None if the CancellationToken is cancelled before the 
  /// computation completes.
  let cancelWithToken (ct:CancellationToken) (a:Async<'a>) : Async<'a option> = async {
    let! ct2 = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
    let tcs = new TaskCompletionSource<'a option>()
    use _reg = cts.Token.Register (fun () -> tcs.TrySetResult None |> ignore)
    let a = async {
      try
        let! a = a
        tcs.TrySetResult (Some a) |> ignore
      with ex ->
        tcs.TrySetException ex |> ignore }
    Async.Start (a, cts.Token)
    return! tcs.Task |> awaitTaskCancellationAsError }

  let cancelWithTask (t:Task<unit>) (a:Async<'a>) : Async<'a option> = async {
    let! ct = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource ct
    let t = t.ContinueWith (fun (_:Task<unit>) -> cts.Cancel () ; None)
    let at = Async.StartAsTask (a, cancellationToken = cts.Token) |> Task.map Some
    let! r = Task.WhenAny (t, at) |> awaitTaskCancellationAsError
    return r.Result }

  let cancelWithTaskThrow (t:Task<unit>) (a:Async<'a>) : Async<'a> = async {
    let! ct = Async.CancellationToken
    use cts = CancellationTokenSource.CreateLinkedTokenSource ct
    let t = 
      t 
      |> Task.extend (fun t -> 
        cts.Cancel () |> ignore
        raise (OperationCanceledException("", t.Exception)))
    let at = Async.StartAsTask (a, cancellationToken = cts.Token)
    let! r = Task.WhenAny (t, at) |> awaitTaskCancellationAsError
    return r.Result }

  let cancelWithTaskTimeout (timeout:TimeSpan) (t:Task<unit>) (a:Async<'a>) : Async<'a> = async {
    let timeout = (Task.Delay timeout).ContinueWith (fun (_:Task) -> failwith "task_timedout")
    let t = t |> Task.extend (fun _ -> failwith "task_cancelled")
    let! at = Async.StartChildAsTask a
    let! r = Task.WhenAny (at, t, timeout) |> awaitTaskCancellationAsError
    return r.Result }
        
  let sleep (s:TimeSpan) : Async<unit> =
    Async.Sleep (int s.TotalMilliseconds)
      
  let timeoutWith (g:'a -> 'b) (f:unit -> 'b) (timeout:TimeSpan) (c:Async<'a>) : Async<'b> =
    let timeout = async {
      do! sleep timeout
      return f () }
    choose (map g c) timeout

  let timeoutOption (timeout:TimeSpan) (c:Async<'a>) : Async<'a option> =
    timeoutWith Some (fun () -> None) timeout c

  let timeoutResultWith (f:unit -> 'e) (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, 'e>> =
    timeoutWith Success (f >> Failure) timeout c

  let timeoutResult (timeout:TimeSpan) (c:Async<'a>) : Async<Result<'a, TimeoutException>> =
    timeoutResultWith (fun () -> TimeoutException(sprintf "The operation timed out after %fsec" timeout.TotalSeconds)) timeout c

  let chooseTasks (a:Task<'T>) (b:Task<'U>) : Async<Choice<'T * Task<'U>, 'U * Task<'T>>> =
    async { 
        let! ct = Async.CancellationToken
        let i = Task.WaitAny( [| (a :> Task);(b :> Task) |],ct)
        if i = 0 then return (Choice1Of2 (a.Result, b))
        elif i = 1 then return (Choice2Of2 (b.Result, a)) 
        else return! failwith (sprintf "unreachable, i = %d" i) }

  let chooseTasksAsync (a:Task<'T>) (b:Task<'U>) : Async<Choice<'T * Task<'U>, 'U * Task<'T>>> = async {
    let ta, tb = a :> Task, b :> Task
    let! i = Task.WhenAny( ta, tb ) |> awaitTaskCancellationAsError
    if i = ta then return (Choice1Of2 (a.Result, b))
    elif i = tb then return (Choice2Of2 (b.Result, a)) 
    else return! failwith "unreachable" }

  let chooseTasksUnit (a:Task<'T>) (b:Task) : Async<Choice<'T * Task, unit * Task<'T>>> = async {
    let ta, tb = a :> Task, b
    let! i = Task.WhenAny( ta, tb ) |> awaitTaskCancellationAsError
    if i = ta then return (Choice1Of2 (a.Result, b))
    elif i = tb then return (Choice2Of2 ((), a)) 
    else return! failwith "unreachable" }
    
  let chooseTasks3 (a:Task<'T>) (b:Task<'U>) (c:Task<'O>): Async<Choice<'T * Task<'U> * Task<'O>, 'U * Task<'T> * Task<'O>, 'O * Task<'T> * Task<'U>>> =
    async { 
        let! ct = Async.CancellationToken
        let i = Task.WaitAny( [| (a :> Task);(b :> Task); (c :> Task) |],ct)
        if i = 0 then return (Choice1Of3 (a.Result, b, c))
        elif i = 1 then return (Choice2Of3 (b.Result, a, c)) 
        elif i = 2 then return (Choice3Of3 (c.Result, a, b))
        else return! failwith (sprintf "unreachable, i = %d" i) }    

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

  let tryFinally (comp:'a -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async.TryFinally (f a, fun () -> comp a)


/// A mailbox processor.
type Mb<'a> = MailboxProcessor<'a>

/// Operations on unbounded FIFO mailboxes.
module Mb =

  /// Creates a new unbounded mailbox.
  let create () : Mb<'a> = 
    MailboxProcessor.Start (fun _ -> async.Return())

  /// Puts a message into a mailbox, no waiting.
  let inline put (a:'a) (mb:Mb<'a>) = mb.Post a

  /// Creates an async computation that completes when a message is available in a mailbox.
  let inline take (mb:Mb<'a>) = async.Delay mb.Receive