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

    /// Starts the specified operation using a new CancellationToken and returns
    /// IDisposable object that cancels the computation. This method can be used
    /// when implementing the Subscribe method of IObservable interface.
    static member StartDisposable (op:Async<unit>) =
      let ct = new System.Threading.CancellationTokenSource()
      Async.Start(op, ct.Token)
      { new IDisposable with member x.Dispose() = ct.Cancel() }

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
        | Choice2Of2 b -> (b,(second |> Async.map (function Choice1Of2 a -> a | _ -> failwith "invalid state"))) |> Choice2Of2)

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
      Async.timeoutResultWith (fun () -> TimeoutException()) timeout c

    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) =
      Async.timeoutResult timeout c |> Async.map Result.throw



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
//        let edi = Runtime.ExceptionServices.ExceptionDispatchInfo.Capture ex
//        error (a,edi.SourceException)
//        edi.Throw ()
//        return failwith "undefined" }

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




type private MVarReq<'a> =
  | PutAsync of Async<'a> * TaskCompletionSource<'a>
  | UpdateAsync of update:('a -> Async<'a>) * TaskCompletionSource<'a>
  | PutOrUpdateAsync of update:('a option -> Async<'a>) * TaskCompletionSource<'a>
  | Get of TaskCompletionSource<'a>
  | Take of TaskCompletionSource<'a>

/// A serialized variable.
type MVar<'a> internal (?a:'a) =

  let [<VolatileField>] mutable state : 'a option = None

  let mbp = MailboxProcessor.Start (fun mbp -> async {
    let rec init () = async {
      return! mbp.Scan (function
        | PutAsync (a,rep) ->
          Some (async {
            try
              let! a = a
              state <- Some a
              rep.SetResult a
              return! loop a
            with ex ->
              rep.SetException ex
              return! init () })
        | PutOrUpdateAsync (update,rep) ->
          Some (async {
            try
              let! a = update None
              state <- Some a
              rep.SetResult a
              return! loop (a)
            with ex ->
              rep.SetException ex
              return! init () })
        | _ ->
          None) }
    and loop (a:'a) = async {
      let! msg = mbp.Receive()
      match msg with
      | PutAsync (a',rep) ->
        try
          let! a = a'
          state <- Some a
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a)
      | PutOrUpdateAsync (update,rep) ->
        try
          let! a = update (Some a)
          state <- Some a
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a)
      | Get rep ->
        rep.SetResult a
        return! loop (a)
      | Take (rep) ->
        state <- None
        rep.SetResult a
        return! init ()
      | UpdateAsync (f,rep) ->
        try
          let! a = f a
          state <- Some a
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a) }
    match a with
    | Some a ->
      state <- Some a
      return! loop (a)
    | None -> 
      return! init () })

  do mbp.Error.Add (fun x -> printfn "|MVar|ERROR|%O" x) // shouldn't happen
  
  let postAndAsyncReply f = 
    let tcs = new TaskCompletionSource<'a>()
    mbp.Post (f tcs)
    tcs.Task |> Async.AwaitTask 

  member __.Get () : Async<'a> =
    postAndAsyncReply (Get)

  member __.Take () : Async<'a> =
    postAndAsyncReply (fun tcs -> Take(tcs))

  member __.GetFast () : 'a option =
    state

  member __.Put (a:'a) : Async<'a> =
    __.PutAsync (async.Return a)

  member __.PutAsync (a:Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutAsync (a,ch))

  member __.Update (f:'a -> 'a) : Async<'a> =
    __.UpdateAsync (f >> async.Return)

  member __.UpdateAsync (update:'a -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> UpdateAsync (update, ch))

  member __.PutOrUpdateAsync (update:'a option -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutOrUpdateAsync (update,ch))

  interface IDisposable with
    member __.Dispose () = (mbp :> IDisposable).Dispose()

/// Operations on serialized variables.
module MVar =
  
  /// Creates an empty MVar.
  let create () : MVar<'a> =
    new MVar<_>()

  /// Creates a full MVar.
  let createFull (a:'a) : MVar<'a> =
    new MVar<_>(a)

  /// Gets the value of the MVar.
  let get (c:MVar<'a>) : Async<'a> =
    c.Get ()

  /// Takes an item from the MVar.
  let take (c:MVar<'a>) : Async<'a> =
    c.Take ()
  
  /// Returns the last known value, if any, without serialization.
  /// NB: unsafe because the value may be null, but helpful for supporting overlapping
  /// operations.
  let getFastUnsafe (c:MVar<'a>) : 'a option =
    c.GetFast ()

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let put (a:'a) (c:MVar<'a>) : Async<'a> =
    c.Put a

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let putAsync (a:Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.PutAsync a

  /// Puts a new value into an MVar or updates an existing value.
  /// Returns the value that was put or the updated value.
  let putOrUpdateAsync (update:'a option -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.PutOrUpdateAsync (update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let update (update:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    c.Update update

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateAsync (update:'a -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.UpdateAsync update





type internal BoundedMbReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Take of AsyncReplyChannel<'a>

type BoundedMb<'a> internal (capacity:int) =
  do if capacity < 1 then invalidArg "capacity" "must be positive"

  let agent = MailboxProcessor.Start <| fun agent ->

    let queue = new Queue<_>()

    let rec loop () = async {
      match queue.Count with
      | 0 -> do! tryReceive ()
      | n when n = capacity -> do! trySend ()
      | _ -> do! trySendOrReceive () 
      return! loop () }

    and tryReceive () = 
      agent.Scan (function
        | Put (a,rep) -> Some (receive(a,rep))
        | _ -> None)

    and receive (a:'a, rep:AsyncReplyChannel<unit>) = async {
      queue.Enqueue a
      rep.Reply () }

    and trySend () = 
      agent.Scan (function
        | Take rep -> Some (send rep)
        | _ -> None)

    and send (rep:AsyncReplyChannel<'a>) = async {
      let a = queue.Dequeue ()
      rep.Reply a }

    and trySendOrReceive () = async {
      let! msg = agent.Receive ()
      match msg with
      | Put (a,rep) -> return! receive (a,rep)
      | Take rep -> return! send rep }

    loop ()

  member __.Put (a:'a) =
    agent.PostAndAsyncReply (fun ch -> Put (a,ch))

  member __.Take () =
    agent.PostAndAsyncReply (fun ch -> Take ch)

  member __.TryTake (timeout:int option) =
    agent.PostAndTryAsyncReply ((fun ch -> Take ch), defaultArg timeout Timeout.Infinite)

  interface IDisposable with
    member __.Dispose () = (agent :> IDisposable).Dispose()


module BoundedMb =

  let create (capacity:int) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(capacity)
    mq

  /// Puts a message into the mailbox.
  /// If the mailbox is full, wait until it has room available.
  /// If the mailbox has vacancy, add the message to the buffer and return immediately.
  let inline put (a:'a) (mb:BoundedMb<'a>) : Async<unit> =
    mb.Put a

  /// Takes a message from
  let inline take (mb:BoundedMb<'a>) : Async<'a> =
    mb.Take ()






// operations on resource monitors.
module Resource =

  /// Resource recovery action
  type Recovery =
      
    /// The resource should be re-created.
    | Recreate

    /// The error should be escalated, notifying dependent
    /// resources.
    | Escalate


  type Epoch<'r> = {
    resource : 'r
    closed : CancellationTokenSource
    version : int
  }
     
  /// <summary>
  /// Recoverable resource supporting the creation recoverable operations.
  /// - create - used to create the resource initially and upon recovery. Overlapped inocations
  ///   of this function are queued and given the instance being created when creation is complete.
  /// - handle - called when an exception is raised by an resource-dependent computation created
  ///   using this resrouce. If this function throws an exception, it is escalated.
  /// </summary>
  /// <notes>
  /// A resource is an entity which undergoes state changes and is used by operations.
  /// Resources can form supervision hierarchy through a message passing and reaction system.
  /// Supervision hierarchies can be used to re-cycle chains of dependent resources.
  /// </notes>
  type Resource<'r> internal (create:Async<'r>, handle:('r * int * obj * exn) -> Async<Recovery>) =
      
    let Log = Log.create "Resource"
    
    let cell : MVar<Epoch<'r>> = MVar.create ()
   
    let create (prev:Epoch<'r> option) = async {      
      let version = 
        match prev with
        | Some prev -> 
          Log.warn "closing_previous_resource_epoch|version=%i" prev.version
          prev.closed.Cancel()
          prev.version + 1
        | None ->
          0
      let! r = create
      return { resource = r ; closed = new CancellationTokenSource() ; version = version } }

    let recover (req:obj) ex ep = async {
      let! recovery = handle (ep.resource,ep.version,req,ex)
      match recovery with
      | Escalate ->
        return raise ex
      | Recreate ->
        let! ep' = create (Some ep)
        return ep' }

    member internal __.Create () = async {
      return! cell |> MVar.putAsync (create None) }

    member internal __.TryGetVersion () =
      MVar.getFastUnsafe cell |> Option.map (fun e -> e.version)

    member private __.Recover (ep':Epoch<'r>, req:obj, ex:exn) =
      cell 
      |> MVar.updateAsync (fun ep -> 
        if ep.version = ep'.version then 
          recover req ex ep 
        else async {
          Log.warn "resource_recovery_already_requested|requested_version=%i current_version=%i" ep'.version ep.version
          return ep })

    member internal __.Recover (req:obj, ex:exn) = async {
      let! ep = MVar.get cell
      let! _ep' = __.Recover (ep, req, ex)
      return () }
        
    member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
      let! epoch = MVar.get cell
      let epoch = ref epoch
      let rec go a = async {
        let ep = !epoch
        try
          return! op ep.resource a //|> Async.withCancellationToken ep.closed.Token
        with ex ->
          let! epoch' = __.Recover (ep, box a, ex)
          epoch := epoch'
          return! go a }
      return go }

    interface IDisposable with
      member __.Dispose () = ()
    
  let recoverableRecreate (create:Async<'r>) (handleError:('r * int * obj * exn) -> Async<Recovery>) = async {
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op



         