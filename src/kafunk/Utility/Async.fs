#nowarn "40"
namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

open Kafunk
open Kafunk.Prelude


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

//    /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
//    /// by the specified timeout.
//    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) = async {
//      let! r = Async.StartChild(c, (int)timeout.TotalMilliseconds)
//      return! r }

//    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) =
//      Async.FromContinuations <| fun (ok,err,cnc) ->
//        let cts = new CancellationTokenSource()
//        cts.CancelAfter timeout
//        //cts.Token.Register (fun () -> printfn "cancelled!") |> ignore
//        //let rec t = new Timer(cnc', null, int timeout.TotalMilliseconds, -1)
////        and cnc' _ = 
////          printfn "timeout!"
////          cnc (OperationCanceledException())
////          cts.Cancel()
////          t.Dispose()          
////        let ok a = 
////          ok a
////          t.Dispose()             
////        let err e =           
////          err e
////          t.Dispose()
//        let ok a = ok a ; cts.Dispose()
//        let err e = err e ; cts.Dispose()
//        let cnc e = cnc e ; cts.Dispose()
//        Async.StartWithContinuations (c, ok, err, cnc, cts.Token)

    static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) =
      let timeout = async {
        do! Async.Sleep (int timeout.TotalMilliseconds)
        return raise (OperationCanceledException()) }
      Async.choose c timeout

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
  | Put of 'a * TaskCompletionSource<'a>
  | PutAsync of Async<'a> * TaskCompletionSource<'a>
  | Update of ('a -> 'a) * TaskCompletionSource<'a>
  | UpdateAsync of ('a -> Async<'a>) * TaskCompletionSource<'a>
  | PutOrUpdate of put:'a * up:('a -> 'a) * TaskCompletionSource<'a>
  | Get of TaskCompletionSource<'a>
  | Take of TaskCompletionSource<'a>

/// A serialized variable.
type MVar<'a> (?a:'a) =

  let [<VolatileField>] mutable state : 'a = Unchecked.defaultof<_>

  let mbp = MailboxProcessor.Start (fun mbp -> async {
    let rec init () = async {
      state <- Unchecked.defaultof<_>
      return! mbp.Scan (function
        | Put (a,rep) ->
          state <- a         
          rep.SetResult a
          Some (loop a)
        | PutAsync (a,rep) ->          
          Some (async {
            try            
              let! a = a
              state <- a
              rep.SetResult a
              return! loop a
            with ex ->
              rep.SetException ex
              return! init () })
        | PutOrUpdate (a,_,rep) ->
          rep.SetResult a
          Some (loop a)
        | _ ->
          None) }
    and loop (a:'a) = async {
      let! msg = mbp.Receive()
      match msg with
      | Put (a,rep) ->
        state <- a
        rep.SetResult a
        return! loop a
      | PutAsync (a',rep) ->
        try
          let! a = a'
          state <- a
          rep.SetResult a
          return! loop a
        with ex ->
          rep.SetException ex
          return! loop a
      | PutOrUpdate (_,up,rep) ->
        let a = up a
        state <- a
        rep.SetResult a
        return! loop a
      | Get rep ->
        rep.SetResult a
        return! loop a
      | Take rep ->        
        rep.SetResult a
        return! init ()
      | Update (f,rep) ->
        try
          let a = f a
          state <- a
          rep.SetResult a
          return! loop a
        with ex ->
          rep.SetException ex
          return! loop a
      | UpdateAsync (f,rep) ->
        try
          let! a = f a
          state <- a
          rep.SetResult a
          return! loop a
        with ex ->
          rep.SetException ex
          return! loop a }
    match a with
    | Some a ->
      state <- a
      return! loop a
    | None -> 
      return! init () })

  do mbp.Error.Add (fun x -> printfn "|Cell|ERROR|%O" x)
  
  let postAndAsyncReply f = 
    let tcs = new TaskCompletionSource<'a>()    
    mbp.Post (f tcs)
    tcs.Task |> Async.AwaitTask 

  member __.Get () : Async<'a> =
    postAndAsyncReply (Get)

  member __.Take () : Async<'a> =
    postAndAsyncReply (Take)

  member __.GetFast () : 'a =
    state

  member __.Put (a:'a) : Async<'a> =
    postAndAsyncReply (fun ch -> Put (a,ch))

  member __.PutAsync (a:Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutAsync (a,ch))

  member __.Update (f:'a -> 'a) : Async<'a> =
    postAndAsyncReply (fun ch -> Update (f,ch))

  member __.UpdateAsync (f:'a -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> UpdateAsync (f,ch))

  member __.PutOrUpdate (put:'a, up:'a -> 'a) : Async<'a> =
    postAndAsyncReply (fun ch -> PutOrUpdate (put,up,ch))

  interface IDisposable with
    member __.Dispose () = (mbp :> IDisposable).Dispose()

/// Operations on serialized variables.
module MVar =
  
  let create () : MVar<'a> =
    new MVar<_>()

  let createFull (a:'a) : MVar<'a> =
    new MVar<_>(a)

  let get (c:MVar<'a>) : Async<'a> =
    c.Get ()

  let take (c:MVar<'a>) : Async<'a> =
    c.Take ()

  let getFastUnsafe (c:MVar<'a>) : 'a =
    c.GetFast ()

  let put (a:'a) (c:MVar<'a>) : Async<'a> =
    c.Put a

  let putAsync (a:Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.PutAsync a

  let update (f:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    c.Update f

  let updateAsync (f:'a -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.UpdateAsync f

  let putOrUpdate (put:'a) (up:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    c.PutOrUpdate (put,up)



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
    closed : TaskCompletionSource<unit>
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
  type Resource<'r> internal (create:Async<'r>, handle:('r * exn) -> Async<Recovery>) =
      
    let Log = Log.create "Resource"
    
    let cell : MVar<Epoch<'r>> = MVar.create ()
   
    member internal __.Create () = async {
      return!
        cell
        |> MVar.putAsync (async {
          Log.info "Resource.Create"
          let! r = create
          let closed = new TaskCompletionSource<unit>()
          return { resource = r ; closed = closed } }) }

    member __.Recover (ep':Epoch<'r>, ex:exn) =
      if ep'.closed.TrySetException ex then
        cell
        |> MVar.updateAsync (fun ep -> async {
          let! recovery = handle (ep.resource,ex)
          match recovery with
          | Escalate -> 
            Log.info "recovery_escalating"
            return raise ex              
          | Recreate ->
            Log.info "recovery_restarting"
            let! ep' = __.Create()
            Log.info "recovery_restarted"
            return ep' })
      else
        cell |> MVar.get
        
    member __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
      let! epoch = MVar.get cell
      let rec go ep a = async {
        try
          return! op ep.resource a
        with ex ->
          Log.info "caught_exception_on_injected_operation|error=%O" ex
          let! epoch = __.Recover (ep, ex)
          Log.info "recovery_complete"
          return! go epoch a }
      return go epoch }

    interface IDisposable with
      member __.Dispose () = ()
    
  let recoverableRecreate (create:Async<'r>) (handleError:('r * exn) -> Async<Recovery>) = async {      
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op
   

module AsyncFunc =
  
  let doBeforeAfter (before:'a -> unit) (after:'a * 'b -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      do before a
      let! b = f a
      do after (a,b)
      return b }

  let doBeforeAfterError (before:'a -> unit) (after:'a * 'b -> unit) (error:'a * exn -> unit) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      do before a
      try
        let! b = f a
        do after (a,b)
        return b
      with ex ->
        let edi = Runtime.ExceptionServices.ExceptionDispatchInfo.Capture ex
        error (a,edi.SourceException)
        edi.Throw ()
        return failwith "undefined" }



         