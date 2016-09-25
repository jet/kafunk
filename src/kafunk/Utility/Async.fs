#nowarn "40"
namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent

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




type MVar<'a> = 
  private 
  | MVar of MailboxProcessor<Choice<('a * AsyncReplyChannel<unit>), AsyncReplyChannel<'a>>>

module MVar =
  
  let create () : MVar<'a> =
    let mbp = 
      MailboxProcessor.Start 
        (fun agent -> async {
          let rec empty () = async {
            return! agent.Scan(function
              | Choice1Of2 (a:'a, rep:AsyncReplyChannel<unit>) -> 
                Some (async { rep.Reply () ; return! full a })
              | _ -> None) }
          and full a = async {
            return! agent.Scan(function
              | Choice2Of2 (rep:AsyncReplyChannel<'a>) ->
                Some <| async { rep.Reply a ; return! empty() }
              | _ -> None) }
          return! empty () } )
    MVar mbp
  
  let un (MVar(mbp)) = mbp

  let put (a:'a) (m:MVar<'a>) : Async<unit> =
    let mbp = un m
    mbp.PostAndAsyncReply(fun ch -> Choice1Of2 (a,ch))

  let take (m:MVar<'a>) : Async<'a> =
    let mbp = un m
    mbp.PostAndAsyncReply(fun ch -> Choice2Of2 ch)





type CellReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Update of ('a -> 'a) * AsyncReplyChannel<'a>
  | UpdateAsync of ('a -> Async<'a>) * AsyncReplyChannel<'a>
  | PutOrUpdate of put:'a * up:('a -> 'a) * AsyncReplyChannel<'a>
  | Get of AsyncReplyChannel<'a>

type Cell<'a> (?a:'a) =

  let [<VolatileField>] mutable state : 'a = Unchecked.defaultof<_>
  //let changes = Event<'a>()

  let mbp = MailboxProcessor.Start (fun mbp -> async {
    let rec init () = async {
      return! mbp.Scan (function
        | Put (a,rep) ->
          state <- a
          rep.Reply()          
          Some (loop a)
        | PutOrUpdate (a,_,rep) -> 
          rep.Reply a
          Some (loop a)
        | _ ->
          None) }
    and loop (a:'a) = async {
      let! msg = mbp.Receive()
      match msg with
      | Put (a,rep) ->
        state <- a
        rep.Reply()
        return! loop a
      | PutOrUpdate (_,up,rep) ->
        let a = up a
        state <- a
        rep.Reply a
        return! loop a
      | Get rep ->
        rep.Reply a
        return! loop a
      | Update (f,rep) ->
        let a = f a
        state <- a
        rep.Reply a
        return! loop a
      | UpdateAsync (f,rep) ->
        let! a = f a
        state <- a
        rep.Reply a
        return! loop a }
    match a with
    | Some a ->
      state <- a
      return! loop a
    | None -> 
      return! init () })

  member __.Get () : Async<'a> =
    mbp.PostAndAsyncReply (Get)

  member __.GetFast () : 'a =
    state

  member __.Put (a:'a) : Async<unit> =
    mbp.PostAndAsyncReply (fun ch -> Put (a,ch))

  member __.Update (f:'a -> 'a) : Async<'a> =
    mbp.PostAndAsyncReply (fun ch -> Update (f,ch))

  member __.UpdateAsync (f:'a -> Async<'a>) : Async<'a> =
    mbp.PostAndAsyncReply (fun ch -> UpdateAsync (f,ch))

  member __.PutOrUpdate (put:'a, up:'a -> 'a) : Async<'a> =
    mbp.PostAndAsyncReply (fun ch -> PutOrUpdate (put,up,ch))

  interface IDisposable with
    member __.Dispose () = (mbp :> IDisposable).Dispose()

module Cell =
  
  let create () : Cell<'a> =
    new Cell<_>()

  let createFull (a:'a) : Cell<'a> =
    new Cell<_>(a)

  let get (c:Cell<'a>) : Async<'a> =
    c.Get ()

  let getFastUnsafe (c:Cell<'a>) : 'a =
    c.GetFast ()

  let put (a:'a) (c:Cell<'a>) : Async<unit> =
    c.Put a

  let update (f:'a -> 'a) (c:Cell<'a>) : Async<'a> =
    c.Update f

  let updateAsync (f:'a -> Async<'a>) (c:Cell<'a>) : Async<'a> =
    c.UpdateAsync f

  let putOrUpdate (put:'a) (up:'a -> 'a) (c:Cell<'a>) : Async<'a> =
    c.PutOrUpdate (put,up)



// operations on resource monitors.
module Resource =

  /// Resource recovery action
  type Recovery =
      
    /// The error should be ignored.
    | Ignore

    /// The resource should be re-created.
    | Recreate

    /// The error should be escalated, notifying dependent
    /// resources.
    | Escalate     

  /// Configuration for a recoverable resource.        
  type Cfg<'r> = {
      
    /// A computation which creates a resource.
    create : Async<'r>
      
    /// A computation which handles an exception received
    /// during action upon a resource.
    /// The resource takes the returned recovery action.
    handle : ('r * exn) -> Async<Recovery>
      
    /// A heartbeat process, started each time the
    /// resource is created.
    /// If and when and heartbeat computation completes,
    /// the returned recovery action is taken.
    hearbeat : 'r -> Async<Recovery>

    /// Closes the resource.
    close : 'r -> Async<unit>

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
  type Resource<'r when 'r : not struct> internal (create:Async<'r>, handle:('r * exn) -> Async<Recovery>) =
      
    let Log = Log.create "Resource"
    let rsrc : 'r ref = ref Unchecked.defaultof<_>
    let mre = new ManualResetEvent(false)
    let st = ref 0 // 0 - initialized/zero | 1 - initializing
   
    /// Creates the resource, ensuring mutual exclusion.
    /// In this case, mutual exclusion is extended with the ability to exchange state.
    /// Protocol:
    /// - atomic { 
    ///   if ZERO then set CREATING, create and set resource, pulse waiters
    ///   else set WAITING (and wait on pulse), once wait completes, read current value
    member internal __.Create () = async {
      match Interlocked.CompareExchange (st, 1, 0) with
      | 0 ->
        Log.info "creating...."
        let! r = create
        rsrc := r
        mre.Set () |> ignore
        Interlocked.Exchange (st, 0) |> ignore
        return ()
      | _ ->        
        Log.info "waiting..."
        let! _ = Async.AwaitWaitHandle mre
        mre.Reset () |> ignore
        return () }

    /// Initiates recovery of the resource by virtue of the specified exception
    /// and executes the resulting recovery action.
    member __.Recover (ex:exn) = async {
      let r = !rsrc
      let! recovery = handle (r,ex)
      match recovery with
      | Ignore -> 
        Log.info "recovery action=ignoring..."
        return ()
      | Escalate -> 
        Log.info "recovery action=escalating..."
        //evt.Trigger (Escalating)
        raise ex
        return ()
      | Recreate ->
        Log.info "recovery action=restarting..."
        do! __.Create()
        Log.info "recovery restarted"
        return () }

    member __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b> =
      let rec go a = async {
        let r = !rsrc
        try
          let! b = op r a
          return b
        with ex ->
          Log.info "caught exception on injected operation, calling recovery..."
          do! __.Recover ex
          Log.info "recovery complete, restarting operation..."
          return! go a }
      go

    interface IDisposable with
      member __.Dispose () = ()
    
  let recoverableRecreate (create:Async<'r>) (handleError:('r * exn) -> Async<Recovery>) = async {      
    let r = new Resource<_>(create, handleError)
    do! r.Create()
    return r }

  /// Injects a resource into a resource-dependent async function.
  /// Failures thrown by the resource-dependent computation are handled by the resource 
  /// recovery logic.
  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : 'a -> Async<'b> =
    r.Inject op
   
      

         