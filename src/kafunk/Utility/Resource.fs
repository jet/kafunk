[<AutoOpen>]
module internal Kafunk.Resource

open System
open System.Threading
open System.Threading.Tasks
open Kafunk
open FSharp.Control
open Kafunk.RetryPolicy

let private Log = Log.create "Kafunk.Resource"

/// An instance of a resource.
type ResourceEpoch<'r> = {
  resource : 'r
  state : IVar<unit>
  cts : CancellationTokenSource
  proc : Task<unit>
  version : int
} with
  member __.isOpen = not __.state.Task.IsCompleted
  member __.isFaulted = __.state.Task.IsFaulted  
  member __.isClosed = __.state.Task.IsCompleted
  member __.isClosedNotFaulted = __.state.Task.IsCompleted && not __.state.Task.IsFaulted
  member __.tryGetError = 
    if __.state.Task.IsFaulted then Some (__.state.Task.Exception :> exn) 
    else None
  member __.tryFault (ex:exn) = __.state.TrySetException ex
  member __.tryClose () = __.state.TrySetResult ()
  member __.tryCloseOrFault (ex:exn option) = 
    match ex with
    | Some ex -> __.tryFault ex
    | None -> __.tryClose ()

/// Resource state.
type ResourceState<'a> =
  
  /// The resource has been closed, either explicitly or as a result of a resource-dependent operation.
  | Closed
  
  /// The resource has been closed due to a fault.
  | Faulted of ex:exn

  /// The resource is open.
  | Open of resource:'a * state:Task<unit>

/// An exception raised when attempt is made to access a faulted resource.
type ClosedResourceException () = 
  inherit Exception ("The resource has been closed.", null)

/// An exception raised when attempt is made to access a faulted resource.
type FaultedResourceException (ex:exn) = 
  inherit Exception ("The resource faulted.", ex)

/// An exception raised when a resource-dependent operation fails and retries are depleted.
type FaultedResourceOperationException (msg:string,exn:exn option) = 
  inherit Exception (msg, exn |> Option.getOr null)

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal (id:string, create:Task<unit> -> ResourceEpoch<'r> option -> Async<'r * Async<unit>>, close:ResourceEpoch<'r> * exn option -> Async<unit>) =
  
  let name = sprintf "%s[%s]" (typeof<'r>.Name) id
  let cell : SVar<ResourceEpoch<'r>> = SVar.create ()
    
  member internal __.Name = name

  /// Opens a resource, if not already open.
  /// If opening the resource fails, the resource is placed into a faulted state.
  member internal __.Open () =
    cell 
    |> SVar.putOrUpdateAsync (fun (prevEpoch:ResourceEpoch<'r> option) -> async {
      let lastVersion = prevEpoch |> Option.map (fun e -> e.version) |> Option.getOr -1      
      match prevEpoch with
      | Some ep when ep.isOpen -> 
        Log.trace "resource_already_open|type=%s last_version=%i" name lastVersion
        return ep
      | Some ep when ep.isFaulted ->
        Log.trace "resource_open_attempt_is_faulted|type=%s last_version=%i" name lastVersion
        return raise (FaultedResourceException (ep.tryGetError |> Option.getOr null))
      | _ ->
        Log.trace "opening_resource|type=%s last_version=%i" name lastVersion
        let state = IVar.create ()
        let cts = new CancellationTokenSource()
        state |> IVar.intoCancellationToken cts
        let version = lastVersion + 1
        let! res = create state.Task prevEpoch |> Async.Catch
        match res with
        | Success (r,proc) ->
          let procTask = Async.StartAsTask (proc, TaskCreationOptions.AttachedToParent, cancellationToken = cts.Token)
          procTask 
          |> Task.extend (fun t -> 
            if t.IsFaulted then 
              Log.trace "resource_loop_failed|type=%s version=%i error=\"%O\"" name version t.Exception
              IVar.tryError t.Exception state |> ignore)
          |> ignore
          return { resource = r ; state = state ; version = version ; proc = procTask ; cts = cts }
        | Failure e ->
          Log.warn "failed_to_open_resource|type=%s version=%i error=\"%O\"" name version e
          state |> IVar.tryError e |> ignore
          return { resource = Unchecked.defaultof<_> ; state = state ; version = version ; proc = Task.FromResult () ; cts = cts } })

  /// Closes the resource, if not already closed.
  member internal __.CloseFault (callingEpoch:ResourceEpoch<'r>, ex:exn option, fault:bool) =
    cell 
    |> SVar.updateAsync (fun currentEpoch -> async {        
      try
        let closed =
          if fault then currentEpoch.tryCloseOrFault ex 
          else currentEpoch.tryCloseOrFault None
        if closed then
          Log.trace "closing_resource|type=%s version=%i" name currentEpoch.version
          currentEpoch.cts.Dispose ()
          do! close (currentEpoch, ex)
          return { currentEpoch with version = currentEpoch.version + 1 ; resource = Unchecked.defaultof<_> }
        else
          Log.trace "resource_already_closed|type=%s calling_version=%i current_version=%i" 
            name callingEpoch.version currentEpoch.version
          return currentEpoch
      with ex ->
        let errMsg = sprintf "resource_close_failed|type=%s version=%i error=\"%O\"" name currentEpoch.version ex
        Log.error "%s" errMsg
        return raise (exn(errMsg, ex)) })
    |> Async.Ignore

  member internal __.Fault (callingEpoch:ResourceEpoch<'r>, ex:exn option) =
    __.CloseFault (callingEpoch, ex, true)

  member internal __.Close (callingEpoch:ResourceEpoch<'r>, ex:exn option) =
    __.CloseFault (callingEpoch, ex, false) |> Async.Ignore

  member internal __.Close (callingEpoch:ResourceEpoch<'r>) =
    __.CloseFault (callingEpoch, None, false) |> Async.Ignore

  member internal __.CloseCurrent (ex:exn option, fault:bool) = async {
    let ep = SVar.getFastUnsafe cell
    match ep with
    | None -> 
      return ()
    | Some ep when ep.isClosed -> 
      return ()
    | Some ep ->
      return! __.CloseFault (ep, ex, fault) |> Async.Ignore }

  /// Gets an instances of the resource, opening if needed.
  /// Throws: FaultedResourceException if the resource is faulted.
  member internal __.Get () = async {
    let ep = SVar.getFastUnsafe cell
    match ep with
    | Some ep when ep.isOpen ->
      return ep
    | _ ->
      let! ep = __.Open ()
      match ep.tryGetError with
      | Some ex -> return raise (FaultedResourceException ex)
      | None -> return ep }

  member internal __.States =
    cell
    |> SVar.tap
    |> AsyncSeq.map (fun ep -> 
      if not ep.state.Task.IsCompleted then 
        ResourceState.Open (ep.resource, ep.state.Task)
      else 
        match ep.tryGetError with
        | Some ex -> ResourceState.Faulted ex
        | None -> ResourceState.Closed)

/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Close the resource and return the specified result without retrying.
  | CloseResume of 'e option * 'a
    
  /// Close the resource, re-open and retry the operation.
  | CloseRetry of 'e option

  /// Retry without closing.
  | Retry of 'e option

/// Operations on resources.
module Resource =
    
  let ensureOpen (r:Resource<'r>) =
    r.Get () |> Async.Ignore

  /// Permanently closes the resource.
  let close (r:Resource<'r>) =
    r.CloseCurrent (None,true)

  /// Faults the resource, which closes it permanently.
  let fault (r:Resource<'r>) (ex:exn) =
    r.CloseCurrent (Some ex,true)
  
  let create 
    (name:string) 
    (create:Task<unit> -> ResourceEpoch<'r> option -> Async<'r * Async<unit>>) 
    (close:(ResourceEpoch<'r> * exn option) -> Async<unit>) : Async<Resource<'r>> = async {
    let r = new Resource<'r>(name, create, close)
    do! ensureOpen r
    return r }

  let get (r:Resource<'r>) =
    r.Get ()

  let getResource (r:Resource<'r>) =
    r.Get () |> Async.map (fun ep -> ep.resource)

  let private resourceError (ex:exn) =
    if isNull ex then ClosedResourceException() :> exn
    else FaultedResourceException(ex) :> exn

  let injectWithRecovery (rp:RetryPolicy) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (r:Resource<'r>) (a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      try
        let! ep = r.Get ()
        let! b = Async.cancelWithTaskThrow resourceError ep.state.Task (op ep.resource a)
        //let! b = op ep.resource a
        match b with
        | Success b -> 
          return b
        | Failure (Retry e) ->
          Log.trace "retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
            r.Name ep.version rs.attempt e
          let! rs' = RetryPolicy.awaitNextState rp rs
          match rs' with
          | None ->
            let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" r.Name ep.version rs.attempt e
            Log.trace "%s" msg
            return raise (FaultedResourceOperationException(msg, e))
          | Some rs' ->
            return! go rs'
        | Failure (CloseResume (ex,b)) ->
          Log.trace "closing_and_resuming_after_failure|type=%s v=%i attempt=%i error=\"%O\"" 
            r.Name ep.version rs.attempt ex
          do! r.Close (ep,ex)
          return b
        | Failure (CloseRetry ex) ->
          // TODO: collect errors?
          Log.trace "closing_and_retrying_after_failure|type=%s v=%i attempt=%i error=\"%O\"" 
            r.Name ep.version rs.attempt ex
          do! r.Close (ep,ex)
          let! rs' = RetryPolicy.awaitNextState rp rs
          match rs' with
          | None ->
            let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s v=%i attempt=%i error=\"%O\"" r.Name ep.version rs.attempt ex
            Log.trace "%s" msg
            return raise (FaultedResourceOperationException(msg, ex))
          | Some rs' ->          
            return! go rs'
      with ex ->
        Log.trace "unhandled_resource_exn|type=%s error=%O attempt=%i" r.Name ex rs.attempt
        return raise ex }
    go RetryState.init

  let states (r:Resource<'r>) : AsyncSeq<ResourceState<'r>> = r.States

  /// Links resources, such that state changes in the parent are propagated to the child.
  /// When the parent is closed/faulted, the child resource is also closed/faulted.
  let link (parent:Resource<'a>) (child:Resource<'b>) = 
    // TODO: catch error?
    parent
    |> states
    |> AsyncSeq.iterAsync (fun state -> async {
      match state with
      | ResourceState.Open (_,_) -> 
        return ()
      | ResourceState.Closed -> 
        do! close child
      | ResourceState.Faulted ex ->
        do! fault child ex })
    |> Async.StartAsTask