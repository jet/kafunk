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
type FaultedResourceException (ex:exn) = inherit Exception ("FaultedResourceException", ex)

/// An exception raised when a resource-dependent operation fails and retries are depleted.
type FaultedResourceOperationException (msg:string,exn:exn) = inherit Exception (msg, exn)

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal (id:string, create:Task<unit> -> ResourceEpoch<'r> option -> Async<'r>, close:ResourceEpoch<'r> * exn option -> Async<unit>) =
  
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
        let version = lastVersion + 1
        let! res = create state.Task prevEpoch |> Async.Catch
        match res with
        | Success r ->
          return { resource = r ; state = state ; version = version }
        | Failure e ->
          Log.warn "failed_to_open_resource|type=%s version=%i error=\"%O\"" name version e
          state |> IVar.tryError e |> ignore
          return { resource = Unchecked.defaultof<_> ; state = state ; version = version } })
          //return raise (FaultedResourceException e) })

  /// Closes the resource, if not already closed.
  member internal __.Close (callingEpoch:ResourceEpoch<'r>, ex:exn option) =
    cell 
    |> SVar.updateAsync (fun currentEpoch -> async {        
      try
        if currentEpoch.tryCloseOrFault ex then
          Log.trace "closing_resource|type=%s version=%i" name currentEpoch.version
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

  member internal __.Close (callingEpoch:ResourceEpoch<'r>) =
    __.Close (callingEpoch, None)

  member internal __.CloseCurrent (ex:exn option) = async {
    let ep = SVar.getFastUnsafe cell
    match ep with
    | None -> 
      return ()
    | Some ep when ep.isClosed -> 
      return ()
    | Some ep ->
      return! __.Close (ep, ex) |> Async.Ignore }

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

  member internal __.Extend (f:Resource<'r> -> Async<'b>) =
    failwith ""


/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Close the resource and return the specified result without retrying.
  | CloseResume of 'e option * 'a
    
  /// Close the resource, re-open and retry the operation.
  | CloseRetry of 'e

  /// Retry without closing.
  | Retry of 'e


/// Operations on resources.
module Resource =
    
  let ensureOpen (r:Resource<'r>) =
    r.Get () |> Async.Ignore

  /// Closes the resource.
  let close (r:Resource<'r>) =
    r.CloseCurrent None

  /// Faults the resource.
  let fault (r:Resource<'r>) (ex:exn) =
    r.CloseCurrent (Some ex)
  
  let create 
    (name:string) 
    (create:Task<unit> -> ResourceEpoch<'r> option -> Async<'r>) 
    (close:(ResourceEpoch<'r> * exn option) -> Async<unit>) : Async<Resource<'r>> = async {
    let r = new Resource<'r>(name, create, close)
    do! ensureOpen r
    return r }

  let createChild
    (name:string) 
    (parent:Resource<'r>)
    (create:ResourceEpoch<'r> -> Task<unit> -> 'r option -> Async<'r>) 
    (close:('b * int * obj * exn) -> Async<unit>) : Async<Resource<'b>> = async {

    return failwith "" }

  let get (r:Resource<'r>) : Async<'r> =
    r.Get () |> Async.map (fun ep -> ep.resource)

  let injectWithRecovery (rp:RetryPolicy) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (r:Resource<'r>) (a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      let! ep = r.Get ()
      //let! b = op ep.resource a
      let! b = Async.cancelWithTaskThrow ep.state.Task (op ep.resource a)
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
        Log.trace "closing_and_resuming_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          r.Name ep.version rs.attempt ex
        let! _ = r.Close (ep)
        return b
      | Failure (CloseRetry ex) ->
        // TODO: collect errors?
        Log.trace "closing_and_retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          r.Name ep.version rs.attempt ex
        let! _ = r.Close (ep)
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" r.Name ep.version rs.attempt ex
          Log.trace "%s" msg
          return raise (FaultedResourceOperationException(msg, ex))
        | Some rs' ->          
          return! go rs' }
    go RetryState.init

  let states (r:Resource<'r>) : AsyncSeq<ResourceState<'r>> = r.States
