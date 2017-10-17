[<AutoOpen>]
module internal Kafunk.Resource

open System
open System.Threading
open Kafunk

/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Recover the resource and return the specified result without retrying.
  | RecoverResume of 'e * 'a
    
  /// Recover the resource and retry the operation.
  | RecoverRetry of 'e

  /// Retry without recovery.
  | Retry of 'e

/// A generation of a resource lifecycle.
type internal ResourceEpoch<'r> = {
  resource : 'r
  closed : CancellationTokenSource
  version : int
}

/// A recoverable resource, supporting resource-dependant operations.
type Resource<'r> internal (create:CancellationToken -> 'r option -> Async<'r>, close:('r * int * obj * exn) -> Async<unit>) =
  
  let name = typeof<'r>.Name    
  let Log = Log.create "Kafunk.Resource"
  let cell : MVar<ResourceEpoch<'r>> = MVar.create ()  
  let recoveryTimeout = TimeSpan.FromSeconds 60.0

  let create (prevEpoch:ResourceEpoch<'r> option) = async {
    match prevEpoch with
    | Some ep when not ep.closed.IsCancellationRequested -> return ep
    | _ ->
      let closed = new CancellationTokenSource()
      let version = 
        match prevEpoch with
        | Some prev ->
          prev.version + 1
        | None ->
          0
      let! res = create closed.Token (prevEpoch |> Option.map (fun e -> e.resource)) |> Async.Catch
      match res with
      | Success r ->
        return { resource = r ; closed = closed ; version = version }
      | Failure e ->
        Log.warn "failed_to_create_resource|type=%s version=%i error=\"%O\"" name version e
        return raise e }

  let recover (req:obj) (ex:exn) (callingEpoch:ResourceEpoch<'r>) = async {
    if callingEpoch.closed.IsCancellationRequested then
      return ()
    else
      callingEpoch.closed.Cancel ()
      do! close (callingEpoch.resource, callingEpoch.version, req, ex)
      return () }

  member internal __.Get () = async {
    let ep = MVar.getFastUnsafe cell
    match ep with
    | None -> 
      let! ep' = __.Create ()
      return ep'
    | Some ep when ep.closed.IsCancellationRequested ->
      let! ep' = __.Create ()
      return ep'
    | Some ep ->
      return ep }
    
  member internal __.Create () = async {
    return! 
      cell 
      |> MVar.putOrUpdateAsync create }

  member private __.Recover (callingEpoch:ResourceEpoch<'r>, req:obj, ex:exn) =
    cell 
    |> MVar.updateAsync (fun currentEpoch -> async {
      if currentEpoch.version = callingEpoch.version then
        try
          do! recover req ex callingEpoch
          return currentEpoch
        with ex ->
          let errMsg = sprintf "recovery_failed|type=%s version=%i error=\"%O\"" name currentEpoch.version ex
          Log.error "%s" errMsg
          return raise (exn(errMsg, ex))
      else
        Log.trace "resource_already_recovered|type=%s calling_version=%i current_version=%i" 
          name callingEpoch.version currentEpoch.version
        return currentEpoch })
    |> Async.timeoutWith id (fun () -> failwithf "resource_recovery_timed_out") recoveryTimeout
    
  member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
    fun a -> async {
      let! ep = __.Get ()
      return! op ep.resource a |> Async.cancelWithToken ep.closed.Token }

  member internal __.InjectResult<'a, 'b> (op:'r -> 'a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>, rp:RetryPolicy, a:'a) : Async<'b> =
    let rec go (rs:RetryState) = async {
      let! ep = __.Get ()
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (Retry e) ->
        Log.trace "retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          return! go rs'
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry e) ->
        Log.trace "recovering_and_retrying_after_failure|type=%s version=%i attempt=%i error=\"%O\"" 
          name ep.version rs.attempt e
        let! rs' = RetryPolicy.awaitNextState rp rs
        match rs' with
        | None ->
          let msg = sprintf "escalating_after_retry_attempts_depleted|type=%s version=%i attempt=%i error=\"%O\"" name ep.version rs.attempt e
          Log.trace "%s" msg
          return raise (exn(msg, e))
        | Some rs' ->
          let! _ = __.Recover (ep, a, e)
          return! go rs' }
    go RetryState.init
        
  member internal __.InjectResult<'a, 'b> (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = __.Get ()
      let! res = op ep.resource a
      match res with
      | Success b -> 
        return b
      | Failure (Retry _) ->
        return! go a
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry ex) ->
        let! _ = __.Recover (ep, a, ex)
        return! go a }
    return go }

  member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = __.Get ()
      try
        return! op ep.resource a
      with ex ->
        let! _ = __.Recover (ep, box a, ex)
        return! go a }
    return go }

  interface IDisposable with
    member __.Dispose () = ()

/// Operations on resources.
module Resource =
    
  let recoverableRecreate (create:CancellationToken -> 'r option -> Async<'r>) (handleError:('r * int * obj * exn) -> Async<unit>) = async {
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }
  
  let get (r:Resource<'r>) : Async<'r> =
    r.Get () |> Async.map (fun ep -> ep.resource)

  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectResult (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.InjectResult op

  let injectWithRecovery (r:Resource<'r>) (rp:RetryPolicy) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) (a:'a) : Async<'b> =
    r.InjectResult (op, rp, a)

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)