[<AutoOpen>]
module internal Kafunk.Resource

open System
open System.Threading
open System.Threading.Tasks
open Kafunk

/// A result of a resource-dependent operation.
type ResourceResult<'a, 'e> = Result<'a, ResourceErrorAction<'a, 'e>>

/// The action to take when a resource-dependent operation fails.
and ResourceErrorAction<'a, 'e> =
  
  /// Recover the resource and return the specified result.
  | RecoverResume of 'e * 'a
    
  /// Recover the resource and retry the operation.
  | RecoverRetry of 'e

  /// Retry the operation.
  | Retry


type internal ResourceEpoch<'r> = {
  resource : 'r
  closed : CancellationTokenSource
  version : int
}

type Resource<'r> internal (create:CancellationToken -> 'r option -> Async<'r>, handle:('r * int * obj * exn) -> Async<unit>) =
      
  let Log = Log.create "Resource"
    
  let cell : MVar<ResourceEpoch<'r>> = MVar.create ()
   
  let create (prevEpoch:ResourceEpoch<'r> option) = async {
    let closed = new CancellationTokenSource()
    let version = 
      match prevEpoch with
      | Some prev ->
        prev.closed.Cancel()
        prev.version + 1
      | None ->
        0
    let! res = create closed.Token (prevEpoch |> Option.map (fun e -> e.resource)) |> Async.Catch
    match res with
    | Success r ->
      return { resource = r ; closed = closed ; version = version }
    | Failure e ->
      return raise e }

  let recover (req:obj) (ex:exn) (callingEpoch:ResourceEpoch<'r>) = async {
    do! handle (callingEpoch.resource, callingEpoch.version, req, ex)
    let! ep' = create (Some callingEpoch)
    return ep' }

  member internal __.Get () =
    MVar.get cell |> Async.map (fun ep -> ep.resource)

  member internal __.Create () = async {
    return! cell |> MVar.putOrUpdateAsync create }

  member internal __.TryGetVersion () =
    MVar.getFastUnsafe cell |> Option.map (fun e -> e.version)

  member private __.Recover (callingEpoch:ResourceEpoch<'r>, req:obj, ex:exn) =
    let update currentEpoch = async {
      if currentEpoch.version = callingEpoch.version then
        try
          let! ep2 = recover req ex callingEpoch
          return ep2
        with ex ->
          Log.error "recovery_failed|error=%O" ex
          //do! Async.Sleep 2000
          return raise ex
      else
        Log.trace "resource_recovery_already_requested|calling_version=%i current_version=%i" callingEpoch.version currentEpoch.version
        return currentEpoch }
    cell |> MVar.updateAsync update
    
  member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
    fun a -> async {
      let! ep = MVar.get cell
      return! op ep.resource a |> Async.cancelWithToken ep.closed.Token }

  member internal __.InjectResult<'a, 'b> (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>), rp:RetryPolicy) : 'a -> Async<'b> =
    let rec go rs a = async {
      let! ep = MVar.get cell
      let! b = op ep.resource a
      match b with
      | Success b -> 
        return b
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry ex) ->
        let! _ = __.Recover (ep, a, ex)
        let! rs = RetryPolicy.awaitNext rp rs
        match rs with
        | None ->
          return raise ex
        | Some (_,rs) ->
          return! go rs a
      | Failure (Retry) ->
        let! rs = RetryPolicy.awaitNext rp rs
        match rs with
        | None ->
          return raise (exn())
        | Some (_,rs) ->
          return! go rs a }
    go RetryState.init
        
  member internal __.InjectResult<'a, 'b> (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = MVar.get cell
      let! res = op ep.resource a
      match res with
      | Success b -> 
        return b
      | Failure (RecoverResume (ex,b)) ->
        let! _ = __.Recover (ep, a, ex)
        return b
      | Failure (RecoverRetry ex) ->
        let! _ = __.Recover (ep, a, ex)
        return! go a
      | Failure (Retry) ->
        return! go a }
    return go }

  member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
    let rec go a = async {
      let! ep = MVar.get cell
      try
        return! op ep.resource a
      with ex ->
        let! _ = __.Recover (ep, box a, ex)
        return! go a }
    return go }

  interface IDisposable with
    member __.Dispose () = ()

// operations on resource monitors.
module Resource =
    
  let recoverableRecreate (create:CancellationToken -> 'r option -> Async<'r>) (handleError:('r * int * obj * exn) -> Async<unit>) = async {
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }
  
  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectResult (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.InjectResult op

  let injectWithRecovery (r:Resource<'r>) (rp:RetryPolicy) (op:'r -> ('a -> Async<Result<'b, ResourceErrorAction<'b, exn>>>)) : 'a -> Async<'b> =
    r.InjectResult (op, rp)

  let injectWithRecovery2 (r:Resource<'r>) (recovery:'b -> ResourceErrorAction<'b, exn> option) (rp:RetryPolicy) (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b> =
    let op r a = op r a |> Async.map (fun b -> match recovery b with Some r -> Failure r | None -> Success b)
    r.InjectResult (op, rp)

  let timeout (r:Resource<'r>) : ('r -> ('a -> Async<'b>)) -> ('a -> Async<'b option>) =
    r.Timeout

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)

  let get (r:Resource<'r>) : Async<'r> =
    r.Get ()