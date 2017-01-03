namespace Kafunk

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
   
    let create (prevEpoch:Epoch<'r> option) = async {
      let version = 
        match prevEpoch with
        | Some prev ->
          //Log.warn "closing_previous_resource_epoch|version=%i cancellation_requested=%b" prev.version prev.closed.IsCancellationRequested
          prev.closed.Cancel()
          prev.version + 1
        | None ->
          0
      let! r = create |> Async.Catch
      match r with
      | Success r ->
        //Log.info "created_resource|version=%i" version
        return { resource = r ; closed = new CancellationTokenSource() ; version = version }
      | Failure e ->
        //Log.error "resource_creation_failed|error=%O" e
        return raise e }

    let recover (req:obj) (ex:exn) (ep:Epoch<'r>) = async {
      let! recovery = handle (ep.resource, ep.version, req, ex)
      match recovery with
      | Escalate ->
        return raise ex
      | Recreate ->
        let! ep' = create (Some ep)
        return ep' }

    member internal __.Get () =
      MVar.get cell |> Async.map (fun ep -> ep.resource)

    member internal __.Create () = async {
      return! cell |> MVar.putOrUpdateAsync create }

    member internal __.TryGetVersion () =
      MVar.getFastUnsafe cell |> Option.map (fun e -> e.version)

    member private __.Recover (callingEpoch:Epoch<'r>, req:obj, ex:exn) =
      let update currentEpoch = async {
        if currentEpoch.version = callingEpoch.version then
          try
            let! ep2 = recover req ex callingEpoch
            return ep2
          with ex ->
            Log.error "recovery_failed|error=%O" ex
            do! Async.Sleep 2000
            return raise ex
        else
          Log.trace "resource_recovery_already_requested|calling_version=%i current_version=%i" callingEpoch.version currentEpoch.version
          return currentEpoch }
      cell |> MVar.updateAsync update
    
    member internal __.Timeout<'a, 'b> (op:'r -> ('a -> Async<'b>)) : 'a -> Async<'b option> =
      fun a -> async {
        let! ep = MVar.get cell
        return! op ep.resource a |> Async.cancelWithToken ep.closed.Token }
        
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
          return! go a }
      return go }

    member internal __.Inject<'a, 'b> (op:'r -> ('a -> Async<'b>)) : Async<'a -> Async<'b>> = async {
      let rec go a = async {
        let! ep = MVar.get cell
        try
          return! op ep.resource a
//          let! res = op ep.resource a |> Async.cancelWithToken ep.closed.Token
//          match res with
//          | Some res ->
//            return res
//          | None ->
//            return! go a
        with ex ->
          let! _ = __.Recover (ep, box a, ex)
          return! go a }
      return go }

    interface IDisposable with
      member __.Dispose () = ()
    
  let recoverableRecreate (create:Async<'r>) (handleError:('r * int * obj * exn) -> Async<Recovery>) = async {
    let r = new Resource<_>(create, handleError)
    let! _ = r.Create()
    return r }

  /// Injects a resource into a resource-dependent computation.
  /// Returns a computation with the resource injected.
  /// Exceptions raised by the computation are caught and recovered using the recovery strategy associated
  /// with the resource.
  let inject (op:'r -> ('a -> Async<'b>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.Inject op

  let injectResult (op:'r -> ('a -> Async<ResourceResult<'b, exn>>)) (r:Resource<'r>) : Async<'a -> Async<'b>> =
    r.InjectResult op

  let timeout (r:Resource<'r>) : ('r -> ('a -> Async<'b>)) -> ('a -> Async<'b option>) =
    r.Timeout

  let timeoutIndep (r:Resource<'r>) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
    r.Timeout (fun _ -> f)