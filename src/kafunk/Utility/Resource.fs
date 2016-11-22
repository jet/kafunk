namespace Kafunk

open System
open System.Threading
open System.Threading.Tasks
open Kafunk

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
          Log.warn "closing_previous_resource_epoch|version=%i cancellation_requested=%b" prev.version prev.closed.IsCancellationRequested
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