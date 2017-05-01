#load "Refs.fsx"

open Kafunk
open System
open System.Threading
open System.Threading.Tasks

type private MVarReq<'a> =
  | PutAsync of Async<'a> * IVar<'a>
  | UpdateAsync of update:('a -> Async<'a>)
  | PutOrUpdateAsync of update:('a option -> Async<'a>) * IVar<'a>
  | Get of IVar<'a>
  | Take of IVar<'a>

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
              IVar.put a rep
              return! loop a
            with ex ->
              state <- None
              IVar.error ex rep
              return! init () })
        | PutOrUpdateAsync (update,rep) ->
          Some (async {
            try
              let! a = update None
              state <- Some a
              IVar.put a rep
              return! loop (a)
            with ex ->
              state <- None
              IVar.error ex rep
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
          IVar.put a rep
          return! loop (a)
        with ex ->
          state <- Some a
          IVar.error ex rep
          return! loop (a)
      | PutOrUpdateAsync (update,rep) ->
        try
          let! a = update (Some a)
          state <- Some a
          IVar.put a rep
          return! loop (a)
        with ex ->
          state <- Some a
          IVar.error ex rep
          return! loop (a)
      | Get rep ->
        IVar.put a rep
        return! loop (a)
      | Take (rep) ->
        state <- None
        IVar.put a rep
        return! init ()
      | UpdateAsync f ->
        let! a = f a
        return! loop a }
    match a with
    | Some a ->
      state <- Some a
      return! loop (a)
    | None -> 
      return! init () })

  do mbp.Error.Add (fun x -> printfn "|MVar|ERROR|%O" x) // shouldn't happen
  
  let postAndAsyncReply f = async {
    let ivar = IVar.create ()
    mbp.Post (f ivar)
    return! IVar.get ivar }

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

  member __.UpdateStateAsync (update:'a -> Async<'a * 's>) : Async<'s> = async {
    let rep = IVar.create ()
    let up a = async {
      try
        let! (a,s) = update a
        state <- Some a
        IVar.put s rep
        return a
      with ex ->
        state <- Some a
        IVar.error ex rep
        return a  }
    mbp.Post (UpdateAsync up)
    return! IVar.get rep }

  member __.PutOrUpdateAsync (update:'a option -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutOrUpdateAsync (update,ch))

  member __.Update (f:'a -> 'a) : Async<'a> =
    __.UpdateAsync (f >> async.Return)

  member __.UpdateAsync (update:'a -> Async<'a>) : Async<'a> =
    __.UpdateStateAsync (update >> Async.map diag)

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
    async.Delay (c.Get)

  /// Takes an item from the MVar.
  let take (c:MVar<'a>) : Async<'a> =
    async.Delay (c.Take)
  
  /// Returns the last known value, if any, without serialization.
  /// NB: unsafe because the value may be null, but helpful for supporting overlapping
  /// operations.
  let getFastUnsafe (c:MVar<'a>) : 'a option =
    c.GetFast ()

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let put (a:'a) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.Put a)

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let putAsync (a:Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.PutAsync a)

  /// Puts a new value into an MVar or updates an existing value.
  /// Returns the value that was put or the updated value.
  let putOrUpdateAsync (update:'a option -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.PutOrUpdateAsync update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateStateAsync (update:'a -> Async<'a * 's>) (c:MVar<'a>) : Async<'s> =
    async.Delay (fun () -> c.UpdateStateAsync update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let update (update:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.Update update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateAsync (update:'a -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    async.Delay (fun () -> c.UpdateAsync update)

type AsyncUpdate<'s, 'u, 'a> = AU of ('s -> Async<'u * 'a>)

module AsyncUpdate =
  
  module Update =
    
    let inline unit< ^S when ^S : (static member Unit : ^S)> () : ^S = 
      (^S : (static member Unit : ^S) ()) 

    let inline merge< ^S when ^S : (static member Merge : ^S * ^S -> ^S )> a b : ^S =
      (^S : (static member Merge : ^S * ^S -> ^S) (a, b)) 

    let inline apply< ^S, ^U when ^U : (static member Apply : ^S * ^U -> ^S )> s a : ^S =
      (^U : (static member Apply : ^S * ^U -> ^S) (s, a)) 

  let run (AU(f)) a = f a

  let inline unit (a:'a) : AsyncUpdate<'s, 'u, 'a> = 
    AU (fun _ -> async.Return(Update.unit(), a))

  let map (f:'a -> 'b) (u:AsyncUpdate<'s, 'u, 'a>) : AsyncUpdate<'s, 'u, 'b> =
    AU <| fun s -> async {
      let! (u,a) = run u s
      return (u, f a) }

  let inline bind (f:'a -> AsyncUpdate<'s, 'u, 'b>) (u:AsyncUpdate<'s, 'u, 'a>) : AsyncUpdate<'s, 'u, 'b> =
    AU <| fun s -> async {
      let! (u,a) = (run u) s
      let s' = Update.apply s u
      let! (u',b) = (run (f a)) s'
      return (Update.merge u u', b) }

  let inline bindAsync (f:'a -> AsyncUpdate<'s, 'u, 'b>) (a:Async<'a>) : AsyncUpdate<'s, 'u, 'b> =
    AU <| fun s -> async {
      let! a = a
      let! (u,b) = run (f a) s
      return u,b }

  let inline zipWith (f:'a -> 'b -> 'c) (a:AsyncUpdate<'s, 'u, 'a>) (b:AsyncUpdate<'s, 'u, 'b>) : AsyncUpdate<'s, 'u, 'c> =
    AU <| fun s -> async {
      let! (u1,a) = (run a) s
      let! (u2,b) = (run b) s
      return (Update.merge u1 u2, f a b) }

  let inline zip (a:AsyncUpdate<'s, 'u, 'a>) (b:AsyncUpdate<'s, 'u, 'b>) : AsyncUpdate<'s, 'u, 'a * 'b> =
    zipWith (fun a b -> a,b) a b

  let inline ap (f:AsyncUpdate<'s, 'u, 'a -> 'b>) (b:AsyncUpdate<'s, 'u, 'a>) : AsyncUpdate<'s, 'u, 'b> =
    zipWith (fun f a -> f a) f b

  type Builder () =
    member inline __.Return a = unit a
    member inline __.ReturnFrom a = a
    member inline __.Bind (u,f) = bind f u
    member inline __.Bind (u,f) = bindAsync f u

let asyncUpdate = AsyncUpdate.Builder ()


type MVarUpdate<'a> =
  | NoUpdate
  | Update of ('a -> 'a)
  with
    static member Unit = NoUpdate
    static member Merge (u1, u2) =
      match u1, u2 with
      | NoUpdate, _ -> u2
      | _, NoUpdate -> u1
      | Update f, Update g -> Update (f >> g)
    static member Apply (s,u) =
      match u with
      | NoUpdate -> s
      | Update f -> f s

module MVarUpdate =
  
  let update (f:'a -> 'a) : AsyncUpdate<'s, MVarUpdate<'a>, 's> =
    AU (fun s -> async { return MVarUpdate.Update f, s })

  let write (a:'a) : AsyncUpdate<'s, MVarUpdate<'a>, 's> =
    update (fun _ -> a)

  let read : AsyncUpdate<'s, MVarUpdate<'a>, 's> =
    AU (fun s -> async { return MVarUpdate.NoUpdate, s })

  let inline run (c:MVar<'a>) (u:AsyncUpdate<'a, 'u, 's>) =
    c |> MVar.updateStateAsync (fun a -> async {
      let! (u,s) = AsyncUpdate.run u a
      let a' = AsyncUpdate.Update.apply a u
      return a',s })
  
open MVarUpdate

let testUpdate = asyncUpdate {
  let! (x:int) = read
  do! Async.Sleep x
  let! _ = update (fun x -> x + 1)
  let! z = update (fun x -> x + 2)
  return z }

let testRead<'a> : AsyncUpdate<'a, MVarUpdate<_>, 'a> = asyncUpdate {
  return! read }

let mvar = MVar.createFull 100

MVarUpdate.run mvar testUpdate |> Async.RunSynchronously

let x = MVar.get mvar |> Async.RunSynchronously 
let y = MVarUpdate.run mvar testRead |> Async.RunSynchronously

printfn "%A" x
printfn "%A" y