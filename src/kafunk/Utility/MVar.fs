[<AutoOpen>]
module internal Kafunk.MVar

// TODO: https://github.com/fsprojects/FSharpx.Async

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