namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading.Tasks

type private MVarReq<'a> =
  | PutAsync of Async<'a> * TaskCompletionSource<'a>
  | UpdateAsync of update:('a -> Async<'a>) * TaskCompletionSource<'a>
  | PutOrUpdateAsync of update:('a option -> Async<'a>) * TaskCompletionSource<'a>
  | Get of TaskCompletionSource<'a>
  | Take of TaskCompletionSource<'a>

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
              rep.SetResult a
              return! loop a
            with ex ->
              rep.SetException ex
              return! init () })
        | PutOrUpdateAsync (update,rep) ->
          Some (async {
            try
              let! a = update None
              state <- Some a
              rep.SetResult a
              return! loop (a)
            with ex ->
              rep.SetException ex
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
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a)
      | PutOrUpdateAsync (update,rep) ->
        try
          let! a = update (Some a)
          state <- Some a
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a)
      | Get rep ->
        rep.SetResult a
        return! loop (a)
      | Take (rep) ->
        state <- None
        rep.SetResult a
        return! init ()
      | UpdateAsync (f,rep) ->
        try
          let! a = f a
          state <- Some a
          rep.SetResult a
          return! loop (a)
        with ex ->
          rep.SetException ex
          return! loop (a) }
    match a with
    | Some a ->
      state <- Some a
      return! loop (a)
    | None -> 
      return! init () })

  do mbp.Error.Add (fun x -> printfn "|MVar|ERROR|%O" x) // shouldn't happen
  
  let postAndAsyncReply f = 
    let tcs = new TaskCompletionSource<'a>()
    mbp.Post (f tcs)
    tcs.Task |> Async.AwaitTask 

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

  member __.Update (f:'a -> 'a) : Async<'a> =
    __.UpdateAsync (f >> async.Return)

  member __.UpdateAsync (update:'a -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> UpdateAsync (update, ch))

  member __.PutOrUpdateAsync (update:'a option -> Async<'a>) : Async<'a> =
    postAndAsyncReply (fun ch -> PutOrUpdateAsync (update,ch))

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
    c.Get ()

  /// Takes an item from the MVar.
  let take (c:MVar<'a>) : Async<'a> =
    c.Take ()
  
  /// Returns the last known value, if any, without serialization.
  /// NB: unsafe because the value may be null, but helpful for supporting overlapping
  /// operations.
  let getFastUnsafe (c:MVar<'a>) : 'a option =
    c.GetFast ()

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let put (a:'a) (c:MVar<'a>) : Async<'a> =
    c.Put a

  /// Puts an item into the MVar, returning the item that was put.
  /// Returns if the MVar is either empty or full.
  let putAsync (a:Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.PutAsync a

  /// Puts a new value into an MVar or updates an existing value.
  /// Returns the value that was put or the updated value.
  let putOrUpdateAsync (update:'a option -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.PutOrUpdateAsync (update)

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let update (update:'a -> 'a) (c:MVar<'a>) : Async<'a> =
    c.Update update

  /// Updates an item in the MVar.
  /// Returns when an item is available to update.
  let updateAsync (update:'a -> Async<'a>) (c:MVar<'a>) : Async<'a> =
    c.UpdateAsync update