namespace Kafunk

/// Backoff represented as a sequence of wait times between retries.
type Backoff = Backoff of seq<int>

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Backoff = 
  
  let un (Backoff s) = s

  let nu s = Backoff s

  let constant (wait:int) = 
    nu <| Seq.initInfinite (fun _ -> wait)

  let linear (init:int) (increment:int) = 
    nu <| Seq.initInfinite (fun i -> init + (i * increment))

  let toAsyncSeq (b:Backoff) =
    AsyncSeq.ofSeq (un b)
    |> AsyncSeq.mapAsync (fun wait -> async {
      do! Async.Sleep wait
      return wait })

  let at (attempt:int) (b:Backoff) : int option =
    Seq.tryItem attempt (un b)

  let maxAttempts (maxAttempts:int) (b:Backoff) : Backoff =
    (un b) |> Seq.truncate maxAttempts |> nu


/// Operations on System.Exception.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Exn =
  
  open System

  let aggregate (msg:string) (exns:exn seq) =
    new AggregateException(msg, exns) :> exn

  let ofSeq (es:#exn seq) =
    new AggregateException(es |> Seq.cast) :> exn

  let empty = ofSeq Seq.empty

  let merge e1 e2 = 
    ofSeq [e1;e2]

  let rec toSeq (e:exn) =
    match e with
    | :? AggregateException as ae -> 
      seq { 
        yield e
        for ie in ae.InnerExceptions do 
          yield! toSeq ie }
    | _ -> Seq.singleton e

  let tryPick (f:exn -> 'a option) (e:exn) : 'a option =
    e |> toSeq |> Seq.tryPick f

  let exists (f:exn -> bool) (e:exn) : bool =
    e |> toSeq |> Seq.exists f

  let throw (e:exn) : 'a =
    raise e

  let existsT<'e> (e:exn) = exists (fun e -> e.GetType() = typeof<'e>) e

  let monoid : Monoid<exn> =
    Monoid.monoid empty merge


/// Fault tolerance.
module Faults =

  let retryAsyncResultReference (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    AsyncSeq.interleave (AsyncSeq.replicateInfiniteAsync a) (Backoff.toAsyncSeq b)
    |> AsyncSeq.choose Choice.tryLeft
    |> AsyncSeq.sequenceResult m

  let retryAsyncResult (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    let rec loop i e = async {
      let! r = a
      match r with
      | Success a -> 
        return Success a
      | Failure e' ->
        let e = m.Merge (e,e')
        match Backoff.at i b with
        | None -> 
          return Failure e
        | Some wait ->
          do! Async.Sleep wait
          return! loop (i + 1) e }
    loop 0 m.Zero

  let retryAsyncResultList (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    retryAsyncResult Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))
    
  /// Retries the specified async computation returning a Result.
  /// Returns the first successful value or throws an exception based on errors observed during retries.
  let retryResultThrow (f:'e -> exn) (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult m b a
    |> Async.map (function
      | Success a -> a
      | Failure e -> raise (f e))

  let retryResultThrowList (f:'e list -> exn) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryResultThrow f Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))

  let retryAsync (b:Backoff) (a:Async<'a>) : Async<'a> =
    a 
    |> Async.Catch
    |> Async.map (Result.mapError List.singleton)
    |> retryResultThrow 
        (fun es -> Exn.aggregate (sprintf "Operation failed after %i attempts." (List.length es)) es) 
        Monoid.freeList
        b
      
  module AsyncFunc =

    let retryResult (m:Monoid<'e>) (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e>> =
      fun a -> async.Delay (fun () -> retryAsyncResult m b (f a))

    let retryResultList (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e list>> =
      retryResult Monoid.freeList b (f >> Async.map (Result.mapError List.singleton))

    let retryResultThrow (ex:'e -> exn) (m:Monoid<'e>) (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryResultThrow ex m b (f a)) 

    let retryResultThrowList (ex:'e list -> exn) (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryResultThrowList ex b (f a))

    let retry (b:Backoff) (f:'a -> Async<'b>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryAsync b (f a))


