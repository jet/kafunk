namespace Kafunk

open FSharp.Control

/// The time to wait between retries.
type WaitTimeMs = int

/// Backoff represented as a sequence of wait times, in milliseconds, between retries.
type Backoff = 
  private
  | Backoff of (int -> WaitTimeMs option)

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Backoff = 
  
  let un (Backoff s) = s

  let nu s = Backoff s

  let at (attempt:int) (b:Backoff) : int option =
    (un b) attempt

  let constant (waitMs:int) = 
    nu <| fun _ -> Some waitMs

  let linear (init:int) (increment:int) = 
    nu <| fun i -> Some (init + (i * increment))
 
  /// Returns a backoff strategy where no wait time is greater than the specified value.
  let maxWait (maxWaitMs:int) (b:Backoff) : Backoff =
    nu <| fun i -> at i b |> Option.map (max maxWaitMs)

  /// Returns a backoff strategy which attempts at most a specified number of times.
  let maxAttempts (maxAttempts:int) (b:Backoff) : Backoff =
    nu <| function i when i < maxAttempts -> b |> at i | _ -> None

  /// Returns an async sequence where each item is emitted after the corresponding backoff time
  /// has elapsed.
  let toAsyncSeq (b:Backoff) =
    AsyncSeq.unfoldAsync (fun i -> async { 
      let bo = b |> at i
      match bo with
      | Some bo -> 
        do! Async.Sleep bo
        return Some (bo,i + 1)
      | None -> 
        return None }) 0
  
  /// Returns an async computation that completes when the specified backoff wait time
  /// elapses.
  let waitAt (attempt:int) (b:Backoff) : Async<unit> =
    match at attempt b with
    | Some sleep -> Async.Sleep sleep
    | None -> Async.empty

  


/// Operations on System.Exception.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Exn =
  
  open System
  open System.Runtime.ExceptionServices

  let aggregate (msg:string) (exns:exn seq) =
    new AggregateException(msg, exns) :> exn

  let ofSeq (es:#exn seq) =
    new AggregateException(es |> Seq.cast) :> exn

  let empty<'e> = ofSeq Seq.empty

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

  let inline throwEdi (edi:ExceptionDispatchInfo) =
    edi.Throw ()
    failwith "undefined"

  let inline captureEdi (e:exn) =
    ExceptionDispatchInfo.Capture e
    


/// Fault tolerance.
module Faults =

  let retryAsyncResultReference (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    AsyncSeq.interleave 
      (AsyncSeq.replicateInfiniteAsync a |> AsyncSeq.map Choice1Of2) 
      (Backoff.toAsyncSeq b |> AsyncSeq.map Choice2Of2)
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
  let retryResultThrow (f:'e -> #exn) (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult m b a
    |> Async.map (function
      | Success a -> a
      | Failure e -> raise (f e))

  let retryResultThrowList (f:'e list -> #exn) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<'a> =
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

    let retryResultThrow (ex:'e -> #exn) (m:Monoid<'e>) (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryResultThrow ex m b (f a)) 

    let retryResultThrowList (ex:'e list -> #exn) (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryResultThrowList ex b (f a))

    let retry (b:Backoff) (f:'a -> Async<'b>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> retryAsync b (f a))

    let doAfterError (g:'a * 'e -> unit) : ('a -> Async<Result<'b, 'e>>) -> ('a -> Async<Result<'b, 'e>>) =
      AsyncFunc.mapOut (fun (a,b) -> b |> Result.mapError (fun e -> g (a,e) ; e))
      


