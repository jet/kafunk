namespace Kafunk

open FSharp.Control

/// Retry policy.
type RetryPolicy = 
  private
  /// Given an attempt number, returns the delay or None if done.
  | RP of (int -> int option)

/// Operations on retry policies.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RetryPolicy = 
  
  let un (RP s) = s

  let nu s = RP s

  let delayAt (attempt:int) (b:RetryPolicy) : int option =
    (un b) attempt

  let constant (waitMs:int) = 
    nu <| fun _ -> Some waitMs

  let linear (init:int) (increment:int) = 
    nu <| fun i -> Some (init + (i * increment))
 
  /// Returns a backoff strategy where no wait time is greater than the specified value.
  let maxDelay (maxDelayMs:int) (b:RetryPolicy) : RetryPolicy =
    nu <| fun i -> delayAt i b |> Option.map (max maxDelayMs)

  /// Returns a backoff strategy which attempts at most a specified number of times.
  let maxAttempts (maxAttempts:int) (p:RetryPolicy) : RetryPolicy =
    nu <| function i when i <= maxAttempts -> p |> delayAt i | _ -> None

  /// Returns an async sequence where each item is emitted after the corresponding backoff time
  /// has elapsed.
  let delayToAsyncSeq (p:RetryPolicy) =
    AsyncSeq.unfoldAsync (fun i -> async { 
      let bo = p |> delayAt i
      match bo with
      | Some bo -> 
        do! Async.Sleep bo
        return Some (bo,i + 1)
      | None -> 
        return None }) 0

  


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

  let private Log = Log.create "Kafunk.Faults"

//  let retryAsyncResultReference (m:Monoid<'e>) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
//    AsyncSeq.interleaveChoice
//      (AsyncSeq.replicateInfiniteAsync a) 
//      (Backoff.toAsyncSeq b)
//    |> AsyncSeq.choose Choice.tryLeft
//    |> AsyncSeq.sequenceResult m

//  let retry (p:RetryPolicy) (shouldRetry:'a -> bool) (a:Async<'a>) : Async<'a option> =
//    let rec loop i = async {
//      let! a = a
//      if not (shouldRetry a) then
//        return Some a
//      else
//        match RetryPolicy.delayAt i p with
//        | None -> 
//          return None
//        | Some wait ->
//          do! Async.Sleep wait
//          return! loop (i + 1) }
//    loop 0

  /// Retries an async computation returning a result according to the specified backoff strategy.
  /// Returns an async computation containing a result, which is Success of a pair of the successful result and errors
  /// accumulated during retries, if any, and otherwise, a Failure of accumulated errors.
  let retryAsyncResultWarn (m:Monoid<'e>) (policy:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a * 'e, 'e>> =
    AsyncSeq.interleaveChoice 
      (AsyncSeq.replicateInfiniteAsync a)
      (RetryPolicy.delayToAsyncSeq policy)
    |> AsyncSeq.choose Choice.tryLeft
    |> AsyncSeq.sequenceResultWarn m

  let retryAsyncResultWarnList (policy:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a * 'e list, 'e list>> =
    retryAsyncResultWarn Monoid.freeList policy (a |> Async.map (Result.mapError List.singleton))

  let retryAsyncResult (m:Monoid<'e>) (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    let rec loop i e = async {
      let! r = a
      match r with
      | Success a -> 
        return Success a
      | Failure e' ->
        let e = m.Merge (e,e')
        match RetryPolicy.delayAt i p with
        | None -> 
          return Failure e
        | Some wait ->
          do! Async.Sleep wait
          return! loop (i + 1) e }
    loop 0 m.Zero

  /// Retries an async computation using the specified retry policy until the shouldRetry condition returns false.
  /// Returns None when shouldRetry returns false.
  let retryAsync (p:RetryPolicy) (shouldRetry:'a -> bool) (a:Async<'a>) : Async<'a option> =
    let a = a |> Async.map (fun a -> if shouldRetry a then Failure (Some a) else Success (Some a))
    retryAsyncResult 
      Monoid.optionLast
      p 
      a
    |> Async.map (Result.codiag)

  let retryAsyncResultList (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    retryAsyncResult Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))
    
  /// Retries the specified async computation returning a Result.
  /// Returns the first successful value or throws an exception based on errors observed during retries.
  let retryResultThrow (f:'e -> #exn) (m:Monoid<'e>) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult m b a |> Async.map (Result.throwMap f)

  let retryResultThrowList (f:'e list -> #exn) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryResultThrow f Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))

//  let retryAsync (b:RetryPolicy) (a:Async<'a>) : Async<'a> =
//    a 
//    |> Async.Catch
//    |> Async.map (Result.mapError List.singleton)
//    |> retryResultThrow 
//        (fun es -> Exn.aggregate (sprintf "Operation failed after %i attempts." (List.length es)) es) 
//        Monoid.freeList
//        b
      
  module AsyncFunc =

    let retry (shouldRetry:'b -> bool) (p:RetryPolicy) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
      fun a -> async.Delay (fun () -> f a) |> retryAsync p shouldRetry

    let retryResult (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResult m p

    let retryResultList (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e list>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultList p

    let retryResultWarn (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b * 'e, 'e>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultWarn m p

    let retryResultWarnList (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b * 'e list, 'e list>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultWarnList p

    let retryResultThrow (ex:'e -> exn) (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrow ex m p

    let retryResultThrowList (ex:'e list -> exn) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrowList ex p

    let doAfterError (g:'a * 'e -> unit) : ('a -> Async<Result<'b, 'e>>) -> ('a -> Async<Result<'b, 'e>>) =
      AsyncFunc.mapOut (fun (a,b) -> b |> Result.mapError (fun e -> g (a,e) ; e))
      


