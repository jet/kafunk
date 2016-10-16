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

  let ofSeq (es:exn seq) =
    new AggregateException(es) :> exn

  let empty = ofSeq Seq.empty

  let merge e1 e2 = 
    ofSeq [e1;e2]

  let rec toSeq (e:exn) =
    match e with
    | :? AggregateException as ae -> ae.InnerExceptions |> Seq.collect (toSeq)
    | _ -> Seq.singleton e

  let tryPick (f:exn -> 'a option) (e:exn) : 'a option =
    e |> toSeq |> Seq.tryPick f

  let exists (f:exn -> bool) (e:exn) : bool =
    e |> toSeq |> Seq.exists f

  let throw (e:exn) : 'a =
    raise e


/// Fault tolerance.
module Faults =

  let retryAsyncResultReference (b:Backoff) (a:Async<Result<'a, 'e>>) =
    (AsyncSeq.replicateInfiniteAsync a, Backoff.toAsyncSeq b)
    ||> AsyncSeq.interleave 
    |> AsyncSeq.choose (function Choice1Of2 a -> Some a | _ -> None)
    |> AsyncSeq.sequenceResult

  let retryAsyncResult (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    let rec loop i (es:'e list) = async {
      let! r = a
      match r with
      | Success a -> 
        return Success a
      | Failure e ->
        let es = e::es
        match Backoff.at i b with
        | None -> 
          return Failure es
        | Some wait ->
          do! Async.Sleep wait
          return! loop (i + 1) es }
    loop 0 []

  /// Retries the specified async computation returning a Result.
  /// Returns the first successful value or throws an exception based on errors observed during retries.
  let retryResultThrow (f:'e list -> exn) (b:Backoff) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult b a
    |> Async.map (function
      | Success a -> a
      | Failure es -> raise (f es))

  let retryAsync (b:Backoff) (a:Async<'a>) : Async<'a> =
    a 
    |> Async.Catch
    |> retryResultThrow 
        (fun es -> Exn.aggregate (sprintf "Operation failed after %i attempts." (List.length es)) es) 
        b 
      

module AsyncFun =
  
  let retryResult (b:Backoff) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e list>> =
    fun a -> async.Delay (fun () -> Faults.retryAsyncResult b (f a))

  let retry (b:Backoff) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async.Delay (fun () -> Faults.retryAsync b (f a))


