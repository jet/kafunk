namespace Kafunk

open System
open FSharp.Control

/// The state of a retry workflow.
[<StructuredFormatDisplay("RetryState({attempt})")>]
type RetryState =
  struct
    val attempt : int
    new (a) = { attempt = a }
  end

/// Retry policy.
type RetryPolicy = 
  private
  /// Given an attempt number, returns the delay or None if done.
  | RP of (RetryState -> TimeSpan option)

/// Operations on retry policies.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RetryPolicy = 
  
  let private un (RP f) = f
  
  /// Creates a retry policy.
  let create f = RP f

  /// The initial retry state.
  let initState = RetryState(0)

  /// Returns the next retry state.
  let nextState (s:RetryState) = RetryState (s.attempt + 1)

  /// No retry.
  let none = create (konst None)

  /// Returns the delay for the specified retry state.
  let delayAt (s:RetryState) (b:RetryPolicy) : TimeSpan option =
    (un b) s

  /// Returns an unbounded retry policy with a constant delay of the specified duration.
  let constant (delay:TimeSpan) = 
    create <| fun _ -> Some delay

  /// Returns an unbounded retry policy with a constant delay of the specified duration.
  let constantMs (delayMs:int) =
    constant (TimeSpan.FromMilliseconds delayMs)

  /// Returns an unbounded retry policy with a linearly increasing delay.
  let linear (init:TimeSpan) (increment:TimeSpan) = 
    create <| fun s -> Some (init + (TimeSpan.Mutiply increment s.attempt))
 
  /// Returns a backoff strategy where no wait time is greater than the specified value.
  let maxDelay (maxDelay:TimeSpan) (b:RetryPolicy) : RetryPolicy =
    create <| fun s -> delayAt s b |> Option.map (max maxDelay)

  /// Returns a backoff strategy which attempts at most a specified number of times.
  let maxAttempts (maxAttempts:int) (p:RetryPolicy) : RetryPolicy =
    create <| fun s -> if s.attempt <= maxAttempts then delayAt s p else None

  /// Returns an async sequence where each item is emitted after the corresponding delay
  /// has elapsed.
  let delayStreamAt (p:RetryPolicy) =
    AsyncSeq.unfoldAsync (fun s -> async { 
      let delay = delayAt s p
      match delay with
      | Some delay -> 
        do! Async.Sleep delay
        return Some (delay, nextState s)
      | None -> 
        return None })

  /// Returns an async sequence where each item is emitted after the corresponding delay
  /// has elapsed.
  let delayStream (p:RetryPolicy) =
    delayStreamAt p initState


/// A retry queue.
[<StructuredFormatDisplay("RetryQueue({items})")>]
type RetryQueue<'k, 'a when 'k : comparison> = private {
  policy : RetryPolicy
  key : 'a -> 'k
  items : Map<'k, RetryState * DateTime * 'a> }

/// Operations on RetryQueue.
[<Compile(Module)>]
module RetryQueue =
  
  /// Creates a RetryQueue with the specified RetryPolicy.
  let create p key = { policy = p ; key = key ; items = Map.empty }

  /// Gets the set of items and due times in the queue.
  let items (q:RetryQueue<'k, 'a>) : seq<'a * DateTime> =
    q.items |> Seq.map (fun kvp -> let (_,due,a) = kvp.Value in a,due)
    
  /// Enqueues an item for retry. If the item is already in the queue,
  /// the next RetryState is used to determine the due time, otherwise,
  /// the initial RetryState is used.
  let retryAt (q:RetryQueue<'k, 'a>) (dt:DateTime) (a:'a) =
    let k = q.key a
    let s =
      match q.items |> Map.tryFind k with
      | None -> RetryPolicy.initState
      | Some (s,_,_) -> RetryPolicy.nextState s
    match RetryPolicy.delayAt s q.policy with
    | Some delay -> { q with items = q.items |> Map.add k (s, dt.Add delay, a) }
    | None -> q

  /// Enqueues an item for retry. If the item is already in the queue,
  /// the next RetryState is used to determine the due time, otherwise,
  /// the initial RetryState is used.
  let retry (q:RetryQueue<'k, 'a>) a =
    retryAt q DateTime.UtcNow a

  let retryAll q xs = (q,xs) ||> Seq.fold retry

  /// Returns all items in the queue due at the specified time.
  let dueAt (q:RetryQueue<'k, 'a>) (dt:DateTime) =
    q
    |> items
    |> Seq.choose (fun (a,due) ->
      if dt >= due then Some a
      else None)

  /// Returns all items in the queue due at DateTime.UtcNow.
  let dueNow (q:RetryQueue<'k, 'a>) = dueAt q (DateTime.UtcNow)

  /// Returns an async computation which completes when all current queued items are due.
  let dueNowAwait (q:RetryQueue<'k, 'a>) = async {
    let now = DateTime.UtcNow
    let latestDue = items q |> Seq.map snd |> Seq.max
    if now >= latestDue then 
      return dueAt q now
    else
      do! Async.Sleep (latestDue - now)
      return dueAt q now }

  /// Removes an item from the queue, resetting its retry state.
  let remove (q:RetryQueue<'k, 'a>) k =
    { q with items = Map.remove k q.items }

  let removeAll q ks = (q,ks) ||> Seq.fold remove

  let retryRemoveAll q retryItems removeItems =
    let q' = retryAll q retryItems 
    removeAll q' removeItems
  




  


/// Operations on System.Exception.
[<Compile(Module)>]
module Exn =
  
  open System
  open System.Runtime.ExceptionServices

  let rec toSeq (e:exn) =
    match e with
    | :? AggregateException as ae -> 
      seq {
        for ie in ae.InnerExceptions do 
          yield! toSeq ie }
    | _ -> 
      Seq.singleton e

  let ofSeq (es:#exn seq) =
    new AggregateException(es |> Seq.collect toSeq) :> exn

  let monoid : Monoid<exn> =
    Monoid.monoid 
      (ofSeq Seq.empty) 
      (fun e1 e2 -> ofSeq [e1;e2])

//  let isCritical (e:exn) =
//    match e with
//    | :? OutOfMemoryException | :? StackOverflowException -> true
//    | _ -> false

  let inline throwEdi (edi:ExceptionDispatchInfo) =
    edi.Throw ()
    failwith "undefined"

  let inline captureEdi (e:exn) =
    ExceptionDispatchInfo.Capture e

  let inline upCast (e:#exn) : exn = upcast e
    


/// Fault tolerance.
module Faults =

  let private Log = Log.create "Kafunk.Faults"

  /// Returns a stream of async computations corresponding to invocations of the argument 
  /// computation until the computations succeeds.
  let retryAsyncResultStream (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : AsyncSeq<Result<'a, 'e>> =
    (AsyncSeq.replicateInfiniteAsync a, RetryPolicy.delayStream p)
    ||> AsyncSeq.interleaveChoice
    |> AsyncSeq.choose Choice.tryLeft

  let retryAsyncResultRef (m:Monoid<'e>) (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    retryAsyncResultStream p a |> AsyncSeq.sequenceResult m
    
  /// Retries an async computation returning a result according to the specified backoff strategy.
  /// Returns an async computation containing a result, which is Success of a pair of the successful result and errors
  /// accumulated during retries, if any, and otherwise, a Failure of accumulated errors.
  let retryAsyncResultWarn (m:Monoid<'e>) (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a * 'e, 'e>> =
    retryAsyncResultStream p a |> AsyncSeq.sequenceResultWarn m

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
          return! loop (RetryPolicy.nextState i) e }
    loop RetryPolicy.initState m.Zero

  /// Retries an async computation using the specified retry policy until the shouldRetry condition returns false.
  /// Returns None when shouldRetry returns false.
  let retryAsync (p:RetryPolicy) (shouldRetry:'a -> bool) (a:Async<'a>) : Async<'a option> =
    let a = a |> Async.map (fun a -> if shouldRetry a then Failure (Some a) else Success (Some a))
    retryAsyncResult 
      Monoid.optionLast
      p 
      a
    |> Async.map (Result.trySuccess >> Option.bind id)

  let retryAsyncResultList (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    retryAsyncResult Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))
    
  /// Retries the specified async computation returning a Result.
  /// Returns the first successful value or throws an exception based on errors observed during retries.
  let retryResultThrow (f:'e -> #exn) (m:Monoid<'e>) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult m b a |> Async.map (Result.throwMap f)

  let retryResultThrowList (f:'e list -> #exn) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryResultThrow f Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))

  module AsyncFunc =

    let retryAsync (shouldRetry:'a * 'b -> bool) (p:RetryPolicy) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
      fun a -> async.Delay (fun () -> f a) |> retryAsync p (fun b -> shouldRetry (a,b))

    let retryResult (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResult m p

    let retryResultList (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e list>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultList p

    let retryResultWarn (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b * 'e, 'e>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultWarn m p

    let retryResultWarnList (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b * 'e list, 'e list>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultWarnList p

    let retryResultThrow (ex:'e -> #exn) (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrow ex m p

    let retryResultThrowList (ex:'e list -> #exn) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrowList ex p
    
    let doAfterError (g:'a * 'e -> unit) : ('a -> Async<Result<'b, 'e>>) -> ('a -> Async<Result<'b, 'e>>) =
      AsyncFunc.mapOut (fun (a,b) -> b |> Result.mapError (fun e -> g (a,e) ; e))
      


