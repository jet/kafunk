namespace Kafunk

open System
open FSharp.Control
open System.Threading
open System.Threading.Tasks

/// Operations on System.Exception.
[<Compile(Module)>]
module internal Exn =
  
  open System
  open System.Runtime.ExceptionServices

  /// Expands all inner exceptions, including for AggregateException.
  let rec private toSeq (e:exn) =
    match e with
    | :? AggregateException as ae -> 
      seq {
        for ie in ae.InnerExceptions do 
          yield! toSeq ie }
    | _ -> 
      if isNull e.InnerException then 
        Seq.singleton e
      else
        seq {
          yield e
          yield! toSeq e.InnerException }

  /// Creates an aggregate exception, ensuring to flatten contiguous AggregateException
  /// instances.
  let ofSeq (es:#exn seq) =
    new AggregateException(es |> Seq.collect toSeq) :> exn

  /// For convenience, let exceptions form a monoid.
  let monoid : Monoid<exn> =
    Monoid.monoid 
      (ofSeq Seq.empty) 
      (fun e1 e2 -> ofSeq [e1;e2])

  let inline throwEdi (edi:ExceptionDispatchInfo) =
    edi.Throw ()
    failwith "undefined"

  let inline captureEdi (e:exn) =
    ExceptionDispatchInfo.Capture e

  let rec tryFindByType<'t when 't :> exn> (e:exn) =
    if e.GetType () = typeof<'t> then Some (e :?> 't)
    else 
      match e with
      | :? AggregateException as ae ->
        ae.InnerExceptions |> Seq.tryPick (tryFindByType<'t>)
      | _ -> None

/// The state of a retry workflow.
[<StructuredFormatDisplay("RetryState({attempt})")>]
type RetryState =
  struct
    /// The attempt number.
    val attempt : int
    new (a) = { attempt = a }
  end

/// Operations on retry states.
[<Compile(Module)>]
module RetryState = 

  /// The initial retry state with attempt = 1.
  let init = RetryState(1)

  /// Returns the next retry state.
  let internal next (s:RetryState) = RetryState (s.attempt + 1)


/// Retry policy.
type RetryPolicy = 
  private
  /// Given an attempt number, returns the delay or None if done.
  | RP of (RetryState -> TimeSpan option)

/// Operations on retry policies.
[<Compile(Module)>]
module RetryPolicy = 
  
  let private un (RP f) = f
  
  /// Creates a retry policy.
  let create f = RP f

  /// Returns the delay for the specified retry state.
  let delayAt (s:RetryState) (b:RetryPolicy) : TimeSpan option =
    (un b) s

  /// Returns a sequence of delays resulting from the specified retry policy, starting
  /// at the specified retry state.
  let delaysFrom (s:RetryState) (p:RetryPolicy) : seq<TimeSpan> =
    s |> Seq.unfold (fun s -> delayAt s p |> Option.map (fun d -> d, RetryState.next s))

  /// Returns a sequence of delays resulting from the specified retry policy, starting
  /// at the initial retry state.
  let delays (p:RetryPolicy) : seq<TimeSpan> =
    delaysFrom RetryState.init p

  /// Returns a backoff strategy where no wait time is greater than the specified value.
  let maxDelay (maxDelay:TimeSpan) (b:RetryPolicy) : RetryPolicy =
    create <| fun s -> delayAt s b |> Option.map (max maxDelay)

  /// Returns a backoff strategy which attempts the operation at most a specified number of times.
  let maxAttempts (maxAttempts:int) (p:RetryPolicy) : RetryPolicy =
    if maxAttempts < 1 then invalidArg "maxAttempts" "must be greater than or equal to 1"
    create <| fun s -> if s.attempt < maxAttempts then delayAt s p else None

  /// Returns an async computation which waits for the delay at the specified retry state and returns
  /// the delay and next state, or None if the retries have stopped.
  let awaitNext (p:RetryPolicy) (s:RetryState) : Async<(TimeSpan * RetryState) option> = async {
    match delayAt s p with
    | None -> 
      return None
    | Some delay -> 
      do! Async.sleep delay
      return Some ((delay, RetryState.next s)) }

  /// Returns an async computation which waits for the delay at the specified retry state and returns
  /// the delay and next state, or None if the retries have stopped.
  let awaitNextState (p:RetryPolicy) (s:RetryState) : Async<RetryState option> =
    awaitNext p s |> Async.map (Option.map snd)

  /// Returns an async sequence where each item is emitted after the corresponding delay
  /// has elapsed.
  let delayStreamAt (p:RetryPolicy) =
    AsyncSeq.unfoldAsync (awaitNext p)

  /// Returns an async sequence where each item is emitted after the corresponding delay
  /// has elapsed.
  let delayStream (p:RetryPolicy) =
    delayStreamAt p RetryState.init


  /// No retry.
  let none = create (konst None)

  /// Infinite retries.
  let infinite = create (konst (Some TimeSpan.Zero))

  /// Returns an unbounded retry policy with a constant delay of the specified duration.
  let constant (delay:TimeSpan) = 
    create <| fun _ -> Some delay

  /// Returns an unbounded retry policy with a constant delay of the specified duration.
  let constantMs (delayMs:int) =
    constant (TimeSpan.FromMilliseconds delayMs)

  /// Returns a bounded retry policy with a constant delay of the specified duration.
  let constantBounded (delay:TimeSpan) (attempts:int) =
    constant delay |> maxAttempts attempts

  /// Returns a bounded retry policy with a constant delay of the specified duration.
  let constantBoundedMs (delayMs:int) (attempts:int) =
    constantMs delayMs |> maxAttempts attempts

  /// Returns an unbounded retry policy with a linearly increasing delay.
  let linear (init:TimeSpan) (increment:TimeSpan) = 
    create <| fun s -> Some (init + (TimeSpan.Mutiply increment s.attempt))

  /// Returns an unbounded retry policy with a linearly increasing delay.
  let linearBounded (init:TimeSpan) (increment:TimeSpan) (attempts:int) = 
    linear init increment |> maxAttempts attempts
   
  let private randomizeMs r = 
    let rand = System.Random()
    let hi, lo = 1.0 + r, 1.0 - r
    fun (ms:int) -> (float ms) * (rand.NextDouble() * (hi - lo) + lo)
  
  let private checkOverflowMs (ms:int) =
    if ms = System.Int32.MinValue then 2000000000
    else ms

  /// A retry policy with exponential, randomized and limited backoff.
  /// @init - the initial delay
  /// @multiplier - exponential multiplier taken to the power of the attempt number
  /// @limitMs - the maximum delay
  /// @attempts - the maximum number of attempts
  let expLimitBoundedMs (initDelayMs:int) (multiplier:float) (delayLimitMs:int) (attempts:int) : RetryPolicy = 
    create (fun s ->
      let x = int (float initDelayMs * pown multiplier (s.attempt - 1))
      let x = checkOverflowMs x
      let x = max (min delayLimitMs (int x)) initDelayMs
      TimeSpan.FromMilliseconds x |> Some)
    |> maxAttempts attempts

  /// A retry policy with exponential, randomized and limited backoff.
  /// @init - the initial delay
  /// @multiplier - exponential multiplier taken to the power of the attempt number
  /// @randFactor - the randomization factor
  /// @limitMs - the maximum delay
  /// @attempts - the maximum number of attempts
  let expRandLimitBoundedMs (initDelayMs:int) (multiplier:float) (randFactor:float) (delayLimitMs:int) (attempts:int) : RetryPolicy = 
    let randomize = randomizeMs randFactor
    create (fun s ->
      let x = int (float initDelayMs * pown multiplier (s.attempt - 1))
      let x = checkOverflowMs x
      let x = randomize x
      let x = max (min delayLimitMs (int x)) initDelayMs
      TimeSpan.FromMilliseconds x |> Some)
    |> maxAttempts attempts

  /// A retry policy with exponential, randomized and limited backoff.
  /// @init - the initial delay
  /// @multiplier - exponential multiplier taken to the power of the attempt number
  /// @randFactor - the randomization factor
  /// @limitMs - the maximum delay
  /// @attempts - the maximum number of attempts
  let expRandLimitBounded (init:TimeSpan) (multiplier:float) (randFactor:float) (limit:TimeSpan) (attempts:int) : RetryPolicy = 
    expRandLimitBoundedMs (int init.TotalMilliseconds) multiplier randFactor (int limit.TotalMilliseconds) attempts

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
      | None -> RetryState.init
      | Some (s,_,_) -> RetryState.next s
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

  /// Returns an async computation which completes when all current queued items are due
  /// and returns the due items.
  let dueNowAwait (q:RetryQueue<'k, 'a>) = async {
    if q.items.Count = 0 then return Seq.empty else
    let now = DateTime.UtcNow
    let latestDue = items q |> Seq.map snd |> Seq.max
    if now >= latestDue then 
      return dueAt q now
    else
      do! Async.sleep (latestDue - now)
      return dueAt q now }

  /// Removes an item from the queue, resetting its retry state.
  let remove (q:RetryQueue<'k, 'a>) k =
    { q with items = Map.remove k q.items }

  let removeAll q ks = (q,ks) ||> Seq.fold remove

  let retryRemoveAll q retryItems removeItems =
    let q' = retryAll q retryItems 
    removeAll q' removeItems
      

[<Compile(Module)>]
module FlowMonitor =
  
  /// Returns a stream of underflows beyond a threshold of the specified input stream.
  /// Threshold = less than @count events are observed during @period.
  let undeflows (count:int) (period:TimeSpan) (stream:AsyncSeq<'a>) =
    stream
    |> AsyncSeq.bufferByTime (period.TotalMilliseconds |> int32)
    |> AsyncSeq.choose (fun buf ->
      if buf.Length < count then Some buf
      else None)

  /// Returns a stream of overflows beyond a threshold of the specified input stream.
  /// Threshold = more than @count events are observed during @period.
  let overflows (count:int) (period:TimeSpan) (stream:AsyncSeq<'a>) =
    stream
    |> AsyncSeq.map (fun a -> a,DateTime.UtcNow)
    |> AsyncSeq.windowed count
    |> AsyncSeq.choose (fun buf ->
      let _,dt0 = buf.[0]
      let _,dt1 = buf.[buf.Length - 1]
      if (dt1 - dt0) <= period then Some (buf |> Seq.map fst |> Seq.toArray)
      else None)

  /// Returns a stream of messages received from the mailbox.
  let private watchMb mb =
    AsyncSeq.replicateInfiniteAsync (Mb.take mb)

  /// Creates a sink and the resulting stream.
  let sinkStream<'a> : ('a -> unit) * AsyncSeq<'a> =
    let mb = Mb.create ()
    let stream = watchMb mb
    mb.Post, stream

  let overflowEvent (count:int) (period:TimeSpan) (e:IObservable<'a>) =
    e
    |> AsyncSeq.ofObservableBuffered
    |> overflows count period
    |> AsyncSeq.toObservable


/// Fault tolerance.
module internal Faults =

  /// Returns a stream of async computations corresponding to invocations of the argument 
  /// computation until the computations succeeds.
  let private retryAsyncResultStream (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : AsyncSeq<Result<'a, 'e>> =
    (AsyncSeq.replicateInfiniteAsync a, RetryPolicy.delayStream p)
    ||> AsyncSeq.interleaveChoice
    |> AsyncSeq.choose Choice.tryLeft

  let private retryAsyncResultRef (m:Monoid<'e>) (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    retryAsyncResultStream p a |> AsyncSeq.sequenceResult m
    
//  /// Retries an async computation returning a result according to the specified backoff strategy.
//  /// Returns an async computation containing a result, which is Success of a pair of the successful result and errors
//  /// accumulated during retries, if any, and otherwise, a Failure of accumulated errors.
//  let retryAsyncResultWarn (m:Monoid<'e>) (p:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a * 'e, 'e>> =
//    retryAsyncResultStream p a |> AsyncSeq.sequenceResultWarn m
//
//  let retryAsyncResultWarnList (policy:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<Result<'a * 'e list, 'e list>> =
//    retryAsyncResultWarn Monoid.freeList policy (a |> Async.map (Result.mapError List.singleton))

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
          do! Async.sleep wait
          return! loop (RetryState.next i) e }
    loop RetryState.init m.Zero

  let retryStateAsyncResult (m:Monoid<'e>) (p:RetryPolicy) (a:RetryState -> Async<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    let rec loop rs e = async {
      let! r = a rs
      match r with
      | Success a -> 
        return Success a
      | Failure e' ->
        let e = m.Merge (e,e')
        match RetryPolicy.delayAt rs p with
        | None -> 
          return Failure e
        | Some wait ->
          do! Async.sleep wait
          return! loop (RetryState.next rs) e }
    loop RetryState.init m.Zero

  /// Retries an async computation using the specified retry policy until the shouldRetry condition returns false.
  /// Returns None when shouldRetry returns false.
  let retryAsyncConditional (p:RetryPolicy) (shouldRetry:'a -> bool) (f:'a -> 'b) (result:'a list -> 'b) (a:Async<'a>) : Async<'b> =
    let rec loop i xs = async {
      let! a = a
      if shouldRetry a then
        match RetryPolicy.delayAt i p with
        | None -> 
          return result (a::xs)
        | Some wait ->
          do! Async.sleep wait
          return! loop (RetryState.next i) (a::xs)
      else
        return (f a) }
    loop RetryState.init []

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

  let retryStateAsyncResultList (b:RetryPolicy) (a:RetryState -> Async<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    retryStateAsyncResult Monoid.freeList b (a >> Async.map (Result.mapError List.singleton))
    
  /// Retries the specified async computation returning a Result.
  /// Returns the first successful value or throws an exception based on errors observed during retries.
  let retryResultThrow (f:'e -> #exn) (m:Monoid<'e>) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryAsyncResult m b a |> Async.map (Result.throwMap f)

  let retryStateResultThrow (f:'e -> #exn) (m:Monoid<'e>) (b:RetryPolicy) (a:RetryState -> Async<Result<'a, 'e>>) : Async<'a> =
    retryStateAsyncResult m b a |> Async.map (Result.throwMap f)

  let retryResultThrowList (f:'e list -> #exn) (b:RetryPolicy) (a:Async<Result<'a, 'e>>) : Async<'a> =
    retryResultThrow f Monoid.freeList b (a |> Async.map (Result.mapError List.singleton))

  let retryStateResultThrowList (f:'e list -> #exn) (b:RetryPolicy) (a:RetryState -> Async<Result<'a, 'e>>) : Async<'a> =
    retryStateResultThrow f Monoid.freeList b (a >> Async.map (Result.mapError List.singleton))

  module AsyncFunc =

    let retryAsyncConditional (p:RetryPolicy) (shouldRetry:'a * 'b -> bool) (m:'a * 'b -> 'c) (result:'a * 'b list -> 'c) (f:'a -> Async<'b>) : 'a -> Async<'c> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncConditional p (fun b -> shouldRetry (a,b)) (fun b -> m (a,b)) (fun bs -> result (a,bs))

    let retryAsync (shouldRetry:'a * 'b -> bool) (p:RetryPolicy) (f:'a -> Async<'b>) : 'a -> Async<'b option> =
      fun a -> async.Delay (fun () -> f a) |> retryAsync p (fun b -> shouldRetry (a,b))

    let retryResult (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResult m p

    let retryResultList (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<Result<'b, 'e list>> =
      fun a -> async.Delay (fun () -> f a) |> retryAsyncResultList p

    let retryResultThrow (ex:'e -> #exn) (m:Monoid<'e>) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrow ex m p

    let retryResultThrowList (ex:'e list -> #exn) (p:RetryPolicy) (f:'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> async.Delay (fun () -> f a) |> retryResultThrowList ex p

    let retryStateResultThrowList (ex:'e list -> #exn) (p:RetryPolicy) (f:RetryState -> 'a -> Async<Result<'b, 'e>>) : 'a -> Async<'b> =
      fun a -> retryStateResultThrowList ex p (fun rs -> f rs a)
      
    let loop (f:Async<'f> -> 'a -> Async<'b * 'f>) : 'a -> Async<'b> =
      let fb = MVar.create ()
      fun a -> async {
        let! (b,fb') = f (MVar.take fb) a 
        let! _ = MVar.put fb' fb
        return b }

