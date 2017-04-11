[<AutoOpen>]
module internal Kafunk.Prelude

[<assembly: System.Runtime.CompilerServices.InternalsVisibleToAttribute("Kafunk.Tests")>]
do ()

open System

/// Determines whether the argument is a null reference.
let inline isNull a = obj.ReferenceEquals(null, a)

/// Given a value, creates a function with one ignored argument which returns the value.
let inline konst x _ = x

let inline flip f a b = f b a

let inline diag a = a,a

/// CompilationRepresentationAttribute
type Compile = CompilationRepresentationAttribute
  
/// CompilationRepresentationFlags.ModuleSuffix
let [<Literal>] Module = CompilationRepresentationFlags.ModuleSuffix


/// Operations on IDisposable.
module Disposable =
  
  let tryDispose (d:#System.IDisposable) = 
    try d.Dispose() finally ()

  let ofFun (f:unit -> unit) = 
    { new IDisposable with member __.Dispose () = f () }

  let ofFuns (fs:(unit -> unit) seq) =
    ofFun (fun () -> for f in fs do f ())

  let ofDisposables (ds:#IDisposable seq) =
    ofFun (fun () -> for d in ds do d.Dispose())


let UnixEpoch = DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)

type DateTime with
  static member UtcNowUnixMilliseconds = int64 (DateTime.UtcNow - UnixEpoch).TotalMilliseconds

type TimeSpan with
  static member FromMilliseconds (ms:int) =
    TimeSpan.FromMilliseconds (float ms)
  static member FromSeconds (sec:int) =
    TimeSpan.FromSeconds (float sec)
  static member Mutiply (s:TimeSpan) (x:int) =
    let mutable s = s
    for _ in [1..x] do
      s <- s.Add s
    s


module Option =
  
  let getOr (defaultValue:'a) = function Some a -> a | None -> defaultValue


module Choice =

  let fold (f:'a -> 'c) (g:'b -> 'c) (c:Choice<'a, 'b>) : 'c =
    match c with
    | Choice1Of2 a -> f a
    | Choice2Of2 b -> g b

  let mapLeft (f:'a -> 'c) (c:Choice<'a, 'b>) : Choice<'c, 'b> =
    match c with
    | Choice1Of2 a -> Choice1Of2 (f a)
    | Choice2Of2 b -> Choice2Of2 b
  
  let mapRight (f:'b -> 'c) (c:Choice<'a, 'b>) : Choice<'a, 'c> =
    match c with
    | Choice1Of2 a -> Choice1Of2 a
    | Choice2Of2 b -> Choice2Of2 (f b)

  let codiag = function Choice1Of2 a -> a | Choice2Of2 a -> a

  let tryLeft = function Choice1Of2 x -> Some x | _ -> None

  let tryRight = function Choice2Of2 x -> Some x | _ -> None
    


// --------------------------------------------------------------------------------------------------

/// A semigroup (+).
type Semigroup<'a> =
  abstract Merge : 'a * 'a -> 'a

/// A monoid (e,+).
type Monoid<'a> =
  inherit Semigroup<'a>
  abstract Zero : 'a

/// A group (e,+,-).
type Group<'a> =
  inherit Monoid<'a>
  abstract Inverse : 'a -> 'a

/// Operations on monoids.
module Monoid =
  
  let inline zero (m:Monoid<_>) = m.Zero

  let inline merge (m:Monoid<_>) a b = m.Merge (a,b)

  let inline monoid (z:'a) (m:'a -> 'a -> 'a) =
    { new Monoid<'a> with
        member __.Zero = z
        member __.Merge (a,b) = m a b }

  let endo<'a> : Monoid<'a -> 'a> =
    monoid id (>>)

  let dual (m:Monoid<'a>) : Monoid<'a> =
    monoid m.Zero (fun a b -> m.Merge (b,a))

  let product (m1:Monoid<'a>) (m2:Monoid<'b>) : Monoid<'a * 'b> =
    monoid 
      (m1.Zero, m2.Zero) 
      (fun (a1,b1) (a2,b2) -> 
        m1.Merge (a1,a2), m2.Merge (b1,b2))

  let product3 (m1:Monoid<'a>) (m2:Monoid<'b>) (m3:Monoid<'c>) : Monoid<'a * 'b * 'c> =
    monoid 
      (m1.Zero, m2.Zero, m3.Zero) 
      (fun (a1,b1,c1) (a2,b2,c2) -> 
        m1.Merge (a1,a2), m2.Merge (b1,b2), m3.Merge (c1, c2))

  let product4 (m1:Monoid<'a>) (m2:Monoid<'b>) (m3:Monoid<'c>) (m4:Monoid<'d>) : Monoid<'a * 'b * 'c * 'd> =
    monoid 
      (m1.Zero, m2.Zero, m3.Zero, m4.Zero) 
      (fun (a1,b1,c1,d1) (a2,b2,c2,d2) -> 
        m1.Merge (a1,a2), m2.Merge (b1,b2), m3.Merge (c1, c2), m4.Merge (d1,d2))

  let product5 (m1:Monoid<'a>) (m2:Monoid<'b>) (m3:Monoid<'c>) (m4:Monoid<'d>) (m5:Monoid<'e>) : Monoid<'a * 'b * 'c * 'd * 'e> =
    monoid 
      (m1.Zero, m2.Zero, m3.Zero, m4.Zero, m5.Zero) 
      (fun (a1,b1,c1,d1,e1) (a2,b2,c2,d2,e2) -> 
        m1.Merge (a1,a2), m2.Merge (b1,b2), m3.Merge (c1, c2), m4.Merge (d1,d2), m5.Merge (e1,e2))

  let freeList<'a> : Monoid<'a list> = 
    monoid [] List.append

  let stringAppend : Monoid<string> =
    monoid "" (+)
  
  let stringConcat (s:string) : Monoid<string> =
    monoid "" (fun a b -> a + s + b)

  /// A monoid for 'a option returning the first non-None value.
  [<GeneralizableValue>]
  let optionFirst<'a> : Monoid<'a option> =
    monoid None (fun a b -> match a,b with Some _,_ -> a | None,b -> b)

  /// A monoid for 'a option returning the last non-None value.
  [<GeneralizableValue>]
  let optionLast<'a> : Monoid<'a option> =
    monoid None (fun a b -> match a,b with _,Some _ -> b | a,None -> a)

  let intSum : Monoid<int> =
    monoid 0 ((+))
    
/// Operations on groups.
module Group =
  
  /// Creates a group.
  let group (z:'a) (add:'a -> 'a -> 'a) (inverse:'a -> 'a) =
    { new Group<'a> with
        member __.Zero = z
        member __.Merge (a,b) = add a b
        member __.Inverse a = inverse a }

  let inline zero (g:Group<'a>) = g.Zero

  let inline add (g:Group<'a>) a b = g.Merge (a,b)

  let inline inverse (g:Group<'a>) a = g.Inverse a

  let inline subtract (g:Group<'a>) a b = g.Merge (a, g.Inverse b)

  /// Creates an additive group based on statically resolved members.
  let inline additive () =
    group LanguagePrimitives.GenericZero (+) (fun x -> -x)

  /// The group formed by integers with (0,+,-).
  let intAdd : Group<int> = additive ()

// --------------------------------------------------------------------------------------------------






// --------------------------------------------------------------------------------------------------
// collection helpers

open System.Collections.Generic

[<Compile(Module)>]
module List =
  
  let inline singleton a = [a]

[<Compile(Module)>]
module Array =

  let inline item i a = Array.get a i
  
  let inline singleton a = [|a|]
  
  let inline take count (array:'a[]) =
      let sub : 'a[] = Array.zeroCreate count
      System.Array.Copy(array, sub, count)
      sub

[<Compile(Module)>]
module Seq =
  
  let rec tryItem index (s:seq<_>) =
    let rec tryItem index (e:IEnumerator<_>) =
      if not (e.MoveNext()) then None
      elif index = 0 then Some(e.Current)
      else tryItem (index-1) e
    use e = s.GetEnumerator()
    tryItem index e

  let partitionChoices (s:seq<Choice<'a, 'b>>) : 'a[] * 'b[] =
    let ax,bx = ResizeArray<_>(),ResizeArray<_>()
    for c in s do
      match c with
      | Choice1Of2 a -> ax.Add(a)
      | Choice2Of2 b -> bx.Add(b)
    ax.ToArray(),bx.ToArray()

  let partitionChoices3 (s:seq<Choice<'a, 'b, 'c>>) : 'a[] * 'b[] * 'c[] =
    let ax,bx,cx = ResizeArray<_>(),ResizeArray<_>(),ResizeArray<_>()
    for c in s do
      match c with
      | Choice1Of3 a -> ax.Add(a)
      | Choice2Of3 b -> bx.Add(b)
      | Choice3Of3 c -> cx.Add(c)
    ax.ToArray(),bx.ToArray(),cx.ToArray()

  let partitionChoices4 (s:seq<Choice<'a, 'b, 'c, 'd>>) : 'a[] * 'b[] * 'c[] * 'd[] =
    let ax,bx,cx,dx = ResizeArray<_>(),ResizeArray<_>(),ResizeArray<_>(),ResizeArray<_>()
    for c in s do
      match c with
      | Choice1Of4 a -> ax.Add(a)
      | Choice2Of4 b -> bx.Add(b)
      | Choice3Of4 c -> cx.Add(c)
      | Choice4Of4 c -> dx.Add(c)
    ax.ToArray(),bx.ToArray(),cx.ToArray(),dx.ToArray()

  let batch (batchSize:int) (s:seq<'a>) : seq<'a[]> =
      seq {
          use en = s.GetEnumerator()
          let more = ref true
          while !more do
              let batch : 'a[] = Array.zeroCreate batchSize
              let rec read i =
                  if i < batchSize && en.MoveNext() then
                      batch.[i] <- en.Current
                      read (i + 1)
                  else i
              let i = read 0
              if i = batchSize then
                  yield batch
              else
                  more := false
                  if i > 0 then
                      yield Array.take i batch
      }
  
  let foldMap (M:Monoid<'m>) (f:'a -> 'm) (s:seq<'a>) : 'm =
    Seq.fold (fun s a -> Monoid.merge M s (f a)) M.Zero s


/// Basic operations on dictionaries.
[<Compile(Module)>]
module Dict =

  let empty<'a, 'b when 'a : equality> : Dictionary<'a, 'b> = Dictionary<_,_>()

  let tryGet k (d:#IReadOnlyDictionary<_,_>) =
    let mutable v = Unchecked.defaultof<_>
    if d.TryGetValue(k, &v) then Some v
    else None

  let ofSeq (xs:seq<'a * 'b>) : Dictionary<'a, 'b> =
    let d = Dictionary<'a, 'b>()
    for (a,b) in xs do
      d.Add (a,b)
    d
  
  let ofMap (m:Map<'a, 'b>) : Dictionary<'a, 'b> =
    let d = Dictionary<'a, 'b>()
    for kvp in m do
      d.Add (kvp.Key, kvp.Value)
    d

    
[<Compile(Module)>]
module Map =

  let mergeChoice (f:'a -> Choice<'b * 'c, 'b, 'c> -> 'd) (a:Map<'a, 'b>) (b:Map<'a, 'c>) : Map<'a, 'd> =
    Set.union (a |> Seq.map (fun k -> k.Key) |> set) (b |> Seq.map (fun k -> k.Key) |> set)
    |> Seq.map (fun k ->
      match Map.tryFind k a, Map.tryFind k b with
      | Some b, Some c -> k, f k (Choice1Of3 (b,c))
      | Some b, None   -> k, f k (Choice2Of3 b)
      | None,   Some c -> k, f k (Choice3Of3 c)
      | None,   None   -> failwith "invalid state")
    |> Map.ofSeq

  let mergeWith (m:'b -> 'b -> 'b) (a:Map<'a, 'b>) (b:Map<'a, 'b>) : Map<'a, 'b> =
    mergeChoice (fun _ -> function Choice1Of3 (x,y) -> m x y | Choice2Of3 b | Choice3Of3 b -> b) a b

  let addMany (kvps:('a * 'b) seq) (m:Map<'a, 'b>) : Map<'a, 'b> =
    kvps |> Seq.fold (fun m (k,v) -> Map.add k v m) m

  let onlyKeys (ks:'a seq) (m:Map<'a, 'b>) : Map<'a, 'b> =
    ks 
    |> Seq.choose (fun k -> Map.tryFind k m |> Option.map (fun v -> k,v))
    |> Map.ofSeq
  
  let removeAll (ks:seq<'a>) (m:Map<'a, 'b>) : Map<'a, 'b> =
    (m, ks) ||> Seq.fold (fun m k -> Map.remove k m)
        


// --------------------------------------------------------------------------------------------------




// --------------------------------------------------------------------------------------------------
// result

type Result<'a, 'e> = Choice<'a, 'e>

type ResultWarn<'a, 'e> = Result<'a * 'e list, 'e>

[<AutoOpen>]
module ResultEx =

  let (|Success|Failure|) r : Result<'a, 'e> = r

  let Success a : Result<'a, 'e> = Choice1Of2 a

  let Failure e : Result<'a, 'e> = Choice2Of2 e

[<Compile(Module)>]
module Result =

  let inline success a : Result<'a, 'e> = Choice1Of2 a

  let inline fail e : Result<'a, 'e> = Choice2Of2 e

  let inline map f (r:Result<'a, 'e>) : Result<'b, 'e> = 
    Choice.mapLeft f r

  let mapError (f:'e -> 'e2) (r:Result<'a, 'e>) : Result<'a, 'e2> =
     Choice.mapRight f r

  let tapError (f:'e -> unit) (r:Result<'a, 'e>) : Result<'a, 'e> =
    mapError (fun e -> f e |> ignore ; e) r

  let map2 (g:'e -> 'e -> 'e) (f:'a -> 'b -> 'c) (r1:Result<'a, 'e>) (r2:Result<'b, 'e>) : Result<'c, 'e> =
    match r1, r2 with
    | Success a, Success b -> Success (f a b)
    | Failure e1, Failure e2 -> Failure (g e1 e2)
    | Failure e1, _ -> Failure e1
    | _, Failure e2 -> Failure e2

  let tryFirst (r1:Result<'a, 'e>) (r2:Result<'a, 'e>) : Result<'a, 'e> =
    match r1, r2 with
    | Success r1, _ -> Success r1
    | _, Success r2 -> Success r2
    | Failure e1, _ -> Failure e1

  let trySecond (r1:Result<'a, 'e>) (r2:Result<'a, 'e>) : Result<'a, 'e> =
    match r1, r2 with
    | _, Success r2 -> Success r2
    | Success r1, _ -> Success r1
    | _, Failure e2 -> Failure e2

  let bind (f:'a -> Result<'b, 'e>) (r:Result<'a, 'e>) : Result<'b, 'e> =
    match r with
    | Choice1Of2 a -> f a
    | Choice2Of2 e -> Choice2Of2 e
 
  let inline fold f g (r:Result<'a, 'e>) : 'b = 
    Choice.fold f g r

  let trySuccess (r:Result<'a, 'e>) : 'a option =
    fold Some (fun _ -> None) r

  let inline codiag (r:Result<'a, 'a>) : 'a = 
    fold id id r

  let traverse (f:'a -> Result<'b, 'e>) (xs:seq<'a>) : Result<'b[], 'e> =
    use en = xs.GetEnumerator()
    let oks = ResizeArray<_>()
    let err = ref None
    while en.MoveNext() && err.Value.IsNone do
      match f en.Current with
      | Choice1Of2 b -> oks.Add b
      | Choice2Of2 e -> err := Some e
    match !err with
    | Some e -> Choice2Of2 e
    | None -> Choice1Of2 (oks.ToArray())

  /// Returns a succesful result or raises an exception in case of failure.
  let throw (r:Result<'a, #exn>) : 'a =
    match r with
    | Success a -> a
    | Failure e -> raise e

  /// Returns a succesful result or raises an exception in case of failure.
  let throwMap (f:'e -> #exn) (r:Result<'a, 'e>) : 'a =
    match r with
    | Success a -> a
    | Failure e -> raise (f e)

  let join (r:Result<Result<'a, 'e1>, 'e2>) : Result<'a, Choice<'e1, 'e2>> =
    match r with
    | Failure e -> Failure (Choice2Of2 e)
    | Success (Success a) -> Success a
    | Success (Failure e) -> Failure (Choice1Of2 e)

  let ofOptionMap (e:unit -> 'e) (o:'a option) : Result<'a, 'e> =
    match o with Some a -> Success a | None -> Failure (e ())

  let ofOption (o:'a option) : Result<'a, unit> =
    match o with Some a -> Success a | None -> Failure ()

  let codiagExn (r:Result<'a, Choice<#exn, #exn>>) : Result<'a, exn> =
    r |> mapError (Choice.fold (fun e -> e :> exn) (fun e -> e :> exn))

// --------------------------------------------------------------------------------------------------






// --------------------------------------------------------------------------------------------------
// pooling

open System.Collections.Concurrent

type ObjectPool<'a>(initial:int, create:unit -> 'a) =

  let pool = new ConcurrentStack<'a>(Seq.init initial (fun _ -> create()))

  member x.Push(a:'a) =
    pool.Push(a)

  member x.Pop() =
    let mutable a = Unchecked.defaultof<'a>
    if not (pool.TryPop(&a)) then
      failwith "out of sockets!"
      //create ()
    else a

// --------------------------------------------------------------------------------------------------


type IBoundedMbCond<'a> =
  abstract member Add : 'a -> unit
  abstract member Remove : 'a -> unit
  abstract member Reset : unit -> unit
  abstract member Satisfied : bool


[<Compile(Module)>]
module Observable =

  open System
  open System.Threading
  open System.Collections.Concurrent
  open System.Runtime.ExceptionServices

  let private disposable dispose = 
    { new IDisposable with member __.Dispose () = dispose () }

  let create (subscribe:IObserver<_> -> unit -> unit) =
    { new IObservable<_> with member __.Subscribe(observer) = subscribe observer |> disposable }

  let interval (timeSpan:TimeSpan) : IObservable<unit> =
    let timeSpanMs = int timeSpan.TotalMilliseconds
    create (fun obs ->
      let cts = new CancellationTokenSource()
      let rec loop() = async {
        do! Async.Sleep timeSpanMs
        obs.OnNext()
        if cts.IsCancellationRequested then return ()
        else return! loop() }
      Async.Start (loop(), cts.Token)
      fun() -> cts.Cancel(false) ; cts.Dispose())

  let bufferByTime (timeSpan:TimeSpan) (source:IObservable<'a>) =

    create (fun (observer:IObserver<'a[]>) ->

      let batchQueue = new BlockingCollection<'a>()

      let batches =
        interval timeSpan
        |> Observable.map (fun _ ->
          let batch = new ResizeArray<_>(batchQueue.Count)
          let mutable item : 'a = Unchecked.defaultof<'a>
          while (batchQueue.TryTake(&item)) do batch.Add(item)
          batch.ToArray())

      let sourceSubs =
        source.Subscribe <| { new IObserver<_> with
          member __.OnNext(a) =
            batchQueue.Add a
          member __.OnError(e) = 
            observer.OnError(e)
          member __.OnCompleted() = 
            observer.OnCompleted() }

      let batchSubs = batches.Subscribe (observer.OnNext)

      fun () -> sourceSubs.Dispose() ; batchSubs.Dispose() ; batchQueue.Dispose())

  let bufferByTimeAndCount (timeSpan:TimeSpan) (bufferSize:int) (source:IObservable<'a>) =

    let takeAny (queue:BlockingCollection<'a>) (count:int) =
      let batch = new ResizeArray<_>(count)
      let mutable item : 'a = Unchecked.defaultof<'a>
      while (batch.Count < count && queue.TryTake(&item)) do batch.Add(item)
      batch.ToArray()

    create (fun (observer:IObserver<'a[]>) ->

      let batchQueue = new BlockingCollection<'a>(bufferSize)
      let batchEvent = new Event<unit>()

      let batches =
        Observable.merge (interval timeSpan) batchEvent.Publish
        |> Observable.choose (fun () ->
          let batch = takeAny batchQueue bufferSize
          if batch.Length > 0 then Some batch
          else None)

      let sourceSubs =
        source.Subscribe <| { new IObserver<_> with
          member __.OnNext(a) =
            batchQueue.Add a
            if batchQueue.Count >= bufferSize then
              batchEvent.Trigger()
          member __.OnError(e) = 
            observer.OnError(e)
          member __.OnCompleted() = 
            observer.OnCompleted() }

      let batchSubs = batches.Subscribe(observer.OnNext)

      fun () -> sourceSubs.Dispose() ; batchSubs.Dispose() ; batchQueue.Dispose())
  
  let bufferByTimeAndCondition (timeSpan:TimeSpan) (cond:IBoundedMbCond<'a>) (source:IObservable<'a>) =

    let takeAny (queue:BlockingCollection<'a>) =
      let batch = new ResizeArray<_>()
      let mutable item : 'a = Unchecked.defaultof<'a>
      while (queue.TryTake(&item)) do 
        batch.Add(item)
      batch.ToArray()

    create (fun (observer:IObserver<'a[]>) ->

      let batchQueue = new BlockingCollection<'a>()
      let batchEvent = new Event<unit>()

      let batches =
        Observable.merge (interval timeSpan) batchEvent.Publish
        |> Observable.choose (fun () ->
          let batch = takeAny batchQueue
          if batch.Length > 0 then Some batch
          else None)

      let sourceSubs =
        source.Subscribe <| { new IObserver<_> with
          member __.OnNext(a) =
            batchQueue.Add a
            cond.Add a
            if cond.Satisfied then
              cond.Reset ()
              batchEvent.Trigger()
          member __.OnError(e) = 
            observer.OnError(e)
          member __.OnCompleted() = 
            observer.OnCompleted() }

      let batchSubs = batches.Subscribe(observer.OnNext)

      fun () -> sourceSubs.Dispose() ; batchSubs.Dispose() ; batchQueue.Dispose())

  /// Union type that represents different messages that can be sent to the
  /// IObserver interface. The IObserver type is equivalent to a type that has
  /// just OnNext method that gets 'ObservableUpdate' as an argument.
  type internal ObservableUpdate<'T> = 
      | Next of 'T
      | Error of ExceptionDispatchInfo
      | Completed


  /// Turns observable into an observable that only calls OnNext method of the
  /// observer, but gives it a discriminated union that represents different
  /// kinds of events (error, next, completed)
  let asUpdates (source:IObservable<'T>) = 
    { new IObservable<_> with
        member x.Subscribe(observer) =
          source.Subscribe
            ({ new IObserver<_> with
                member x.OnNext(v) = observer.OnNext(Next v)
                member x.OnCompleted() = observer.OnNext(Completed) 
                member x.OnError(e) = observer.OnNext(Error (ExceptionDispatchInfo.Capture e)) }) }


  