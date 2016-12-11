namespace Kafunk

[<AutoOpen>]
module Prelude =

  /// Determines whether the argument is a null reference.
  let inline isNull a = obj.ReferenceEquals(null, a)

  /// Given a value, creates a function with one ignored argument which returns the value.
  let inline konst x _ = x

  /// Active pattern for matching Result<'a, 'e>.
  let (|Success|Failure|) = function | Choice1Of2 a -> Success a | Choice2Of2 b -> Failure b

  let flip f a b = f b a

  let tryDispose (d:#System.IDisposable) = 
    try d.Dispose() finally ()


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

type Semigroup<'a> =
  abstract Merge : 'a * 'a -> 'a

type Monoid<'a> =
  inherit Semigroup<'a>
  abstract Zero : 'a

module Monoid =
  
  let inline zero (m:Monoid<_>) = m.Zero

  let inline merge (m:Monoid<_>) a b = m.Merge (a,b)

  let inline monoid (z:'a) (m:'a -> 'a -> 'a) =
    { new Monoid<'a> with
        member __.Zero = z
        member __.Merge (a,b) = m a b }

  let product (m1:Monoid<'a>) (m2:Monoid<'b>) : Monoid<'a * 'b> =
    monoid (m1.Zero, m2.Zero) (fun (a1,b1) (a2,b2) -> m1.Merge (a1,a2), m2.Merge (b1,b2))

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
    

// --------------------------------------------------------------------------------------------------






// --------------------------------------------------------------------------------------------------
// collection helpers

open System.Collections.Generic

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module List =
  
  let inline singleton a = [a]

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Seq =
  
  let rec tryItem index (s:seq<_>) =
    let rec tryItem index (e:IEnumerator<_>) =
      if not (e.MoveNext()) then None
      elif index = 0 then Some(e.Current)
      else tryItem (index-1) e
    use e = s.GetEnumerator()
    tryItem index e

/// Basic operations on dictionaries.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Dict =

  let tryGet k (d:#IDictionary<_,_>) =
    let mutable v = Unchecked.defaultof<_>
    if d.TryGetValue(k, &v) then Some v
    else None


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Array =

  let inline item i a = Array.get a i
  
  let inline singleton a = [|a|]
  
  /// Partitions the array into the specified number of groups.
  /// If there are more groups than elements, then the empty groups are returned.
  /// When the array doesn't divide into the number of groups, the last group will 
  /// have one fewer element.
  let groupInto (groups:int) (a:'a[]) : 'a[][] =
    if groups < 1 then invalidArg "groups" "must be positive"
    let perGroup = int (ceil (float a.Length / float groups))
    let groups = Array.zeroCreate groups
    for i = 0 to groups.Length - 1 do
      let group = ResizeArray<_>(perGroup)
      for j = 0 to perGroup - 1 do
        let idx = i * perGroup + j
        if idx < a.Length then
          group.Add (a.[idx])
      groups.[i] <- group.ToArray()
    groups
    
    
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Map =

  let addMany (kvps:('a * 'b) seq) (m:Map<'a, 'b>) : Map<'a, 'b> =
    kvps |> Seq.fold (fun m (k,v) -> Map.add k v m) m

// --------------------------------------------------------------------------------------------------




// --------------------------------------------------------------------------------------------------
// result

type Result<'a, 'e> = Choice<'a, 'e>

type ResultWarn<'a, 'e> = Result<'a * 'e list, 'e>

[<AutoOpen>]
module ResultEx =

  let (|Success|Failure|) r : Result<'a, 'e> = r

  let inline Success a : Result<'a, 'e> = Choice1Of2 a

  let inline Failure e : Result<'a, 'e> = Choice2Of2 e

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Result =

  let inline success a : Result<'a, 'e> = Choice1Of2 a

  let inline fail e : Result<'a, 'e> = Choice2Of2 e

  let inline map f (r:Result<'a, 'e>) : Result<'b, 'e> = 
    Choice.mapLeft f r

  let inline mapError f (r:Result<'a, 'e>) : Result<'a, 'e2> =
     Choice.mapRight f r

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



module KafkaUri =

  open System
  open System.Text.RegularExpressions

  let [<Literal>] DefaultPortKafka = 9092
  let [<Literal>] UriSchemeKafka = "kafka"
  let private KafkaBrokerUriRegex = Regex("^(?<scheme>(kafka|tcp)://)?(?<host>[-._\w]+)(:(?<port>[\d]+))?", RegexOptions.Compiled)

  let parse (host:string) =
    let m = KafkaBrokerUriRegex.Match host
    if not m.Success then invalidArg "host" (sprintf "invalid host string '%s'" host)
    else
      let host = m.Groups.["host"].Value
      let port = 
        let g = m.Groups.["port"]
        if g.Success then Int32.Parse g.Value
        else DefaultPortKafka
      let ub = UriBuilder(UriSchemeKafka, host, port)
      ub.Uri