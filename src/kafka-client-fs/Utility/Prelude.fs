namespace Kafunk

module Prelude =

  /// Determines whether the argument is a null reference.
  let inline isNull a = obj.ReferenceEquals(null, a)

  /// Given a value, creates a function with one ignored argument which returns the value.
  let inline konst x _ = x

  /// Active pattern for matching a Choice<'a, 'b> with Choice1Of2 = Success and Choice2Of2 = Failure
  let (|Success|Failure|) = function | Choice1Of2 a -> Success a | Choice2Of2 b -> Failure b

  let flip f a b = f b a

module Choice =

  let fold (f:'a -> 'c) (g:'b -> 'c) (c:Choice<'a, 'b>) : 'c =
    match c with
    | Choice1Of2 a -> f a
    | Choice2Of2 b -> g b

  let mapLeft (f:'a -> 'c) (c:Choice<'a, 'b>) : Choice<'c, 'b> =
    match c with
    | Choice1Of2 a -> Choice1Of2 (f a)
    | Choice2Of2 b -> Choice2Of2 b

  let inline mapSuccess f c = mapLeft f c

  let mapRight (f:'b -> 'c) (c:Choice<'a, 'b>) : Choice<'a, 'c> =
    match c with
    | Choice1Of2 a -> Choice1Of2 a
    | Choice2Of2 b -> Choice2Of2 (f b)

  let inline mapError f c = mapRight f c

  let flattenLeft (c:Choice<Choice<'a, 'e>, 'e>) : Choice<'a, 'e> =
    match c with
    | Choice1Of2 (Choice1Of2 a) -> Choice1Of2 a
    | Choice1Of2 (Choice2Of2 e) | Choice2Of2 e -> Choice2Of2 e

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

// log

// TODO: implement!

type LogLevel =
  | Trace | Info | Warn | Error | Fatal
  with
    override x.ToString () =
      match x with
      | Trace -> "TRACE" | Info -> "INFO" | Warn -> "WARN" | Error -> "ERROR" | Fatal -> "FATAL"

type Logger = {
  name : string
}


[<AutoOpen>]
module LoggerEx =
  open System

  type Logger with

    member inline ts.log (format, level:LogLevel) =
      let inline trace (m:string) =
        Console.WriteLine(String.Format("{0:yyyy-MM-dd hh:mm:ss:ffff}|{1}|{2}|{3}", DateTime.Now, (level.ToString()), ts.name, m))
      Printf.kprintf trace format

    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)

    member inline ts.error format = ts.log (format, LogLevel.Error)

    member inline ts.warn format = ts.log (format, LogLevel.Warn)

    member inline ts.info format = ts.log (format, LogLevel.Info)

    member inline ts.trace format = ts.log (format, LogLevel.Trace)



[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Log =

  let create name = { name = name }

// collection helpers


open System.Collections.Generic

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


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Seq =

  /// Creates a map from a sequnce based on a key and value selection function.
  let toKeyValueMap (key:'a -> 'Key) (value:'a -> 'Value) (s:seq<'a>) : Map<'Key, 'Value> =
      s
      |> Seq.map (fun a -> key a,value a)
      |> Map.ofSeq

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Map =

  let addMany (kvps:('a * 'b) seq) (m:Map<'a, 'b>) : Map<'a, 'b> =
    kvps |> Seq.fold (fun m (k,v) -> Map.add k v m) m
