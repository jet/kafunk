[<AutoOpen>]
module internal Kafunk.AsyncSeq

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks

/// An async transducer.
type AsyncPipe<'a, 'b> = Async<Step<'a, 'b>>

/// An individual step of an async transducer.
and Step<'a, 'b> =
  | Halt
  | Emit of 'b * tail:AsyncPipe<'a, 'b>
  | Await of ('a option -> AsyncPipe<'a, 'b>)

module AsyncPipe =
    
  let encased (step:Step<'a, 'b>) : AsyncPipe<'a, 'b> = 
    async.Return step

  [<GeneralizableValue>]
  let halt<'a, 'b> : AsyncPipe<'a, 'b> = 
    encased Halt

  let emit (b:'b) (tail:AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    encased (Emit (b, tail))

  let emitHalt (b:'b) : AsyncPipe<'a, 'b> =
    emit b halt

  let await (f:'a option -> AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    encased (Await f)

  let awaitHalt (f:'a -> AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    await (function Some a -> f a | None -> halt)

  let drain (pipe:AsyncPipe<_, 'b>) : AsyncSeq<'b> =
    { new IAsyncEnumerable<_> with
        member __.GetEnumerator () =
          let step = ref pipe
          { new IAsyncEnumerator<_> with
              member __.MoveNext () = async {
                let! step' = !step
                match step' with
                | Halt | Await _ -> 
                  return None
                | Emit (a,tail) ->
                  step := tail
                  return Some a }
              member __.Dispose () = () } }
            
  let windowed (windowSize:int) : AsyncPipe<'a, 'a[]> =
    let rec loop (win:ResizeArray<_>) =
      awaitHalt (fun a ->
        win.Add a
        if win.Count > windowSize then
          win.RemoveAt 0
        if win.Count = windowSize then
          emit (win.ToArray()) (loop win)
        else 
          loop win)
    loop (ResizeArray<_>(windowSize))

  let bufferByTime (timeSpan:TimeSpan) : AsyncPipe<'a, 'a[]> =
    let rec loop (t:DateTime) (buf:ResizeArray<_>) =
      awaitHalt (fun a ->
        buf.Add a
        let t' = DateTime.UtcNow
        if (t' - t) >= timeSpan then
          emit (buf.ToArray()) (loop t' (ResizeArray<_>()))
        else
          loop t buf)
    loop DateTime.UtcNow (ResizeArray<_>())

  /// Feeds an async sequence into a transducer and emits the resulting async sequence.
  let transduce (pipe:AsyncPipe<'a, 'b>) (source:AsyncSeq<'a>) : AsyncSeq<'b> = 
    asyncSeq {
      use enum = source.GetEnumerator()
      let rec go pipe = asyncSeq {
        let! step = pipe
        match step with
        | Halt -> ()
        | Emit (b,tail) ->
          yield b
          yield! go tail
        | Await recv ->
          let! next = enum.MoveNext()
          match next with
          | None ->
            yield! drain (recv None)
          | Some a ->
            yield! go (recv (Some a)) }
      yield! go pipe }
        
        
/// Module with helper functions for working with asynchronous sequences
module AsyncSeq =

  let windowed (windowSize:int) : AsyncSeq<'a> -> AsyncSeq<'a[]> =
    AsyncPipe.transduce (AsyncPipe.windowed windowSize)

  let bufferByTime (timeSpan:TimeSpan) : AsyncSeq<'a> -> AsyncSeq<'a[]> =
    AsyncPipe.transduce (AsyncPipe.bufferByTime timeSpan)

  let unfoldInfiniteAsync (s:'s) (f:'s -> Async<'a * 's>) : AsyncSeq<'a> =
    AsyncSeq.unfoldAsync (f >> Async.map Some) s 

  let interleaveChoice (a:AsyncSeq<'a>) (b:AsyncSeq<'b>) : AsyncSeq<Choice<'a, 'b>> =
    AsyncSeq.interleave (a |> AsyncSeq.map Choice1Of2) (b |> AsyncSeq.map Choice2Of2)

  /// A traversal of an async sequence specialized to the Result type.
  /// Returns the first successful result or a list of all erroneous results.
  let traverseAsyncResult
    (m:Monoid<'e>)
    (f:'a -> Async<Result<'b, 'e>>) 
    (s:AsyncSeq<'a>) : Async<Result<'b, 'e>> = async {
    use en = s.GetEnumerator()
    let rec go e = async {
      let! s = en.MoveNext ()
      match s with
      | None -> 
        return Failure e
      | Some hd ->
        let! r = f hd
        match r with
        | Success a -> 
          return Success a
        | Failure e' -> 
          return! go (m.Merge (e,e')) }
    return! go m.Zero }

  let traverseAsyncResultWarn
    (m:Monoid<'e>)
    (f:'a -> Async<Result<'b, 'e>>) 
    (s:AsyncSeq<'a>) : Async<Result<'b * 'e, 'e>> = async {
    use en = s.GetEnumerator()
    let rec go e = async {
      let! s = en.MoveNext ()
      match s with
      | None -> 
        return Failure e
      | Some hd ->
        let! r = f hd
        match r with
        | Success a -> 
          return Success (a,e)
        | Failure e' -> 
          return! go (m.Merge (e,e')) }
    return! go m.Zero }

  let traverseAsyncResultList (f:'a -> Async<Result<'b, 'e>>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e list>> =
    traverseAsyncResult Monoid.freeList (f >> Async.map (Result.mapError List.singleton)) s
    
  let traverseResult (m:Monoid<'e>) (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e>> =
    traverseAsyncResult m (f >> async.Return) s

  let traverseResultWarn (m:Monoid<'e>) (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b * 'e, 'e>> =
    traverseAsyncResultWarn m (f >> async.Return) s

  let traverseResultList (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e list>> =
    traverseResult (Monoid.freeList) (f >> Result.mapError List.singleton) s

  let sequenceResultWarn (m:Monoid<'e>) (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a * 'e, 'e>> =
    traverseResultWarn m id s
  
  let sequenceResult (m:Monoid<'e>) (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    traverseResult m id s
            
  let sequenceResultList (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    traverseResultList id s

  let iterAsyncParallel (f:'a -> Async<unit>) (s:AsyncSeq<'a>) : Async<unit> =
    AsyncSeq.mapAsyncParallel f s |> AsyncSeq.iter ignore

  let replicateUntilNoneAsync (next:Async<'a option>) : AsyncSeq<'a> =
    AsyncSeq.unfoldAsync 
      (fun () -> next |> Async.map (Option.map (fun a -> a,()))) 
      ()
    