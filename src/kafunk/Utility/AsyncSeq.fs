namespace Kafunk

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks

/// Module with helper functions for working with asynchronous sequences
module AsyncSeq =

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