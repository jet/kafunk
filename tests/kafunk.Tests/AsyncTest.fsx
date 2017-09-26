#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

module AsyncSeq =

  let iterAsyncParallel (f:'a -> Async<unit>) (s:AsyncSeq<'a>) : Async<unit> =
    asyncSeq {
      for a in s do
        let! c = Async.StartChild (f a)
        yield c }
    |> AsyncSeq.iterAsync id

  let spanInclusive (f:'a -> bool) (s:AsyncSeq<'a>) : AsyncSeq<'a> =
    { new IAsyncEnumerable<'a> with
        member __.GetEnumerator () = 
          let en = s.GetEnumerator ()
          let fin = ref false
          { new IAsyncEnumerator<'a> with
              member __.MoveNext () = async {
                if !fin then return None else
                let! next = en.MoveNext ()
                match next with
                | None -> 
                  return None
                | Some a ->
                  if f a then return Some a
                  else 
                    fin := true
                    return Some a }
              member __.Dispose () = 
                en.Dispose () } }
  
  let takeWhileInclusive (f:'a -> bool) (s:AsyncSeq<'a>) : AsyncSeq<'a> =
    { new IAsyncEnumerable<'a> with
        member __.GetEnumerator () = 
          let en = s.GetEnumerator ()
          let fin = ref false
          { new IAsyncEnumerator<'a> with
              member __.MoveNext () = async {
                if !fin then return None else
                let! next = en.MoveNext ()
                match next with
                | None -> 
                  return None
                | Some a ->
                  if f a then return Some a
                  else 
                    fin := true
                    return Some a }
              member __.Dispose () = 
                en.Dispose () } }



let s = asyncSeq {
  yield 1
  yield 100000
  yield 1
  yield 2 }

s
|> AsyncSeq.iterAsyncParallel (fun x -> async {
  printfn "x=%i" x
  do! Async.Sleep x })
|> Async.RunSynchronously



//let s = asyncSeq {  
//  yield (1,10L)
//  yield (2,11L)
//  yield (1,11L)
//  yield (2,12L)
//  do! Async.Sleep -1 }
//
//let endOfTopic : Map<Partition, Offset> = 
//  Map.ofList [ (1,11L) ; (2,12L) ]
//
//let rs =
//  (Map.empty, s)
//  ||> AsyncSeq.scan (fun observedOffsets (p,o) -> observedOffsets |> Map.add p o)
//  |> AsyncSeq.skip 1
//  |> AsyncSeq.takeWhileInclusive (fun observedOffsets -> 
//    let notYetReached =
//      observedOffsets
//      |> Map.toSeq
//      |> Seq.choose (fun (p,o') -> 
//        match endOfTopic |> Map.tryFind p with
//        | Some o when o' < o -> Some (p,o)
//        | _ -> None)
//      |> Seq.toArray  
//    notYetReached.Length > 0)
//  |> AsyncSeq.toList
//
//printfn "%A" rs

  

//let host = ""
//let topic = ""
//let conn = Kafka.connHost host
//let offsetRange = Offsets.offsetRange conn topic [] |> Async.RunSynchronously