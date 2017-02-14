#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks

module Task =
  
  let join (t:Task<Task<'a>>) : Task<'a> =
    t.Unwrap()

  let continueWith (f:Task<'a> -> 'b) (t:Task<'a>) : Task<'b> =
    t.ContinueWith f

module AsyncSeq =

  let tryWithPrint (tag:string) (a:Async<'a>) : Async<'a> =
    async.TryWith (a, fun ex -> async { 
      //printfn "%s%O" tag ex 
      return raise ex })

//  let private peekTaskError (t:Task<_>) (a:Async<'b>) : Async<'b> = async {
//    if t.IsFaulted then 
//      return raise t.Exception
//    else return! a }

  let private chooseTask (t:Task<'a>) (a:Async<'a>) : Async<'a> = async {
    let! a = Async.StartChildAsTask a
    return! Task.WhenAny (t, a) |> Task.join |> Async.AwaitTask }

  /// Starts an async computation as a child.
  /// Returns a Task which completes with error if the computation errors.
  let private startWithError (a:Async<unit>) = async {
    let err = IVar.create ()
    let! _ =
      a
      |> Async.tryWith (fun ex -> async { IVar.error ex err })
      |> Async.StartChild
    return err }

  let mapAsyncParallel (f:'a -> Async<'b>) (s:AsyncSeq<'a>) : AsyncSeq<'b> = asyncSeq {
    use mb = Mb.create ()
    let! err =
      s 
      |> AsyncSeq.iterAsync (fun a -> async {
        let! b = Async.StartChild (f a)
        mb.Post (Some b) })
      |> Async.map (fun _ -> mb.Post None)
      |> startWithError
    yield! 
      AsyncSeq.replicateUntilNoneAsync (chooseTask err.Task (Mb.take mb))
      |> AsyncSeq.mapAsync id }

  let iterAsyncParallel (f:'a -> Async<unit>) (s:AsyncSeq<'a>) : Async<unit> = async {
    use mb = Mb.create ()
    let! err =
      s 
      |> AsyncSeq.iterAsync (fun a -> async {
        let! b = Async.StartChild (f a)
        mb.Post (Some b) })
      |> Async.map (fun _ -> mb.Post None)
      |> startWithError
    return!
      AsyncSeq.replicateUntilNoneAsync (chooseTask err.Task (Mb.take mb))
      |> AsyncSeq.iterAsync id }


let N = 1000
let fail = 50


let op i = async {
  do! Async.SwitchToThreadPool ()
  if i = fail then 
    printfn "error"
    return failwith "error"
  do! Async.Sleep 1
  return 1 }


let res = 
  Seq.init N id
  |> AsyncSeq.ofSeq
  |> AsyncSeq.mapAsyncParallel op
  |> AsyncSeq.mapAsyncParallel (id >> async.Return)
  |> AsyncSeq.mapAsyncParallel (id >> async.Return)
  |> AsyncSeq.mapAsyncParallel (id >> async.Return)
  //|> AsyncSeq.mapAsyncParallel (id >> async.Return)
  //|> AsyncSeq.mapAsyncParallel (id >> async.Return)
  //|> AsyncSeq.mapAsyncParallel (id >> async.Return)
  //|> AsyncSeq.mapAsyncParallel op
  //|> AsyncSeq.mapAsyncParallel op
  //|> FSharp.Control.AsyncSeq.mapAsyncParallel op
  //|> AsyncSeq.iter ignore
  //|> AsyncSeq.iterAsyncParallel (id >> Async.Ignore)
  |> AsyncSeq.iterAsyncParallel (async.Return >> Async.Ignore)
  |> Async.Catch
  |> Async.RunSynchronously

