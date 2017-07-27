#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

let N = 10000

module AsyncSeq =

  let replicateUntilNoneAsync (next:Async<'a option>) : AsyncSeq<'a> =
    AsyncSeq.unfoldAsync 
      (fun () -> next |> Async.map (Option.map (fun a -> a,()))) 
      ()

  let private chooseTaskAsTask (t:Task<'a>) (a:Async<'a>) = async {
    let! a = Async.StartChildAsTask a
    return Task.WhenAny (t, a) |> Task.join }

  let private chooseTask (t:Task<'a>) (a:Async<'a>) : Async<'a> =
    chooseTaskAsTask t a |> Async.bind Async.awaitTaskCancellationAsError

  let iterAsyncParallelThrottled (parallelism:int) (f:'a -> Async<unit>) (s:AsyncSeq<'a>) : Async<unit> = async {
    use mb = Mb.create ()
    use sm = new SemaphoreSlim(parallelism)
    let! err =
      s 
      |> AsyncSeq.iterAsync (fun a -> async {
        do sm.Wait ()
        let! b = Async.StartChild (async {
          try do! f a
          finally sm.Release () |> ignore })
        mb.Post (Some b) })
      |> Async.map (fun _ -> mb.Post None)
      |> Async.StartChildAsTask
    return!
      replicateUntilNoneAsync (chooseTask (err |> Task.taskFault) (Mb.take mb))
      |> AsyncSeq.iterAsync id }

AsyncSeq.init 100L id
|> AsyncSeq.iterAsyncParallelThrottled 10
  (fun i -> async { 
    printfn "%i" i
    do! Async.Sleep 1000
    return () })
|> Async.RunSynchronously