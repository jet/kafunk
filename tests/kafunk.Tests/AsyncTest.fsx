#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

let choose (a:Async<'a>) (b:Async<'a>) : Async<'a> = async {
  let! ct = Async.CancellationToken
  return!
    Async.FromContinuations <| fun (ok,err,cnc) ->
      let state = ref 0
      let cts = CancellationTokenSource.CreateLinkedTokenSource ct
      let cancel () =
        cts.Cancel()
        cts.Dispose()
      let ok a =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
          ok a
          cancel ()
      let err (ex:exn) =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
          cancel ()
          err ex
      let cnc ex =
        if (Interlocked.CompareExchange(state, 1, 0) = 0) then 
          cancel ()
          cnc ex
      Async.startThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
      Async.startThreadPoolWithContinuations (b, ok, err, cnc, cts.Token) }


let chooseTaskOrAsync2 (t:Task<'a>) (a:Async<'a>) : Async<'a> = async {
  let! ct = Async.CancellationToken
  use cts = CancellationTokenSource.CreateLinkedTokenSource ct
  let tcs = new TaskCompletionSource<_>()
  use _reg = cts.Token.Register (fun () -> tcs.TrySetCanceled () |> ignore)
  IVar.intoCancellationToken cts tcs
  //let opts = TaskContinuationOptions.DenyChildAttach
  let t = Task.Factory.ContinueWhenAny([|t|], (fun (t:Task<_>) ->  tcs.TrySetResult t.Result |> ignore), cts.Token)
  //t.ContinueWith((fun (t:Task<'a>) -> tcs.TrySetResult t.Result |> ignore ; cts.Cancel ()), opts) |> ignore
  let a = async {
    try
      let! a = a
      tcs.TrySetResult a |> ignore
      cts.Cancel ()
    with ex ->
      tcs.TrySetException ex |> ignore
      cts.Cancel () }
  Async.Start (a, cts.Token)
  return! tcs.Task |> Async.awaitTaskCancellationAsError }

let chooseTaskOrAsync (t:Task<'a>) (a:Async<'a>) : Async<'a> = async {
  let! ct = Async.CancellationToken
  use cts = CancellationTokenSource.CreateLinkedTokenSource ct
  let tcs = new TaskCompletionSource<_>()
  IVar.intoCancellationToken cts tcs
  let t = async {
    try
      let! t = Async.AwaitTask t
      tcs.TrySetResult t |> ignore
    with ex ->
      tcs.TrySetException ex |> ignore }
  let! _ = Async.StartChild t
  let a = async {
    try
      let! a = a
      tcs.TrySetResult a |> ignore
    with ex ->
      tcs.TrySetException ex |> ignore }
  let! _ = Async.StartChild a
  return! tcs.Task |> Async.awaitTaskCancellationAsError }

let cancelWithToken (ct:CancellationToken) (a:Async<'a>) : Async<'a option> = async {
  let! ct2 = Async.CancellationToken
  use cts = CancellationTokenSource.CreateLinkedTokenSource (ct, ct2)
  let tcs = new TaskCompletionSource<'a option>()
  use _reg = cts.Token.Register (fun () -> tcs.TrySetResult None |> ignore)
  let a = async {
    try
      let! a = a
      tcs.TrySetResult (Some a) |> ignore
    with ex ->
      tcs.TrySetException ex |> ignore }
  Async.Start (a, cts.Token)
  return! tcs.Task |> Async.awaitTaskCancellationAsError }


let go2 = async {
  let cts = new CancellationTokenSource()
  let N = 1000000
  return!
    Seq.init N id
    |> Seq.map (fun i -> async {
      do! cancelWithToken cts.Token (Async.Sleep 100) |> Async.Ignore
      return () })
    |> Async.parallelThrottledIgnore 1000
}

let go = async {
  let t = Task.never
  let N = 1000000
  return!
    Seq.init N id
    |> Seq.map (fun i -> async {
      do! chooseTaskOrAsync2 t (Async.Sleep 100)
      return () })
    |> Async.parallelThrottledIgnore 1000
}


let go3 = async {
  let t = Task.never |> Async.AwaitTask
  let N = 1000000
  return!
    Seq.init N id
    |> Seq.map (fun i -> async {
      do! choose t (Async.Sleep 100)
      return () })
    |> Async.parallelThrottledIgnore 1000
}

Async.RunSynchronously go3