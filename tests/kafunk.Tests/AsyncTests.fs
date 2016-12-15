module AsyncTests

open Kafunk
open NUnit.Framework
open System.Threading
open System.Threading.Tasks

[<Test>]
let ``Async.never should regard infinite timeouts as equal`` () =
  Assert.AreEqual (Async.never, Async.never)

[<Test>]
let ``Async.choose should choose first to complete`` () =
  for fastMs in [1..20] do
    let fast = Async.Sleep fastMs |> Async.map Choice1Of2
    let slow = Async.Sleep (fastMs + 20) |> Async.map Choice2Of2
    let first = Async.choose fast slow
    Assert.AreEqual (fast, first)

[<Test>]
let ``MVar.updateAsync should execute update serially`` () =
  let mv = MVar.create ()
  
  let calling = 1
  let calls = 100

  mv |> MVar.put calling |> Async.RunSynchronously |> ignore
  
  let st = ref 0

  let update i = async {
    do! Async.SwitchToThreadPool ()
    return i + 1 }

  let op calling = async {
    let! i' =
      mv 
      |> MVar.updateAsync (fun i -> async {
        if Interlocked.CompareExchange (st, 1, 0) <> 0 then
          return failwith "overlapping execution detected"
        let! r = 
          if i = calling then update i
          else async { return i }
        if Interlocked.CompareExchange (st, 0, 1) <> 1 then
          return failwith "overlapping execution detected"
        return r })
    return i' }

  let actual = 
    Async.Parallel (Seq.init calls (fun _ -> op calling))
    |> Async.RunSynchronously
    |> List.ofArray
  
  let expected = List.init calls (fun _ -> calling + 1)
  
  shouldEqual expected actual None


[<Test>]
let ``Async.withCancellation should cancel`` () =
  
  let cts = new CancellationTokenSource()
  let cancelled = ref false

  let comp = async {
    let! ct = Async.CancellationToken
    ct.Register (fun () -> cancelled := true) |> ignore
    while true do
      do! Async.Sleep 2 }

  let cancellableComp = Async.cancelWithToken cts.Token comp

  cts.CancelAfter 200

  let expected = None
  let actual = Async.RunSynchronously cancellableComp

  shouldEqual expected actual None
  shouldEqual true !cancelled None


[<Test>]
let ``Async.choose should respect ambient cancellation token`` () =
  
  let cancelled0 = ref false
  let cancelled1 = ref false
  let cancelled2 = ref false

  let comp1 = async {
    let! ct = Async.CancellationToken
    ct.Register (fun () -> cancelled1 := true) |> ignore
    while not (ct.IsCancellationRequested) do
      () }

  let comp2 = async {
    let! ct = Async.CancellationToken
    ct.Register (fun () -> cancelled2 := true) |> ignore
    while not (ct.IsCancellationRequested) do
      () }

  let r = TaskCompletionSource<unit>()

  let c = async {
    let! ct = Async.CancellationToken
    ct.Register (fun () -> cancelled0 := true) |> ignore
    let! _ = Async.choose comp1 comp2
    r.SetResult() }

  let cts = new CancellationTokenSource()

  Async.Start (c, cts.Token)

  cts.CancelAfter 50

  let completed = r.Task.Wait (200)

  shouldEqual true !cancelled0 None
  shouldEqual true !cancelled1 None
  shouldEqual true !cancelled2 None