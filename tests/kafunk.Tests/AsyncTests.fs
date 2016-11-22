module AsyncTests

open Kafunk
open NUnit.Framework

[<Test>]
let ``should regard infinite timeouts as equal`` () =
  Assert.AreEqual (Async.never, Async.never)

[<Test>]
let ``should choose first to complete`` () =
  for fastMs in [1..10] do
    let fast = Async.Sleep fastMs |> Async.map Choice1Of2
    let slow = Async.Sleep (fastMs * 5) |> Async.map Choice2Of2
    let first = Async.choose fast slow  
    Assert.AreEqual (fast, fast)
