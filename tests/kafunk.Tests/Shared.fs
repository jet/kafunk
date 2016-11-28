namespace Kafunk

open System
open NUnit.Framework

[<AutoOpen>]
module Testing =

  let rec IsCancellationExn (e:exn) =
    match e with
    | :? OperationCanceledException -> true
    | :? TimeoutException -> true
    | :? AggregateException as x -> x.InnerExceptions |> Seq.filter (IsCancellationExn) |> Seq.isEmpty |> not
    | _ -> false

  /// Time after which uncompleted async computations are considered equal.
  let NEVER_TIMEOUT_MS = 2000

  let shouldEqual (expected:'a) (actual:'a) (msg:string option) =    
    if expected <> actual then
      let msg = 
        match msg with
        | Some msg -> sprintf "expected=%A\nactual=%A message=%s" expected actual msg
        | None -> sprintf "expected=%A\nactual=%A" expected actual
      Assert.Fail msg

  type Async with
    static member timeoutMs (timeoutMs:int) (a:Async<'a>) = async {
      let! a = Async.StartChild(a, timeoutMs)
      return! a }

  type Assert with

    static member AreEqual (expected:Async<'a>, actual:Async<'a>) =
      Assert.AreEqual (expected, actual, null, NEVER_TIMEOUT_MS)

    static member AreEqual (expected:Async<'a>, actual:Async<'a>, ?message) =
      Assert.AreEqual (expected, actual, defaultArg message null, NEVER_TIMEOUT_MS)

    static member AreEqual (expected:Async<'a>, actual:Async<'a>, ?message, ?timeout) =
      let expected = Async.RunSynchronously (expected |> Async.timeoutMs (defaultArg timeout NEVER_TIMEOUT_MS) |> Async.Catch)
      let actual = Async.RunSynchronously (actual |> Async.timeoutMs (defaultArg timeout NEVER_TIMEOUT_MS) |> Async.Catch)
      match expected,actual with
      | Choice2Of2 e1, Choice2Of2 e2 -> 
        if IsCancellationExn e1 && IsCancellationExn e2 then ()
        else
          shouldEqual e1 e2 message  
      | Choice1Of2 a, Choice1Of2 b ->
        shouldEqual a b message
      | _ -> 
        Assert.Fail (defaultArg message "")
        
      
      