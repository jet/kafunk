module FaultsTests

open NUnit.Framework
open FSharp.Control
open System
open System.Threading
open Kafunk

[<Test>]
let ``should return timeout result and cancel when past timeout`` () =
  
  let serviceTime = TimeSpan.FromMilliseconds 50.0

  for timeout in [true;false] do

    let sleepTime = 
      if timeout then int serviceTime.TotalMilliseconds * 2
      else 0

    let cancelled = ref false

    let sleepEcho () = async {
      use! _cnc = Async.OnCancel (fun () -> cancelled := true)
      do! Async.Sleep sleepTime
      return () }

    let sleepEcho =
      sleepEcho
      |> AsyncFunc.timeoutResult serviceTime
      |> AsyncFunc.mapOut (snd >> Result.mapError ignore)

    let expected = 
      if timeout then Failure ()
      else Success ()

    let actual = sleepEcho () |> Async.RunSynchronously
  
    shouldEqual expected actual None
    shouldEqual timeout !cancelled None

  
[<Test>]
let ``should retry with reevaluation`` () =

  let time = TimeSpan.FromMilliseconds 50.0

  for attempts in [1..5] do

    let mutable i = 0

    let sleepEcho () = 
      if Interlocked.Increment &i > attempts then
        async.Return (Success ())
      else
        async.Return (Failure ())

    let backoff = Backoff.constant 10 |> Backoff.maxAttempts attempts

    let sleepEcho =
      sleepEcho 
      |> Faults.AsyncFunc.retryResultList backoff
  
    sleepEcho () |> Async.RunSynchronously |> ignore

    let expected = attempts + 1
    let actual = i

    shouldEqual expected actual None

[<Test>]
let ``should retry timeout with backoff and succeed`` () = 

  let time = TimeSpan.FromMilliseconds 50.0

  for attempts in [1..5] do

    for fail in [true;false] do

      let sleepEcho =
        let mutable i = 0
        let attempts = if fail then attempts + 1 else attempts
        fun () -> async {
          if Interlocked.Increment &i > attempts then
            return ()
          else
            do! Async.Sleep (int time.TotalMilliseconds * 2)
            return () }

      let backoff = Backoff.constant 10 |> Backoff.maxAttempts attempts

      let sleepEcho =
        sleepEcho
        |> AsyncFunc.timeoutResult time
        |> AsyncFunc.mapOut (snd >> Result.mapError ignore)
        |> Faults.AsyncFunc.retryResultList backoff

      let expected = 
        if fail then Failure (List.init (attempts + 1) ignore)
        else Success ()

      let actual = sleepEcho () |> Async.RunSynchronously

      shouldEqual expected actual None

