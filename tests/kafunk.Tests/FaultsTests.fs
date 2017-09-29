module FaultsTests

open NUnit.Framework
open FSharp.Control
open System
open System.Threading
open Kafunk

[<Test>]
let ``Async.timeoutResult should timeout`` () =
  let timeMs = 100
  let a = Async.Sleep (timeMs * 2)
  let a = Async.timeoutResultWith ignore (TimeSpan.FromMilliseconds (float timeMs)) a
  let actual = a |> Async.RunSynchronously
  let expected = Failure ()
  shouldEqual expected actual None


[<Test>]
let ``AsyncFunc.timeoutResult should return timeout result and cancel when past timeout`` () =
  
  let serviceTime = TimeSpan.FromMilliseconds 100.0

  for timeout in [true;false] do

    let sleepTime = 
      if timeout then int serviceTime.TotalMilliseconds * 2
      else 0

    let cancelled = ref false

    let sleepEcho () = async {
      do! Async.Sleep sleepTime
      return () }

    let sleepEcho = 
      sleepEcho
      >> Async.tryCancelled (fun _ -> cancelled := true)
    
    let sleepEcho =
      sleepEcho
      |> AsyncFunc.timeoutResult serviceTime
      |> AsyncFunc.mapOut (snd >> Result.mapError ignore)

    let expected = 
      if timeout then Failure ()
      else Success ()

    let actual = sleepEcho () |> Async.RunSynchronously
  
    shouldEqual expected actual (Some (sprintf "timeout=%b result" timeout))
    shouldEqual timeout !cancelled (Some (sprintf "timeout=%b cancellation" timeout))

  
[<Test>]
let ``Faults.AsyncFunc.retryResultList should retry with reevaluation`` () =

  let time = TimeSpan.FromMilliseconds 50.0

  for attempts in [1..5] do

    let mutable i = 1

    let sleepEcho () = 
      if Interlocked.Increment &i > attempts then
        async.Return (Success ())
      else
        async.Return (Failure ())

    let backoff = RetryPolicy.constantMs 10 |> RetryPolicy.maxAttempts attempts

    let sleepEcho =
      sleepEcho 
      |> Faults.AsyncFunc.retryResultList backoff
  
    sleepEcho () |> Async.RunSynchronously |> ignore

    let expected = attempts + 1
    let actual = i

    shouldEqual expected actual None

let partitionByCount 
  (count:int) 
  (before:'a -> Async<'b>)
  (after:'a -> Async<'b>) : 'a -> Async<'b> =
  let mutable i = 0
  fun a -> async {
    if Interlocked.Increment &i > count then
      return! after a
    else
      return! before a }

[<Test>]
let ``Faults.AsyncFunc.retryResultList should retry timeout with backoff and succeed`` () = 

  let time = TimeSpan.FromMilliseconds 100.0
  let sleepTime = int time.TotalMilliseconds * 2

  for attempts in [1..5] do

    for fail in [true;false] do

      let policy = RetryPolicy.constantMs 10 |> RetryPolicy.maxAttempts attempts

      let attempts = if fail then attempts + 1 else attempts

      let sleepEcho =
        let mutable i = 1
        fun () -> async {
          if Interlocked.Increment &i > attempts then
            return ()
          else
            do! Async.Sleep sleepTime
            return () }

      let sleepEcho =
        sleepEcho
        |> AsyncFunc.timeoutResult time
        |> AsyncFunc.mapOut (snd >> Result.mapError ignore)
        |> Faults.AsyncFunc.retryResultList policy

      let expected = 
        if fail then Failure (List.init (attempts - 1) ignore)
        else Success ()

      let actual = sleepEcho () |> Async.RunSynchronously

      shouldEqual expected actual (Some (sprintf "[fail=%A attempts=%i]" fail attempts))

[<Test>]
let ``Faults.AsyncFunc.retry should retry with condition and retry policy`` () =
      
  for attempts in [1..5] do
    
    let policy = RetryPolicy.constantMs 1 |> RetryPolicy.maxAttempts attempts
    
    for shouldRetry in [true;false] do

      let svc () = async.Return ()

      let svcRetry =
        svc
        |> Faults.AsyncFunc.retryAsync (fun (_,r) -> shouldRetry) policy

      let actual = svcRetry () |> Async.RunSynchronously
  
      let expected = 
        if shouldRetry then None
        else Some ()

      shouldEqual expected actual None

[<Test>]
let ``Resource`` () = 
  
  let go = async {

    let! r = 
      Resource.recoverableRecreate 
        (fun ct prev -> async { return () })
        (fun (r,v,s,ex) -> async { return () })

    let! op =
      r
      |> Resource.injectResult
        (fun r a -> async {
          return Failure (ResourceErrorAction.RecoverResume (exn(""), a))
          })


    return ()

  }

  ()