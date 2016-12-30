#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"


open Kafunk
open System
open System.Threading

let N = 1000000

//let choose i = async {
//  return! Async.choose2 (Async.Sleep 1) (Async.Sleep 1) }

[<AutoOpen>]
module Ex =
  
  type Async with

    /// Creates an async computation which completes when any of the argument computations completes.
    /// The other computation is cancelled.
    static member choose3 (a:Async<'a>) (b:Async<'a>) : Async<'a> = async {
      let! ct = Async.CancellationToken
      return!
        Async.FromContinuations <| fun (ok,err,cnc) ->
          let state = ref 0
          let cts = CancellationTokenSource.CreateLinkedTokenSource ct
          let cancel () = 
            cts.Cancel ()
            //cts.Dispose () // under load, this can cause slow downs and errors interllay in Async
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
          Async.StartThreadPoolWithContinuations (a, ok, err, cnc, cts.Token)
          Async.StartThreadPoolWithContinuations (b, ok, err, cnc, cts.Token) }

    static member choose2 a b : Async<'T> = async {
      let! ct = Async.CancellationToken
      return!
        Async.FromContinuations (fun (cont, econt, ccont) ->
          let result1 = ref (Choice1Of3())
          let result2 = ref (Choice1Of3())
          let handled = ref false
          let lock f = lock handled f
          let cts = CancellationTokenSource.CreateLinkedTokenSource ct

          // Called when one of the workflows completes
          let complete () = 
            let op =
              lock (fun () ->
                // If we already handled result (and called continuation)
                // then ignore. Otherwise, if the computation succeeds, then
                // run the continuation and mark state as handled.
                // Only throw if both workflows failed.
                match !handled, !result1, !result2 with 
                | true, _, _ -> ignore
                | false, (Choice2Of3 value), _ 
                | false, _, (Choice2Of3 value) -> 
                    handled := true
                    cts.Cancel ()
                    //cts.Dispose ()
                    (fun () -> cont value)
                | false, Choice3Of3 e1, Choice3Of3 e2 -> 
                    handled := true
                    cts.Cancel () 
                    //cts.Dispose ()
                    (fun () -> econt (new AggregateException("Both clauses of a choice failed.", [| e1; e2 |])))
                | false, Choice1Of3 _, Choice3Of3 _ 
                | false, Choice3Of3 _, Choice1Of3 _ 
                | false, Choice1Of3 _, Choice1Of3 _ -> ignore)
            op()
            cts.Cancel()

          let run resCell a = async {
            try
              let! res = a
              lock (fun () -> resCell := Choice2Of3 res)
            with e ->
              lock (fun () -> resCell := Choice3Of3 e)
            complete () }

          Async.Start (run result1 a, cts.Token)
          Async.Start (run result2 b, cts.Token)) }


let choose i = async {
  return! Async.choose (Async.empty) (Async.empty) }

Seq.init N choose
|> Async.ParallelThrottledIgnore Int32.MaxValue
//|> Async.Parallel
|> Async.RunSynchronously
|> ignore



// Real: 00:00:02.676, CPU: 00:00:11.203, GC gen0: 520, gen1: 217, gen2: 1