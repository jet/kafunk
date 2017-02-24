#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

module Async =
  
  let raceAsyncsAsTask (a:Async<'a>) (b:Async<'b>) : Async<Choice<'a * Task<'b>, 'b * Task<'a>>> = async {
    let! ct = Async.CancellationToken
    return! Async.FromContinuations <| fun (ok,err,cnc) ->
      let a = Async.StartAsTask (a, cancellationToken = ct)
      let b = Async.StartAsTask (b, cancellationToken = ct)
      a 
      |> Task.extend (fun a -> 
        if a.IsCanceled then cnc (OperationCanceledException())
        elif a.IsFaulted then err (a.Exception :> exn)
        else ok (Choice1Of2 (a.Result, b)) )
      |> ignore
      b 
      |> Task.extend (fun b -> 
        if b.IsCanceled then cnc (OperationCanceledException())
        elif b.IsFaulted then err (b.Exception :> exn)
        else ok (Choice2Of2 (b.Result, a)))
      |> ignore }

  let raceTasks (a:Task<'T>) (b:Task<'U>) =
    Task.WhenAny [| a |> Task.map (fun a -> Choice1Of2 (a, b)) ; b |> Task.map (fun b -> Choice2Of2 (b, a))   |]
    |> Task.join

//  let raceTasksAsAsync (a:Task<'T>) (b:Task<'U>) =
//    raceTasks a b |> Async.AwaitTask

  let raceTasksAsAsync (a:Task<'T>) (b:Task<'U>) : Async<Choice<'T * Task<'U>, 'U * Task<'T>>> =
    async { 
        let! ct = Async.CancellationToken
        let i = Task.WaitAny( [| (a :> Task);(b :> Task) |],ct)
        if i = 0 then return (Choice1Of2 (a.Result, b))
        elif i = 1 then return (Choice2Of2 (b.Result, a)) 
        else return! failwith (sprintf "unreachable, i = %d" i) }

//  let chooseTasks (a:Task<'T>) (b:Task<'U>) : Async<Choice<'T * Task<'U>, 'U * Task<'T>>> =
//    async { 
//        let! t = 
//          Task.WhenAny [| (Task.map Choice1Of2 a) ; (Task.map Choice2Of2 b) |] 
//          |> Task.join 
//          |> Async.AwaitTask
//        match t with
//        | Choice1Of2 a -> 
//          return Choice1Of2 (a, b)
//        | Choice2Of2 b -> 
//          return Choice2Of2 (b, a) }

module internal AsyncSeq =

  let bufferByConditionAndTime (cond:IBoundedMbCond<'T>) (timeoutMs:int) (source:AsyncSeq<'T>) : AsyncSeq<'T[]> = 
    if (timeoutMs < 1) then invalidArg "timeoutMs" "must be positive"
    asyncSeq {
      let buffer = new ResizeArray<_>()
      use ie = source.GetEnumerator()
      let rec loop rem rt = asyncSeq {
        let! move = 
          match rem with
          | Some rem -> async.Return rem
          | None -> Async.StartChildAsTask (ie.MoveNext())
        let t = Stopwatch.GetTimestamp()
        //let! time = Async.StartChildAsTask (Async.Sleep (max 0 rt))
        let! time = Async.StartChildAsTask (Async.Sleep (max 0 rt))
        let! moveOr = Async.chooseTasks move time
        let delta = int ((Stopwatch.GetTimestamp() - t) * 1000L / Stopwatch.Frequency)
        match moveOr with
        | Choice1Of2 (None, _) -> 
          if buffer.Count > 0 then
            yield buffer.ToArray()
        | Choice1Of2 (Some v, _) ->
          buffer.Add v
          cond.Add v
          if cond.Satisfied then
            yield buffer.ToArray()
            buffer.Clear()
            cond.Reset ()
            yield! loop None timeoutMs
          else
            yield! loop None (rt - delta)
        | Choice2Of2 (_, rest) ->
          if buffer.Count > 0 then
            yield buffer.ToArray()
            buffer.Clear()
            cond.Reset ()
            yield! loop (Some rest) timeoutMs
          else
            yield! loop (Some rest) timeoutMs }
      yield! loop None timeoutMs }


let N = 1000000

AsyncSeq.init (int64 N) id

|> AsyncSeq.toObservable
|> Observable.bufferByTimeAndCondition (TimeSpan.FromMilliseconds 100.0) (BoundedMbCond.count 100)
|> AsyncSeq.ofObservableBuffered

//|> AsyncSeq.bufferByConditionAndTime (BoundedMbCond.count 100) 100
//|> AsyncSeq.bufferByCountAndTime 100 100

|> AsyncSeq.iter ignore
|> Async.RunSynchronously