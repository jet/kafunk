[<AutoOpen>]
module internal Kafunk.AsyncSeq

open FSharp.Control
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

/// An async transducer.
type AsyncPipe<'a, 'b> = Async<Step<'a, 'b>>

/// An individual step of an async transducer.
and Step<'a, 'b> =
  | Halt
  | Emit of 'b * tail:AsyncPipe<'a, 'b>
  | Await of ('a option -> AsyncPipe<'a, 'b>)

module AsyncPipe =
    
  let encased (step:Step<'a, 'b>) : AsyncPipe<'a, 'b> = 
    async.Return step

  [<GeneralizableValue>]
  let halt<'a, 'b> : AsyncPipe<'a, 'b> = 
    encased Halt

  let emit (b:'b) (tail:AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    encased (Emit (b, tail))

  let emitHalt (b:'b) : AsyncPipe<'a, 'b> =
    emit b halt

  let await (f:'a option -> AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    encased (Await f)

  let awaitHalt (f:'a -> AsyncPipe<'a, 'b>) : AsyncPipe<'a, 'b> =
    await (function Some a -> f a | None -> halt)

  let drain (pipe:AsyncPipe<_, 'b>) : AsyncSeq<'b> =
    { new IAsyncEnumerable<_> with
        member __.GetEnumerator () =
          let step = ref pipe
          { new IAsyncEnumerator<_> with
              member __.MoveNext () = async {
                let! step' = !step
                match step' with
                | Halt | Await _ -> 
                  return None
                | Emit (a,tail) ->
                  step := tail
                  return Some a }
              member __.Dispose () = () } }

  /// Feeds an async sequence into a transducer and emits the resulting async sequence.
  let transduce (pipe:AsyncPipe<'a, 'b>) (source:AsyncSeq<'a>) : AsyncSeq<'b> = 
    asyncSeq {
      use enum = source.GetEnumerator()
      let rec go pipe = asyncSeq {
        let! step = pipe
        match step with
        | Halt -> ()
        | Emit (b,tail) ->
          yield b
          yield! go tail
        | Await recv ->
          let! next = enum.MoveNext()
          match next with
          | None ->
            yield! drain (recv None)
          | Some a ->
            yield! go (recv (Some a)) }
      yield! go pipe }
            
  let windowed (windowSize:int) : AsyncPipe<'a, 'a[]> =
    let rec loop (win:ResizeArray<_>) =
      awaitHalt (fun a ->
        win.Add a
        if win.Count > windowSize then
          win.RemoveAt 0
        if win.Count = windowSize then
          emit (win.ToArray()) (loop win)
        else 
          loop win)
    loop (ResizeArray<_>(windowSize))

  // TODO: non-deterministic choice for time
  let bufferByTime (timeSpan:TimeSpan) : AsyncPipe<'a, 'a[]> =
    let rec loop (t:DateTime) (buf:ResizeArray<_>) =
      awaitHalt (fun a ->
        buf.Add a
        let t' = DateTime.UtcNow
        if (t' - t) >= timeSpan then
          emit (buf.ToArray()) (loop t' (ResizeArray<_>()))
        else
          loop t buf)
    loop DateTime.UtcNow (ResizeArray<_>())

  let bufferByState (z:'s) (f:'s -> ResizeArray<'a> -> Choice<'s, 's>) : AsyncPipe<'a, 'a[]> =
    let rec loop (s:'s) (buf:ResizeArray<_>) =
      awaitHalt (fun a ->
        buf.Add a
        match f s buf with
        | Choice1Of2 s' ->
          emit (buf.ToArray()) (loop s' (ResizeArray<_>()))
        | Choice2Of2 s' ->
          loop s' buf)
    loop z (ResizeArray<_>())

  let bufferByBufferState (f:ResizeArray<'a> -> bool) : AsyncPipe<'a, 'a[]> =
    bufferByState () (fun _ buf -> if f buf then Choice1Of2 () else Choice2Of2 ())
    
        
        
/// Module with helper functions for working with asynchronous sequences
module AsyncSeq =

  let windowed (windowSize:int) : AsyncSeq<'a> -> AsyncSeq<'a[]> =
    AsyncPipe.transduce (AsyncPipe.windowed windowSize)

  let unfoldInfiniteAsync (s:'s) (f:'s -> Async<'a * 's>) : AsyncSeq<'a> =
    AsyncSeq.unfoldAsync (f >> Async.map Some) s 

  let interleaveChoice (a:AsyncSeq<'a>) (b:AsyncSeq<'b>) : AsyncSeq<Choice<'a, 'b>> =
    AsyncSeq.interleave (a |> AsyncSeq.map Choice1Of2) (b |> AsyncSeq.map Choice2Of2)

  /// A traversal of an async sequence specialized to the Result type.
  /// Returns the first successful result or a list of all erroneous results.
  let traverseAsyncResult
    (m:Monoid<'e>)
    (f:'a -> Async<Result<'b, 'e>>) 
    (s:AsyncSeq<'a>) : Async<Result<'b, 'e>> = async {
    use en = s.GetEnumerator()
    let rec go e = async {
      let! s = en.MoveNext ()
      match s with
      | None -> 
        return Failure e
      | Some hd ->
        let! r = f hd
        match r with
        | Success a -> 
          return Success a
        | Failure e' -> 
          return! go (m.Merge (e,e')) }
    return! go m.Zero }

  let traverseAsyncResultWarn
    (m:Monoid<'e>)
    (f:'a -> Async<Result<'b, 'e>>) 
    (s:AsyncSeq<'a>) : Async<Result<'b * 'e, 'e>> = async {
    use en = s.GetEnumerator()
    let rec go e = async {
      let! s = en.MoveNext ()
      match s with
      | None -> 
        return Failure e
      | Some hd ->
        let! r = f hd
        match r with
        | Success a -> 
          return Success (a,e)
        | Failure e' -> 
          return! go (m.Merge (e,e')) }
    return! go m.Zero }

  let traverseAsyncResultList (f:'a -> Async<Result<'b, 'e>>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e list>> =
    traverseAsyncResult Monoid.freeList (f >> Async.map (Result.mapError List.singleton)) s
    
  let traverseResult (m:Monoid<'e>) (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e>> =
    traverseAsyncResult m (f >> async.Return) s

  let traverseResultWarn (m:Monoid<'e>) (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b * 'e, 'e>> =
    traverseAsyncResultWarn m (f >> async.Return) s

  let traverseResultList (f:'a -> Result<'b, 'e>) (s:AsyncSeq<'a>) : Async<Result<'b, 'e list>> =
    traverseResult (Monoid.freeList) (f >> Result.mapError List.singleton) s

  let sequenceResultWarn (m:Monoid<'e>) (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a * 'e, 'e>> =
    traverseResultWarn m id s
  
  let sequenceResult (m:Monoid<'e>) (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a, 'e>> =
    traverseResult m id s
            
  let sequenceResultList (s:AsyncSeq<Result<'a, 'e>>) : Async<Result<'a, 'e list>> =
    traverseResultList id s

  // TODO: refactor to a more generic condition
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
        let time = Task.Delay (max 0 rt)
        let! moveOr = Async.chooseTasksUnit move time
//        let! time = Async.StartChildAsTask (Async.Sleep (max 0 rt))
//        let! moveOr = Async.chooseTasks move time
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

  let bufferByCountAndTimeAndTimeInterval (bufferSize:int) (timeoutMs:int) (timeInterval:int) (source:AsyncSeq<'T>) : AsyncSeq<'T[]> = 
    if (bufferSize < 1) then invalidArg "bufferSize" "must be positive"
    if (timeoutMs < 1) then invalidArg "timeoutMs" "must be positive"
    if (timeInterval < 1) then invalidArg "timeIntervalMs" "must be positive"
    asyncSeq {
      let buffer = new ResizeArray<_>()
      use ie = source.GetEnumerator()
      let rec loop rem rt = asyncSeq {
        let! move = 
          match rem with
          | Some rem -> async.Return rem
          | None -> Async.StartChildAsTask(ie.MoveNext())
        let t = Stopwatch.GetTimestamp()
        let! time = Async.StartChildAsTask(Async.Sleep (max 0 rt))
        let! interval = Async.StartChildAsTask(Async.Sleep timeInterval)
        let! moveOr = Async.chooseTasks3 move time interval
        let delta = int ((Stopwatch.GetTimestamp() - t) * 1000L / Stopwatch.Frequency)
        match moveOr with
        | Choice1Of3 (None, _, _) -> 
          if buffer.Count > 0 then
            yield buffer.ToArray()
        | Choice1Of3 (Some v, _, _) ->
          buffer.Add v
          if buffer.Count = bufferSize then
            yield buffer.ToArray()
            buffer.Clear()
            yield! loop None timeoutMs
          else
            yield! loop None (rt - delta)
        | Choice2Of3 (_, rest, _) ->
          if buffer.Count > 0 then
            yield buffer.ToArray()
            buffer.Clear()
            yield! loop (Some rest) timeoutMs
          else
            yield! loop (Some rest) timeoutMs
        | Choice3Of3 (_, rest, _) ->
          if buffer.Count > 0 then
            yield buffer.ToArray()
            buffer.Clear()
            yield! loop (Some rest) (rt - delta)
          else 
            yield! loop (Some rest) (rt - delta)
      }
      yield! loop None timeoutMs
    }

  let ofObservableBuffered (source : System.IObservable<_>) = 
    asyncSeq {
      let! ct = Async.CancellationToken
      let cts = CancellationTokenSource.CreateLinkedTokenSource (ct)
      try 
        // The body of this agent returns immediately.  It turns out this is a valid use of an F# agent, and it
        // leaves the agent available as a queue that supports an asynchronous receive.
        //
        // This makes the cancellation token is somewhat meaningless since the body has already returned.  However
        // if we don't pass it in then the default cancellation token will be used, so we pass one in for completeness.
        use agent = MailboxProcessor<_>.Start((fun _ -> async.Return() ), cancellationToken = cts.Token)
        use _d = source |> Observable.asUpdates |> Observable.subscribe agent.Post
        let fin = ref false
        while not fin.Value do 
          let! msg = agent.Receive()
          match msg with
          | Observable.ObservableUpdate.Error e -> e.Throw()
          | Observable.Completed -> fin := true
          | Observable.Next v -> yield v 
      finally 
         // Cancel on early exit 
         cts.Cancel() }

  let mergeChoice3 (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) (s3:AsyncSeq<'c>) : AsyncSeq<Choice<'a, 'b, 'c>> =
    AsyncSeq.mergeAll 
      [ s1 |> AsyncSeq.map Choice1Of3 ; s2 |> AsyncSeq.map Choice2Of3 ; s3 |> AsyncSeq.map Choice3Of3 ]

  let mergeChoice4 (s1:AsyncSeq<'a>) (s2:AsyncSeq<'b>) (s3:AsyncSeq<'c>) (s4:AsyncSeq<'d>) : AsyncSeq<Choice<'a, 'b, 'c, 'd>> =
    AsyncSeq.mergeAll 
      [ s1 |> AsyncSeq.map Choice1Of4 ; s2 |> AsyncSeq.map Choice2Of4 ; s3 |> AsyncSeq.map Choice3Of4 ; s4 |> AsyncSeq.map Choice4Of4 ]

  /// merges multiple async seqs together, distribution spread across evenly throughout each async seq
  let mergeAllRoundRobin (ss:AsyncSeq<'T> list) : AsyncSeq<'T> =
    asyncSeq { 
      let n = ss.Length
      if n > 0 then 
        let ies = [| for source in ss -> source.GetEnumerator()  |]
        let tasks = Array.zeroCreate n
        for i in 0 .. ss.Length - 1 do 
          let! task = Async.StartChildAsTask (ies.[i].MoveNext())
          do tasks.[i] <- task
        let fin = ref n
        while fin.Value > 0 do 
          let! results = 
            tasks 
            |> Array.map(fun t -> async { 
              let! ti = Task.WhenAny([t]) |> Async.AwaitTask
              let i  = Array.IndexOf (tasks, ti)
              let v = ti.Result
              match v.IsSome with 
              | true -> 
                  let! task = Async.StartChildAsTask (ies.[i].MoveNext())
                  do tasks.[i] <- task
              | false ->
                  let t = System.Threading.Tasks.TaskCompletionSource()
                  tasks.[i] <- t.Task // result never gets set
                  fin := fin.Value - 1
              return v
               })
            |> Async.Parallel         
          for res in results do
            if res.IsSome then
              yield res.Value
            
    }