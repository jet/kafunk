//ignore this, this is for test purpose
#r "system.net.http.dll"
#r "bin/Debug/FSharp.Control.AsyncSeq.dll"
#r "bin/Debug/Kafunk.dll"
#load "../../tests/kafunk.Tests/Refs.fsx"

open System
open Kafunk
open System.Collections.Concurrent
open FSharp.Control
open System.Diagnostics

module AsyncSeq = 

  open System.Threading.Tasks

  let bufferByCountAndTimeInterval (bufferSize:int) (timeInterval:int) (source:AsyncSeq<'T>) : AsyncSeq<'T[]> = 
    if (bufferSize < 1) then invalidArg "bufferSize" "must be positive"
    if (timeInterval < 1) then invalidArg "timeoutMs" "must be positive"
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
        let! moveOr = Async.chooseTasks move time
        match moveOr with
        | Choice1Of2 (None, _) -> 
          if buffer.Count > 0 then
            yield buffer.ToArray()
        | Choice1Of2 (Some v, _) ->
          buffer.Add v
          if buffer.Count = bufferSize then
            yield buffer.ToArray()
            buffer.Clear()
          yield! loop None timeInterval
        | Choice2Of2 (_, rest) ->
          if buffer.Count > 0 then
            yield buffer.ToArray()
            buffer.Clear()
            yield! loop (Some rest) timeInterval
          else
            yield! loop (Some rest) timeInterval
      }
      yield! loop None timeInterval
    }

  let chooseTasks3 (a:Task<'T>) (b:Task<'U>) (c:Task<'O>): Async<Choice<'T * Task<'U> * Task<'O>, 'U * Task<'T> * Task<'O>, 'O * Task<'T> * Task<'U>>> =
    async { 
        let! ct = Async.CancellationToken
        let i = Task.WaitAny( [| (a :> Task);(b :> Task); (c :> Task) |],ct)
        if i = 0 then return (Choice1Of3 (a.Result, b, c))
        elif i = 1 then return (Choice2Of3 (b.Result, a, c)) 
        elif i = 2 then return (Choice3Of3 (c.Result, a, b))
        else return! failwith (sprintf "unreachable, i = %d" i) }

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
        let! moveOr = chooseTasks3 move time interval
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
  let log = Log.create "Kafunk.Buffer"
  let timer = Metrics.timer log 5000

  let iterAsyncWithMeasure (action:'T -> Async<unit>) (source:AsyncSeq<'T>) =
    let ie = source.GetEnumerator()
    let rec go (emu:IAsyncEnumerator<'T>) (prev:int64)= async {
      let! next = emu.MoveNext()
      match next with
      | Some x -> 
        let t2 = Stopwatch.GetTimestamp ()
        timer.Record (int ((t2 - prev) * 1000L / Stopwatch.Frequency))
        do! action x
        return! go emu t2
      | None -> return ()}
    let res = go ie <| Stopwatch.GetTimestamp()
    res

/// A bounded asynchronous buffer.
type Buffer<'a> (bound:BufferBound) =

  let queue = 
    match bound with
    | Unbounded | DiscardAfter _ -> new BlockingCollection<'a>()
    | BlockAfter c -> new BlockingCollection<'a>(c)
  
  let blockingEvent = Event<int>()
  let discardingEvent = Event<int>()

  /// Triggered when the buffer blocks an item.
  member __.Blocking = blockingEvent.Publish

  /// Triggered when the buffer discards an item.
  member __.Discarding = discardingEvent.Publish

  /// Adds an item to the buffer respecting the bound configuration.
  /// Returns a bool indicating whether the item was added.
  member __.Add (a:'a) = 
    match bound with
    | Unbounded -> 
      queue.Add a
      true
    | DiscardAfter c ->
      if queue.Count >= c then 
        discardingEvent.Trigger queue.Count
        false
      else 
        queue.Add a
        true
    | BlockAfter c ->
      if queue.Count >= c then blockingEvent.Trigger queue.Count
      queue.Add a
      true

  /// Returns an async computation which consumes the buffer using the specified function, 
  /// batched by time and space.
  member __.Consume (batchSize:int, timeInterval:int, f:'a[] -> Async<unit>) : Async<unit> =
    queue.GetConsumingEnumerable ()
    |> AsyncSeq.ofSeq
    |> AsyncSeq.bufferByCountAndTimeInterval batchSize timeInterval
    |> AsyncSeq.iterAsyncWithMeasure f
 
  /// Returns an async computation which consumes the buffer using the specified function, 
  /// batched by time and space.
  member __.Consume' (batchSize:int, batchTimeMs:int, timeInterval:int,  f:'a[] -> Async<unit>) : Async<unit> =
    queue.GetConsumingEnumerable ()
    |> AsyncSeq.ofSeq
    |> AsyncSeq.bufferByCountAndTimeAndTimeInterval batchSize batchTimeMs timeInterval
    |> AsyncSeq.iterAsyncWithMeasure f
    
  /// Closes the buffer.
  member __.Close () =
    queue.CompleteAdding ()

  interface IDisposable with
    member __.Dispose () = __.Close ()

/// A bound configuration for a buffer.
and BufferBound =  
  
  /// An unbounded buffer.
  | Unbounded

  /// The buffer discards item after reaching the specified capacity.
  | DiscardAfter of capacity:int

  /// The buffer blocks additions after reaching the specified capacity.
  | BlockAfter of capacity:int

module Buffer =

  let startBlockingWithConsumer 
    (capacity:int) 
    (batchSize:int) 
    (batchTimeMs:int) 
    (consume:'a[] -> Async<unit>)
    (blocking:int -> unit) : 'a -> bool =
    let buf = new Buffer<'a> (BufferBound.BlockAfter capacity)
    buf.Consume (batchSize, batchTimeMs, consume) |> Async.Start
    buf.Blocking |> Event.add blocking
    buf.Add
  
  let startDiscardingWithConsumer
    (capacity:int)
    (batchSize:int) 
    (timeIntervalMs:int) 
    (consume:'a[] -> Async<unit>)
    (discarding:int -> unit) : 'a -> bool =
    let buf = new Buffer<'a> (BufferBound.DiscardAfter capacity)
    buf.Consume (batchSize, timeIntervalMs, consume) |> Async.Start
    buf.Discarding |> Event.add discarding
    buf.Add

  let startDiscardingWithConsumer'
    (capacity:int) 
    (batchSize:int) 
    (batchTimeMs:int) 
    (timeIntervalMs:int)
    (consume:'a[] -> Async<unit>)
    (discarding:int -> unit) : 'a -> bool =
    let buf = new Buffer<'a> (BufferBound.DiscardAfter capacity)
    buf.Consume' (batchSize, batchTimeMs, timeIntervalMs, consume) |> Async.Start
    buf.Discarding |> Event.add discarding
    buf.Add

module BufferingProducer = 
  type BufferingProducer  = {
    producer : Producer
    buffer : ProducerMessage -> bool }

  let private Log = Log.create "Kafunk.BufferingProducer"

  let timer = Metrics.timer Log 5000

  let createBufferingProducer (conn:KafkaConn) (cfg:ProducerConfig) (capacity:int) (batchSize:int) (timeIntervalMs:int) (errorHandle:(ProducerMessage seq -> unit) option)= 
    let producer = 
      Producer.create conn cfg

    let consume (producer: Producer) (batch: ProducerMessage seq): Async<unit> = async {
      try
        //let t1 = Stopwatch.GetTimestamp ()
        Log.info "send batch to producer, size is %d" (Seq.length batch)
        do! Producer.produceBatched producer batch |> Async.Ignore
        //let t2 = Stopwatch.GetTimestamp ()
        //timer.Record (int (TimeSpan(t2 - t1).TotalMilliseconds))
        return ()
      with ex ->
        Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex cfg.topic
        match errorHandle with
        | Some x -> x batch
        | None -> () }

    {producer = producer; buffer = Buffer.startDiscardingWithConsumer capacity batchSize timeIntervalMs (consume producer) (fun x -> Log.warn "buffering_producer_process_overflow_warning|cap=%O size=%O" capacity x)}

  let createBufferingProducer' (conn:KafkaConn) (cfg:ProducerConfig) (capacity:int) (batchSize:int) (batchTimeMs:int) (timeIntervalMs:int) (errorHandle:(ProducerMessage seq -> unit) option)= 
    let producer = 
      Producer.create conn cfg

    let consume (producer: Producer) (batch: ProducerMessage seq): Async<unit> = async {
      try
        //let t1 = Stopwatch.GetTimestamp ()
        do! Producer.produceBatched producer batch |> Async.Ignore
        //let t2 = Stopwatch.GetTimestamp ()
        //timer.Record (int (TimeSpan(t2 - t1).TotalMilliseconds))
        return ()
      with ex ->
        Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex cfg.topic
        match errorHandle with
        | Some x -> x batch
        | None -> () }

    {producer = producer; buffer = Buffer.startDiscardingWithConsumer' capacity batchSize batchTimeMs timeIntervalMs (consume producer) (fun x -> Log.warn "buffering_producer_process_overflow_warning|cap=%O size=%O" capacity x)}
  
  let produceWithBuffer (producer : BufferingProducer) (message : ProducerMessage) = 
    producer.buffer message


//test code below
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading
open Refs
open Kafunk.AsyncEx.Async

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let N = argiDefault 3 "10000" |> Int64.Parse
let batchSize = argiDefault 4 "10" |> Int32.Parse
let messageSize = argiDefault 5 "10" |> Int32.Parse
let parallelism = argiDefault 6 "1" |> Int32.Parse

let volumeMB = (N * int64 messageSize) / int64 1000000

let payload = 
  let bytes = Array.zeroCreate messageSize
  let rng = Random()
  rng.NextBytes bytes
  bytes

let batchCount = int (N / int64 batchSize)

Log.info "producer_run_starting|host=%s topic=%s messages=%i batch_size=%i batch_count=%i message_size=%i parallelism=%i MB=%i" 
  host topic N batchSize batchCount messageSize parallelism volumeMB

let conn = Kafka.connHost host

let producerCfg =
  ProducerConfig.create (
    topic, 
    Partitioner.roundRobin, 
    requiredAcks = RequiredAcks.AllInSync,
    timeout = ProducerConfig.DefaultTimeoutMs,
    bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes,
    batchSizeBytes = ProducerConfig.DefaultBatchSizeBytes,
    //batchSizeBytes = 0,
    batchLingerMs = ProducerConfig.DefaultBatchLingerMs,
    compression = CompressionCodec.None
    //maxInFlightRequests = 1
    )

let buffer = BufferingProducer.createBufferingProducer' conn producerCfg 1000 100 1000 90 None
//let buffer = BufferingProducer.createBufferingProducer conn producerCfg 1000 100 150 None
let produce = BufferingProducer.produceWithBuffer buffer
let sw = Stopwatch.StartNew()
let mutable completed = 0L


let x =   
  Array.init batchCount id
  |> Array.map (fun batchNo ->
    try
      sleep (TimeSpan.FromMilliseconds <| 100.0) |> Async.RunSynchronously
      let msgs = Array.init batchSize (fun i -> ProducerMessage.ofBytes payload)
      let res =
        msgs
        |> Seq.map (fun m -> produce m)
      let (total: int) = Seq.fold (fun s x -> if x then (s + 1) else s) 0 res
      Interlocked.Add(&completed, int64 total) |> ignore
      Log.info "Completed is %O, total is %O" completed total
      res
    with ex ->
      Log.error "produce_error|%O" ex
      raise ex )
sw.Stop ()

let missing = N - completed
let ratePerSec = float completed / sw.Elapsed.TotalSeconds

Log.info "producer_run_completed|messages=%i missing=%i batch_size=%i message_size=%i parallelism=%i elapsed_sec=%f rate_per_sec=%f MB=%i" 
  completed missing batchSize messageSize parallelism sw.Elapsed.TotalSeconds ratePerSec volumeMB


Thread.Sleep 60000