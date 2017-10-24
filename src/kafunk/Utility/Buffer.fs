[<AutoOpen>]
module internal Kafunk.Buffer

open System.Collections.Concurrent
open FSharp.Control
open System

/// A bounded asynchronous buffer.
type Buffer<'a> internal (bound:BufferBound) =

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

  /// Get the size of queue
  member __.Size = queue.Count

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
  member __.Consume (batchSize:int, batchTimeMs:int, timeInterval:int,  f:'a[] -> Async<unit>) : Async<unit> =
    queue.GetConsumingEnumerable ()
    |> AsyncSeq.ofSeq
    |> AsyncSeq.bufferByCountAndTimeAndTimeInterval batchSize batchTimeMs timeInterval
    |> AsyncSeq.iterAsync f
    
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

  let getSize (buf: Buffer<'a>) = buf.Size

  let startBlockingWithConsumer 
    (capacity:int) 
    (batchSize:int) 
    (batchTimeMs:int)
    (timeIntervalMs:int)
    (consume:'a[] -> Async<unit>)
    (blocking:int -> unit) : Buffer<'a> * ('a -> bool) =
    let buf = new Buffer<'a> (BufferBound.BlockAfter capacity)
    buf.Consume (batchSize, batchTimeMs, timeIntervalMs, consume) |> Async.Start
    buf.Blocking |> Event.add blocking
    buf, buf.Add
  
  let startDiscardingWithConsumer
    (capacity:int) 
    (batchSize:int) 
    (batchTimeMs:int) 
    (timeIntervalMs:int)
    (consume:'a[] -> Async<unit>)
    (discarding:int -> unit) : Buffer<'a> * ('a -> bool) =
    let buf = new Buffer<'a> (BufferBound.DiscardAfter capacity)
    buf.Consume (batchSize, batchTimeMs, timeIntervalMs, consume) |> Async.Start
    buf.Discarding |> Event.add discarding
    buf, buf.Add