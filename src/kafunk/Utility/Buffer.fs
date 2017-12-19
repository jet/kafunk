[<AutoOpen>]
module internal Kafunk.Buffer

open System.Collections.Concurrent
open FSharp.Control
open System

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
 
  /// Returns an async sequence consuming messages from the buffer.
  member __.ToAsyncSeq () =
    queue.GetConsumingEnumerable ()
    |> AsyncSeq.ofSeq
    
  /// Closes the buffer, allowing the consumer to complete.
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