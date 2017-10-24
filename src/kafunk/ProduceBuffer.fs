namespace Kafunk

open FSharp.Control
open Kafunk

type BufferingProducer  = private {

  /// The producer
  producer : Producer

  /// The buffer
  buffer : Buffer<ProducerMessage> 
  
  /// Error Handling Event
  errorHandling : IEvent<ProducerMessage[]>
  
  /// Result Handling Event
  resultHandling : IEvent<bool> }

and BufferType = 
    | Blocking
    | Discarding

[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  /// Create a producer with buffer with capacity specified by user. In order to increase throughput, the buffer would flush the messages to kafka producer in a batch. 
  /// But users who care about latency can specify the largest time to wait for flushing by setting batchTimeMs. 
  /// Users can also set timeInervalMs to specify the maximum time to wait between two message arrivals.
  let createBufferingProducer (producer:Producer) (buffertype:BufferType) (capacity:int) (batchSize:int) (batchTimeMs:int) (timeIntervalMs:int) = 

    let consume (producer: Producer) (batch: ProducerMessage seq): Async<unit> = async {
      try
        do! Producer.produceBatched producer batch |> Async.Ignore
        return ()
      with ex ->
        Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex producer.config.topic }
    
    let buf = 
      match buffertype with
      | Blocking -> 
          new Buffer<'a> (BufferBound.BlockAfter capacity)
      | Discarding -> 
          new Buffer<'a> (BufferBound.DiscardAfter capacity)

    buf.Consume (batchSize, batchTimeMs, timeIntervalMs, (consume producer)) |> Async.Start

    let errorHandlingEvent = Event<ProducerMessage[]>()
    let resultHandlingEvent = Event<bool>()

    { producer = producer; buffer = buf; errorHandling = errorHandlingEvent.Publish; resultHandling = resultHandlingEvent.Publish }
  
  /// Get the current size of buffer
  let getSize (producer:BufferingProducer) = 
    producer.buffer.Size
   
  /// Buffering the message
  let produce (producer:BufferingProducer) = 
    producer.buffer.Add
  
  /// Subscribe blocking events
  let subscribeBlocking (producer:BufferingProducer) (handle:int -> unit) = 
    producer.buffer.Blocking |> Event.add handle
  
  /// Subscribe discarding events
  let subscribeDiscarding (producer:BufferingProducer) (handle:int -> unit) = 
    producer.buffer.Discarding |> Event.add handle    

  /// Subscribe error events
  let subscribeError (producer:BufferingProducer) (handle:ProducerMessage seq -> unit) = 
    producer.errorHandling |> Event.add handle

  /// Subscribe produce result
  let subsribeProduceResult (producer:BufferingProducer) (handle:bool -> unit) = 
    producer.resultHandling |> Event.add handle