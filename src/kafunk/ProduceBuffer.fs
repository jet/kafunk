namespace Kafunk

open FSharp.Control
open Kafunk

type BufferingProducer  = private {

  /// The producer
  producer : Producer

  /// The buffer
  buffer : Buffer<ProducerMessage> 
  
  /// Error Handling Event
  errorHandling : IEvent<ProducerMessage seq>
  
  /// Result Handling Event
  resultHandling : IEvent<ProducerResult[]> }

and BufferType = 
    | Blocking
    | Discarding

and BufferConfig = {

  /// The type of buffer based on the behavior when buffer is full could be either Discarding or Blocking
  buftype : BufferType

  /// The capacity of the buffer
  capacity : int

  /// The batch size when buffer flush to Kafka Producer
  batchSize : int
  
  /// The maximum wait time of buffer to form a batch
  batchTimeMs : int

  /// The maximum wait time between two arrival of two messages
  timeIntervalMs : int }

[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  /// Create a producer with buffer with capacity specified by user. In order to increase throughput, the buffer would flush the messages to kafka producer in a batch. 
  /// But users who care about latency can specify the largest time to wait for flushing by setting batchTimeMs. 
  /// Users can also set timeInervalMs to specify the maximum time to wait between two message arrivals.
  let create (producer:Producer) (config:BufferConfig) = 
    let errorHandlingEvent = Event<ProducerMessage seq>()
    let resultHandlingEvent = Event<ProducerResult[]>()

    let consume (producer: Producer) (batch: ProducerMessage seq): Async<unit> = async {
      try
        let! res = Producer.produceBatched producer batch
        return resultHandlingEvent.Trigger res
      with ex ->
        Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex producer.config.topic 
        errorHandlingEvent.Trigger batch}
    
    let buf = 
      match config.buftype with
      | Blocking -> 
          new Buffer<ProducerMessage> (BufferBound.BlockAfter config.capacity)
      | Discarding -> 
          new Buffer<ProducerMessage> (BufferBound.DiscardAfter config.capacity)

    buf.Consume (config.batchSize, config.batchTimeMs, config.timeIntervalMs, (consume producer)) |> Async.Start

    { producer = producer; buffer = buf; errorHandling = errorHandlingEvent.Publish; resultHandling = resultHandlingEvent.Publish }
  
  /// Get the current size of buffer
  let getSize (producer:BufferingProducer) = 
    producer.buffer.Size
   
  /// Buffering the message
  let produce (producer:BufferingProducer) = 
    producer.buffer.Add
  
  /// Subscribe blocking events, the input of handler should be the size of buffer
  let subscribeBlocking (producer:BufferingProducer) (handle:int -> unit) = 
    producer.buffer.Blocking |> Event.add handle
  
  /// Subscribe discarding events, the input of handler should be the size of buffer
  let subscribeDiscarding (producer:BufferingProducer) (handle:int -> unit) = 
    producer.buffer.Discarding |> Event.add handle    

  /// Subscribe error events, the input of handler should a sequence of message of failed batch
  let subscribeError (producer:BufferingProducer) (handle:ProducerMessage seq -> unit) = 
    producer.errorHandling |> Event.add handle

  /// Subscribe produce result, the input of handler should be the ProducerResult of a batch
  let subsribeProduceResult (producer:BufferingProducer) (handle:ProducerResult[] -> unit) = 
    producer.resultHandling |> Event.add handle