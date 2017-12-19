namespace Kafunk

open FSharp.Control
open Kafunk
open System.Threading.Tasks

/// A buffering producer.
type BufferingProducer = private {

  /// The buffer
  buffer : Buffer<ProducerMessage> 
  
  /// Error Handling Event
  errorEvent : IEvent<exn * ProducerMessage seq>
  
  /// Result Handling Event
  resultEvent : IEvent<ProducerResult[]>
  
  /// The flush task.
  flushTask : Task<unit> }

/// Buffer at-capacity action
and ProducerBufferType = 
  
  /// The buffer will block incoming requests until capacity is available.
  | Blocking
  
  /// The buffer will discard incoming requests until capacity is available.
  | Discarding

/// Buffering producer configuration.
and BufferingProducerConfig = {

  /// The type of buffer based on the behavior when buffer is full could be either Discarding or Blocking
  bufferType : ProducerBufferType

  /// The capacity of the buffer
  capacity : int

  /// The batch size when buffer flush to Kafka Producer.
  batchSize : int
  
  /// The maximum wait time of buffer to form a batch before flushing.
  batchTimeMs : int

  /// The maximum wait time between two arrival of two messages before flushing.
  timeIntervalMs : int }

/// Operations on buffering producers.
[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  /// Create a producer with buffer with capacity specified by user. In order to increase throughput, 
  /// the buffer would flush the messages to kafka producer in a batch. 
  /// But users who care about latency can specify the largest time to wait for flushing by setting batchTimeMs. 
  /// Users can also set timeInervalMs to specify the maximum time to wait between two message arrivals.
  let create (producer:Producer) (config:BufferingProducerConfig) = 
    
    let errorHandlingEvent = Event<exn * ProducerMessage seq>()
    let resultHandlingEvent = Event<ProducerResult[]>()
    let produceBatched = Producer.produceBatched producer

    let consumeBuffer (batch:ProducerMessage seq) : Async<unit> = async {
      try
        let! res = produceBatched batch
        resultHandlingEvent.Trigger res
        return ()
      with ex ->
        //Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex producer.config.topic 
        errorHandlingEvent.Trigger (ex,batch) }
    
    let bound =
      match config.bufferType with
      | Blocking -> BufferBound.BlockAfter config.capacity
      | Discarding -> BufferBound.DiscardAfter config.capacity
    
    let buf = new Buffer<ProducerMessage> (bound)

    let flushTask = 
      buf.ToAsyncSeq ()
      |> AsyncSeq.bufferByCountAndTimeAndTimeInterval config.batchSize config.batchTimeMs config.timeIntervalMs
      |> AsyncSeq.iterAsync consumeBuffer
      |> Async.StartAsTask

    { buffer = buf ; errorEvent = errorHandlingEvent.Publish 
      resultEvent = resultHandlingEvent.Publish ; flushTask = flushTask }
  
  /// Get the current size of buffer.
  let bufferSize (producer:BufferingProducer) = 
    producer.buffer.Size
   
  /// Queues a message into the buffer, returning a value indicating whether it was added.
  let produce (producer:BufferingProducer) (m:ProducerMessage) : bool = 
    producer.buffer.Add m

  /// Queues a message into the buffer, returning an int indicating the number of messages 
  /// which were added.
  let produceBatched (producer:BufferingProducer) (ms:ProducerMessage seq) : int =
    let mutable added = 0
    for m in ms do
      if producer.buffer.Add m then
        added <- added + 1
    added
  
  /// Returns an event raised when messages are blocked due to buffer overflow.
  let blocking (producer:BufferingProducer) : IEvent<int> = 
    producer.buffer.Blocking
  
  /// Returns an event raised when messages are discarded due to buffer overflow.
  let discarding (producer:BufferingProducer) : IEvent<int> = 
    producer.buffer.Discarding

  /// Returns the event raised when there is an error producing a batch.
  let errors (producer:BufferingProducer) : IEvent<exn * ProducerMessage seq> = 
    producer.errorEvent

  /// Returns the event raised when a batch of messages is produced.
  let results (producer:BufferingProducer) : IEvent<ProducerResult[]> =
    producer.resultEvent

  /// Returns the task corresponding to the flush process.
  let flushTask (bp:BufferingProducer) : Task<unit> =
    bp.flushTask

  /// Closes the buffer allowing the flushing process to complete.
  let close (bp:BufferingProducer) =
    bp.buffer.Close ()