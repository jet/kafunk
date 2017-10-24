namespace Kafunk

open FSharp.Control
open Kafunk

type BufferingProducer  = private {

  /// The producer
  producer : Producer

  /// The buffer
  buffer : Buffer<ProducerMessage> }

and BufferType = 
    | Blocking
    | Discarding

[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  /// Create a producer with buffer with capacity specified by user. In order to increase throughput, the buffer would flush the messages to kafka producer in a batch. 
  /// But users who care about latency can specify the largest time to wait for flushing by setting batchTimeMs. 
  /// Users can also set timeInervalMs to specify the maximum time to wait between two message arrivals.
  let createBufferingProducer (producer:Producer) (buffertype:BufferType) (capacity:int) (batchSize:int) (batchTimeMs:int) (timeIntervalMs:int) (errorHandle:(ProducerMessage seq -> unit) option)= 

    let consume (producer: Producer) (batch: ProducerMessage seq): Async<unit> = async {
      try
        do! Producer.produceBatched producer batch |> Async.Ignore
        return ()
      with ex ->
        Log.error "buffering_producer_process_error|error=\"%O\" topic=%s" ex producer.config.topic
        match errorHandle with
        | Some x -> x batch
        | None -> () }
    
    let buf = 
      match buffertype with
      | Blocking -> 
        let tmp = new Buffer<'a> (BufferBound.BlockAfter capacity)
        tmp.Blocking |> Event.add (fun x -> Log.warn "producer_buffer_full_warning|cap=%O size=%O" capacity x)
        tmp
      | Discarding -> 
        let tmp = new Buffer<'a> (BufferBound.DiscardAfter capacity)
        tmp.Discarding |> Event.add (fun x -> Log.warn "producer_buffer_full_warning|cap=%O size=%O" capacity x)
        tmp

    buf.Consume (batchSize, batchTimeMs, timeIntervalMs, (consume producer)) |> Async.Start

    { producer = producer; buffer = buf }
  
  ///Get the current size of buffer
  let getSize (producer:BufferingProducer) = 
    producer.buffer.Size
   
  ///Buffering the message
  let produce (producer:BufferingProducer) (message : ProducerMessage) = 
    producer.buffer.Add message