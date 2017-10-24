namespace Kafunk

open FSharp.Control
open Kafunk

type BufferingProducer  = private {

  ///The producer
  producer : Producer

  ///The buffer
  buffer : Buffer<ProducerMessage>
  
  ///The function that consume the buffer and send to Kafka
  consume : ProducerMessage -> bool }

and BufferType = 
    | Blocking
    | Discarding

[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  ///Create a producer with buffer with capacity specified by user. In order to increase throughput, the buffer would flush the messages to kafka producer in a batch. 
  ///But user who care about latency can specify the largest time to wait for flushing by setting batchTimeMs. 
  ///The user can also set timeInervalMs to specify the maximum time to wait between two message arrivals.
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
    
    let buf, consume =
      match buffertype with
      | Blocking -> Buffer.startBlockingWithConsumer capacity batchSize batchTimeMs timeIntervalMs (consume producer) (fun x -> Log.warn "buffering_producer_process_overflow_warning|cap=%O size=%O" capacity x)
      | Discarding -> Buffer.startDiscardingWithConsumer capacity batchSize batchTimeMs timeIntervalMs (consume producer) (fun x -> Log.warn "buffering_producer_process_overflow_warning|cap=%O size=%O" capacity x)

    { producer = producer; buffer = buf; consume = consume }
  
  ///Get the current size of buffer
  let getSize (producer:BufferingProducer) = 
    Buffer.getSize (producer.buffer)
   
  ///Buffering the message
  let produce (producer:BufferingProducer) (message : ProducerMessage) = 
    producer.consume message