namespace Kafunk

open FSharp.Control
open Kafunk

type BufferingProducer  = private {

  ///The producer
  producer : Producer

  ///The buffer
  buffer : Buffer<ProducerMessage>
  
  consume : ProducerMessage -> bool }

and BufferType = 
    | Blocking
    | Discarding

[<Compile(Module)>]
module BufferingProducer = 

  let private Log = Log.create "Kafunk.BufferingProducer"
  
  ///Create a producer with buffer
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
  
  let getSize (producer:BufferingProducer) = 
    Buffer.getSize (producer.buffer)

  let produce (producer:BufferingProducer) (message : ProducerMessage) = 
    producer.consume message