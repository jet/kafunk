#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "existential-group"

let go = async {
  let! conn = Kafka.connHostAsync host
  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      initialFetchTime = Time.EarliestOffset, 
      fetchMaxBytes = 50000,
      fetchBufferSize= 1,
      outOfRangeAction = ConsumerOffsetOutOfRangeAction.ResumeConsumerWithFreshInitialFetchTime)
  let! consumer = 
    Consumer.createAsync conn consumerConfig
  let handle (ms:ConsumerMessageSet) = async {
    Log.info "consuming_message_set|topic=%s partition=%i count=%i size=%i first_offset=%i last_offset=%i high_watermark_offset=%i lag=%i"
      ms.topic
      ms.partition
      (ms.messageSet.messages.Length)
      (ConsumerMessageSet.size ms)
      (ConsumerMessageSet.firstOffset ms)
      (ConsumerMessageSet.lastOffset ms)
      (ms.highWatermarkOffset)
      (ConsumerMessageSet.lag ms) }
  do! consumer |> Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 10.0) handle
}

Async.RunSynchronously go