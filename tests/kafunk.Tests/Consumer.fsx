#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk
open System

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "test-topic_1019"
let group = argiDefault 3 "leo_test16"

let go = async {
  let! conn = Kafka.connHostAsync host
  let consumerCfg = 
    ConsumerConfig.create (group, topic, 
      initialFetchTime=Time.EarliestOffset, 
      fetchMaxBytes=10000,
      fetchBufferSize=1,
      outOfRangeAction=ConsumerOffsetOutOfRangeAction.ResumeConsumerWithFreshInitialFetchTime)
  let! consumer = 
    Consumer.createAsync conn consumerCfg
  let handle (ms:ConsumerMessageSet) = async {
    Log.info "consuming_message_set|topic=%s partition=%i count=%i size=%i last_offset=%i high_watermark_offset=%i"
      ms.topic
      ms.partition
      (ms.messageSet.messages.Length)
      (ConsumerMessageSet.size ms)
      (ConsumerMessageSet.lastOffset ms)
      (ms.highWatermarkOffset) }
  do! consumer |> Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 30) handle
}

Async.RunSynchronously go