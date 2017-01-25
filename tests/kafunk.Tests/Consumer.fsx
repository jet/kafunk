#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#r "bin/release/kafunk.tests.dll"
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
  let! conn = 
    let connConfig = 
      let chanConfig = 
        ChanConfig.create (
          requestTimeout = TimeSpan.FromSeconds 60.0,
          receiveBufferSize = 8192 * 3)
      KafkaConfig.create ([KafkaUri.parse host], tcpConfig = chanConfig)
    Kafka.connAsync connConfig
  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      initialFetchTime = Time.EarliestOffset, 
      fetchMaxBytes = 200000,
      fetchBufferSize= 2,
      outOfRangeAction = ConsumerOffsetOutOfRangeAction.ResumeConsumerWithFreshInitialFetchTime,
      sessionTimeout = 10000)
  let! consumer = 
    Consumer.createAsync conn consumerConfig
  
  let handle (s:GroupMemberState) (ms:ConsumerMessageSet) = async {
    use! _cnc = Async.OnCancel (fun () -> Log.warn "cancelling_handler")
    Log.trace "consuming_message_set|topic=%s partition=%i count=%i size=%i first_offset=%i last_offset=%i high_watermark_offset=%i lag=%i"
      ms.topic
      ms.partition
      (ms.messageSet.messages.Length)
      (ConsumerMessageSet.size ms)
      (ConsumerMessageSet.firstOffset ms)
      (ConsumerMessageSet.lastOffset ms)
      (ms.highWatermarkOffset)
      (ConsumerMessageSet.lag ms)
    do! Async.Sleep 30000 }

  use counter = Metrics.counter Log 5000

  let handle = 
    handle
    |> Metrics.throughputAsync2To counter (fun (_,ms,_) -> ms.messageSet.messages.Length)

  do! consumer |> Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 10.0) handle
}

Async.RunSynchronously go