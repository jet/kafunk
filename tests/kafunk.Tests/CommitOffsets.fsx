#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "absurd-topic"

let connCfg = KafkaConfig.create ([KafkaUri.parse host])
let conn = Kafka.conn connCfg

let go = async {

  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      autoOffsetReset = AutoOffsetReset.StartFromTime Time.EarliestOffset,
      fetchMaxBytes = 50000,
      fetchMinBytes = 1,
      fetchMaxWaitMs = 1000,
      fetchBufferSize = 1,
      sessionTimeout = 30000,
      heartbeatFrequency = 3,
      checkCrc = true,
      endOfTopicPollPolicy = RetryPolicy.constantMs 1000
    )
  let! consumer = 
    Consumer.createAsync conn consumerConfig

  do! Consumer.commitOffsetsToTime consumer Time.EarliestOffset


}

Async.RunSynchronously go