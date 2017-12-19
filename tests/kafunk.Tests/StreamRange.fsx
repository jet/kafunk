#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System

//Log.MinLevel <- LogLevel.Trace
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
          requestTimeout = TimeSpan.FromSeconds 30.0,
          receiveBufferSize = 8192 * 50,
          sendBufferSize = 8192 * 50,
          connectRetryPolicy = ChanConfig.DefaultConnectRetryPolicy,
          requestRetryPolicy = ChanConfig.DefaultRequestRetryPolicy)
      KafkaConfig.create (
        [KafkaUri.parse host], 
        tcpConfig = chanConfig,
        requestRetryPolicy = KafkaConfig.DefaultRequestRetryPolicy,
        version = Versions.V_0_10_1,
        autoApiVersions = false)
    Kafka.connAsync connConfig
  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      autoOffsetReset = AutoOffsetReset.StartFromTime Time.EarliestOffset,
      fetchMaxBytes = 500000,
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
  
  let! range = Offsets.offsetRange conn topic []
  Log.info "offset_range|range=[%s]" 
    (range |> Map.toSeq |> Seq.map (fun (p,(e,l)) -> sprintf "[p=%i e=%i l=%i]" p e l) |> String.concat " ; ")

  let! mss = Consumer.streamRange consumer range

  for ms in mss do
    for m in ms.messageSet.messages do
      Log.info "p=%i key=%s" 
        ms.partition 
        (Binary.toString m.message.key)

  Log.info "done"

}

try Async.RunSynchronously go
with ex -> Log.error "error=%O" ex
