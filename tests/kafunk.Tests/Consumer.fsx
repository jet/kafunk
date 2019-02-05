﻿#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Collections.Concurrent

//Log.MinLevel <- LogLevel.Trace
let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"
let group = argiDefault 3 "existential-group"
let count = argiDefault 4 "1" |> Int32.Parse

let go = async {
  let! conn = 
    let connConfig = 
      let chanConfig = 
        ChanConfig.create (
          requestTimeout = TimeSpan.FromSeconds 60.0,
          receiveBufferSize = 8192 * 50,
          sendBufferSize = 8192 * 50,
          connectRetryPolicy = ChanConfig.DefaultConnectRetryPolicy,
          requestRetryPolicy = ChanConfig.DefaultRequestRetryPolicy)
      KafkaConfig.create (
        [KafkaUri.parse host], 
        //[KafkaUri.parse "localhost:9092" ; KafkaUri.parse "localhost:9093" ; KafkaUri.parse "localhost:9094"],
        tcpConfig = chanConfig,
        requestRetryPolicy = KafkaConfig.DefaultRequestRetryPolicy,
        version = Versions.V_0_10_1,
        //autoApiVersions = true,
        //version = Versions.V_0_9_0,
        //autoApiVersions = false,
        clientId = "leo")
    Kafka.connAsync connConfig
  let consumerConfig = 
    ConsumerConfig.create (
      groupId = group, 
      topic = topic, 
      autoOffsetReset = AutoOffsetReset.StartFromTime Time.EarliestOffset,
      fetchMaxBytes = 1000000,
      fetchMaxBytesTotal = 50000000,
      fetchMaxBytesOverride = 1000000,
      //fetchMinBytes = 0,
      //fetchMaxWaitMs = 1000,
      fetchBufferSize = 1,
      sessionTimeout = 30000,
      heartbeatFrequency = 3,
      checkCrc = false,
      endOfTopicPollPolicy = RetryPolicy.constantMs 1000
    )
  let! consumer = 
    Consumer.createAsync conn consumerConfig
  
  let showProgress =
    AsyncSeq.intervalMs 10000
    |> AsyncSeq.iterAsync (fun _ -> async {
      let! info = ConsumerInfo.consumerProgress consumer
      let str = 
        info.partitions
        |> Seq.map (fun p -> sprintf "[p=%i o=%i hwo=%i lag=%i lead=%i eo=%i mc=%i]" p.partition p.consumerOffset p.highWatermarkOffset p.lag p.lead p.earliestOffset p.messageCount)
        |> String.concat " ; "
      Log.info "consumer_progress|conn_id=%s topic=%s total_lag=%i min_lead=%i partitions=%s" conn.Config.connId info.topic info.totalLag info.minLead str
      return () })

  let! _ = Async.StartChild showProgress

  let partitionOffsets = new ConcurrentDictionary<Partition, Offset> ()

  let handle (s:ConsumerState) (ms:ConsumerMessageSet) = async {
    
    do! Async.Sleep 5000
    
    if ms.partition = 0 then
      Log.info "consuming_message_set|topic=%s partition=%i count=%i size=%i os=[%i-%i] ts=[%O] hwo=%i lag=%i"
        ms.topic
        ms.partition
        (ms.messageSet.messages.Length)
        (ConsumerMessageSet.size ms)
        (ConsumerMessageSet.firstOffset ms)
        (ConsumerMessageSet.lastOffset ms)
        (ConsumerMessageSet.firstTimestamp ms)
        (ms.highWatermarkOffset)
        (ConsumerMessageSet.lag ms) 

    for msi in ms.messageSet.messages do
      match partitionOffsets.TryGetValue (ms.partition) with
      | true, lastOffset ->
        if (lastOffset + 1L < msi.offset) then
          let gap = msi.offset - (lastOffset + 1L)
          failwithf "non_contig_offsets_detected|partition=%i last_offset=%i current_offset=%i gap=%i" ms.partition lastOffset msi.offset gap
      | _ -> ()
      partitionOffsets.[ms.partition] <- msi.offset

    return () }

  use counter = Metrics.counter Log 5000

  let handle = 
    handle
    |> Metrics.throughputAsync2To counter (fun (_,ms,_) -> ms.messageSet.messages.Length)

  do! Consumer.consumePeriodicCommit consumer (TimeSpan.FromSeconds 10.0) handle
  //do! Consumer.consume consumer handle
  //do! Consumer.stream consumer |> AsyncSeq.iterAsync (fun (s,ms) -> handle s ms)

  Log.info "done_consuming"

}

Seq.init count (fun _ -> go)
|> Async.Parallel
|> Async.RunSynchronously
|> ignore