# Kafunk [![NuGet Status](http://img.shields.io/nuget/v/kafunk.svg?style=flat)](https://www.nuget.org/packages/kafunk/)

**Kafunk** is a [Kafka](https://kafka.apache.org/) client written in F#.

See [the home page](http://jet.github.io/kafunk) for details.

Please also join the [F# Open Source Group](http://fsharp.github.com)

# Hello World

```fsharp
open Kafunk
open System

let conn = Kafka.connHost "existential-host"

// metadata

let metadata = 
  Kafka.metadata conn (Metadata.Request([|"absurd-topic"|])) 
  |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  for p in t.partitionMetadata do
    printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId


// producer

let producerCfg =
  ProducerConfig.create ("absurd-topic", Partitioner.roundRobin, requiredAcks = RequiredAcks.Local)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let prodRes =
  Producer.produce producer [| ProducerMessage.ofBytes ("hello world"B) |]
  |> Async.RunSynchronously

for (p,offset) in prodRes.offsets do
  printfn "partition=%i offset=%i" p offset


// consumer

let consumerCfg = 
  ConsumerConfig.create ("consumer-group", "absurd-topic")

let consumer =
  Consumer.create conn consumerCfg

// commit on every message set

consumer
|> Consumer.consume (fun ms -> async {
  printfn "topic=%s partition=%i" ms.topic ms.partition
  do! Consumer.commitOffsets consumer (ConsumerMessageSet.commitPartitionOffsets ms) })
|> Async.RunSynchronously


// commit periodically

consumer
|> Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 10) (fun ms -> async {
  printfn "topic=%s partition=%i" ms.topic ms.partition })
|> Async.RunSynchronously



// consumer commit offsets

Consumer.commitOffsets consumer [| 0, 1L |]
|> Async.RunSynchronously



// offset information

let offsets = 
  Offsets.offsets conn "absurd-topic" [] [ Time.EarliestOffset ; Time.LatestOffset ] 1
  |> Async.RunSynchronously

for kvp in offsets do
  for (tn,offsets) in kvp.Value.topics do
    for p in offsets do
      printfn "time=%i topic=%s partition=%i offsets=%A" kvp.Key tn p.partition p.offsets
```

# Maintainer(s)

- [@strmpnk](https://github.com/strmpnk)
- [@eulerfx](https://github.com/eulerfx)
