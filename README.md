# Kafunk [![NuGet Status](http://img.shields.io/nuget/v/kafunk.svg?style=flat)](https://www.nuget.org/packages/kafunk/)

**Kafunk** is a [Kafka](https://kafka.apache.org/) client written in F#.

See [the home page](http://jet.github.io/kafunk) for details.

Please also join the [F# Open Source Group](http://fsharp.github.com)

## Version Support

| Version   | Status   |
| ----------|----------|
| 0.9.0     | Complete |
| 0.10.0    | Complete |
| 0.10.1    | Complete |

## Hello World

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
|> Consumer.consumePeriodicCommit (TimeSpan.FromSeconds 10.0) (fun ms -> async {
  printfn "topic=%s partition=%i" ms.topic ms.partition })
|> Async.RunSynchronously


// commit consumer offsets explicitly

Consumer.commitOffsets consumer [| 0, 1L |]
|> Async.RunSynchronously

// commit consumer offsets explicitly to a relative time

Consumer.commitOffsetsToTime consumer Time.EarliestOffset
|> Async.RunSynchronously


// get current consumer state

let consumerState = 
  Consumer.state consumer
  |> Async.RunSynchronously

printfn "generation_id=%i member_id=%s leader_id=%s partitions=%A" 
  consumerState.generationId consumerState.memberId consumerState.leaderId consumerState.assignments



// fetch offsets of a consumer group for a topic

let consumerOffsets =
  Consumer.fetchOffsetsByTopic conn "consumer-group" "absurd-topic"
  |> Async.RunSynchronously

for (p,o) in consumerOffsets do
  printfn "partition=%i offset=%i" p o



// fetch topic offset information

let offsets = 
  Offsets.offsets conn "absurd-topic" [] [ Time.EarliestOffset ; Time.LatestOffset ] 1
  |> Async.RunSynchronously

for kvp in offsets do
  for (tn,offsets) in kvp.Value.topics do
    for p in offsets do
      printfn "time=%i topic=%s partition=%i offsets=%A" kvp.Key tn p.partition p.offsets
```

## Maintainers

- [@strmpnk](https://github.com/strmpnk)
- [@eulerfx](https://github.com/eulerfx)
