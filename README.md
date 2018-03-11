# Kafunk 
[![NuGet Status](http://img.shields.io/nuget/v/kafunk.svg?style=flat)](https://www.nuget.org/packages/kafunk/)
[![Build status](https://ci.appveyor.com/api/projects/status/j61df5g4tbxrmfg5/branch/master?svg=true)](https://ci.appveyor.com/project/Jet/kafunk/branch/master)
[![Build status](https://travis-ci.org/jet/kafunk.svg?branch=master)](https://travis-ci.org/jet/kafunk)


**Kafunk** is a [Kafka](https://kafka.apache.org/) client written in F#.

See [the home page](http://jet.github.io/kafunk) for details.

Please also join the [F# Open Source Group](http://fsharp.github.com)

## Version Support

| Version    | Status   |
| -----------|----------|
| 0.9.0      | Complete |
| 0.10.0     | Complete |
| 0.10.1     | Complete |
| 0.11+auto  | Protocol |

## Feature Support

| Feature   | Status   |
| ----------|----------|
| GZip      | Complete |
| Snappy    | Complete |
| LZ4       | https://github.com/jet/kafunk/issues/125 |
| TLS       | https://github.com/jet/kafunk/issues/66  |
| SASL      | https://github.com/jet/kafunk/issues/139 |
| ACL       | https://github.com/jet/kafunk/issues/140 |
| TXNS      | https://github.com/jet/kafunk/issues/214 |


## Hello World

```fsharp
#r "kafunk.dll"
#r "FSharp.Control.AsyncSeq.dll"

open Kafunk
open System

let conn = Kafka.connHost "existential-host"

// metadata

let metadata = 
  Kafka.metadata conn (MetadataRequest([|"absurd-topic"|])) 
  |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  for p in t.partitionMetadata do
    printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId


// producer

let producerCfg =
  ProducerConfig.create (
    topic = "absurd-topic", 
    partition = Partitioner.roundRobin, 
    requiredAcks = RequiredAcks.Local)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

// produce single message

let prodRes =
  Producer.produce producer (ProducerMessage.ofBytes ("hello world"B))
  |> Async.RunSynchronously

printfn "partition=%i offset=%i" prodRes.partition prodRes.offset






// consumer

let consumerCfg = 
  ConsumerConfig.create ("consumer-group", "absurd-topic")

let consumer =
  Consumer.create conn consumerCfg

// commit on every message set

Consumer.consume consumer 
  (fun (s:ConsumerState) (ms:ConsumerMessageSet) -> async {
    printfn "member_id=%s topic=%s partition=%i" s.memberId ms.topic ms.partition
    do! Consumer.commitOffsets consumer (ConsumerMessageSet.commitPartitionOffsets ms) })
|> Async.RunSynchronously


// commit periodically


Consumer.consumePeriodicCommit consumer
  (TimeSpan.FromSeconds 10.0) 
  (fun (s:ConsumerState) (ms:ConsumerMessageSet) -> async {
    printfn "member_id=%s topic=%s partition=%i" s.memberId ms.topic ms.partition })
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

printfn "generation_id=%i member_id=%s leader_id=%s assignment_stratgey=%s partitions=%A" 
  consumerState.generationId consumerState.memberId consumerState.leaderId 
  consumerState.assignmentStrategy consumerState.assignments 



// fetch offsets of a consumer group for all topics

let consumerOffsets =
  Consumer.fetchOffsets conn "consumer-group" [||]
  |> Async.RunSynchronously

for (t,os) in consumerOffsets do
  for (p,o) in os do
    printfn "topic=%s partition=%i offset=%i" t p o


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