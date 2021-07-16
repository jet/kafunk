NOTICE: SUPPORT FOR THIS PROJECT ENDED ON 18 November 2020

This projected was owned and maintained by Jet.com (Walmart). This project has reached its end of life and Walmart no longer supports this project.

We will no longer be monitoring the issues for this project or reviewing pull requests. You are free to continue using this project under the license terms or forks of this project at your own risk. This project is no longer subject to Jet.com/Walmart's bug bounty program or other security monitoring.


## Actions you can take

We recommend you take the following action:

  * Review any configuration files used for build automation and make appropriate updates to remove or replace this project
  * Notify other members of your team and/or organization of this change
  * Notify your security team to help you evaluate alternative options

## Forking and transition of ownership

For [security reasons](https://www.theregister.co.uk/2018/11/26/npm_repo_bitcoin_stealer/), Walmart does not transfer the ownership of our primary repos on Github or other platforms to other individuals/organizations. Further, we do not transfer ownership of packages for public package management systems.

If you would like to fork this package and continue development, you should choose a new name for the project and create your own packages, build automation, etc.

Please review the licensing terms of this project, which continue to be in effect even after decommission.

ORIGINAL README BELOW

----------------------

# UPDATE

We found a bug in the implementation of the v0.11+ protocol wherein messages were skipped during consumption. The bug only manifests when using the newer protocol version (the default). Due to this bug and for long term maintenance, we've started investing into the Confluent.Kafka client: https://github.com/jet/confluent-kafka-fsharp.

# Kafunk 
[![NuGet Status](http://img.shields.io/nuget/v/kafunk.svg?style=flat)](https://www.nuget.org/packages/kafunk/)
[![Build status](https://ci.appveyor.com/api/projects/status/j61df5g4tbxrmfg5/branch/master?svg=true)](https://ci.appveyor.com/project/Jet/kafunk/branch/master)
[![Build status](https://travis-ci.org/jet/kafunk.svg?branch=master)](https://travis-ci.org/jet/kafunk)

**Kafunk** is a [Kafka](https://kafka.apache.org/) client written in F#.


See [the home page](http://jet.github.io/kafunk) for details.

Please also join the [F# Open Source Group](http://fsharp.github.com)

## Version Support

| Version    | Status   | Notes |
| -----------|----------|-------|
| 0.9.0      | Complete |
| 0.10.0     | Complete |
| 0.10.1     | Complete |
| 0.11+auto  | Protocol | Protocol implementation bug found (skipped messages) |

## Feature Support

| Feature   | Status   |
| ----------|----------|
| GZip      | Complete |
| Snappy    | Complete |
| LZ4       | Complete |
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

- [@eulerfx](https://github.com/eulerfx)
