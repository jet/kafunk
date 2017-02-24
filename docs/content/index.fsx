(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../src/kafunk/bin/Release"

(**
Kafunk - F# Kafka client
======================

Example
-------

This example demonstrates a few uses of the Kafka client.

*)

#r "kafunk.dll"
#r "FSharp.Control.AsyncSeq.dll"

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


(**
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Apache 2.0. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/jet/kafunk/docs/content
  [gh]: https://github.com/jet/kafunk
  [issues]: https://github.com/jet/kafunk/issues
  [readme]: https://github.com/jet/kafunk/project/README.md 
  [license]: https://github.com/jet/kafunk/project/LICENSE.txt
  
*)
