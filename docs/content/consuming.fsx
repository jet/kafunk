(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../src/kafunk/bin/Release"

(**
Consuming
======================

This example demonstrates how to consume using Kafunk.
*)


(**

Joining the consumer group:
See also: [ConsumerConfig](https://jet.github.io/kafunk/reference/kafunk-consumerconfig.html)

*)


#r "kafunk.dll"
#r "FSharp.Control.AsyncSeq.dll"

open FSharp.Control
open Kafunk
open System

let conn = Kafka.connHost "existential-host"

/// Configuration.
let consumerConfig = 
  ConsumerConfig.create (

    /// The name of the consumer group.
    groupId = "consumer-group", 

    /// The topic to consume.
    topic = "absurd-topic")


/// This creates a consumer and joins it to the group.
let consumer =
  Consumer.create conn consumerConfig



(**

## Consumer State

Consumer state consists of consumer group member state, as well as state particular to the consumer group protocol, such
as the set of partitions assigned to the consumer. The state can be retrieved, but note that state changes when group
membership changes.

*)

let consumerState = 
  Consumer.state consumer
  |> Async.RunSynchronously

printfn "generation_id=%i member_id=%s leader_id=%s assignment_stratgey=%s partitions=%A" 
  consumerState.generationId consumerState.memberId consumerState.leaderId 
  consumerState.assignmentStrategy consumerState.assignments 



(**

## Consuming

Consume with commit on every message set. In this case, offsets are committed as soon as
a message set is processed. Note that this may result in needless synchronization - since the
strongest delivery model supported is at-least-once, consumers have to be tolerant to
receiving duplicate messages. Therefore, it is acceptable to commit offsets asynchronously.

*)



Consumer.consume consumer 
  (fun (s:ConsumerState) (ms:ConsumerMessageSet) -> async {
    printfn "member_id=%s assignment_strategy=%s topic=%s partition=%i" 
      s.memberId s.protocolName ms.topic ms.partition
    do! Consumer.commitOffsets consumer (ConsumerMessageSet.commitPartitionOffsets ms) })
|> Async.RunSynchronously



(**

Consume with periodic commit. This commits offsets asynchronously thereby eliminating a synchronization in
the critical path.

*)


Consumer.consumePeriodicCommit consumer
  (TimeSpan.FromSeconds 10.0) 
  (fun (s:ConsumerState) (ms:ConsumerMessageSet) -> async {
    printfn "member_id=%s assignment_strategy=%s topic=%s partition=%i" 
      s.memberId s.protocolName ms.topic ms.partition })
|> Async.RunSynchronously



(**

## Periodic Offset Commit

`Consumer.consumePeriodicCommit` commits offsets periodically using the following mechanism.

*)


/// Create a commit queue.
let (commitQueue,commitProccess) = 
  Consumer.periodicOffsetCommitter consumer (TimeSpan.FromSeconds 60.0)
  |> Async.RunSynchronously

/// Start the background commit process.
/// You may wish to trap exceptions in this process in order to crash.
Async.Start commitProccess

let ms : ConsumerMessageSet = failwith "some message set"

/// Asynchronously enqueue offsets to be committed.
Offsets.enqueuePeriodicCommit commitQueue (ConsumerMessageSet.commitPartitionOffsets ms)

(**

The commit queue will commit enqueued offsets periodically. It will commit the greatest offset enqueued by partition.

*)




(**

## Streaming

In order to have explicit control of buffering and parallelism, or to perform other streaming operations, it is possible
to consume messages directly using an asynchronous sequence:

*)



Consumer.stream consumer
|> AsyncSeq.iterAsync (fun (s,ms) -> async {
  printfn "member_id=%s assignment_strategy=%s topic=%s partition=%i" 
    s.memberId s.protocolName ms.topic ms.partition })


(**

The resulting stream merges all of the per-broker streams covering all assigned partitions of the consumed topic. Note that offsets 
must be committed explicitly as described earlier.

*)





(**

## Consumer Offsets

Consumer offsets can be committed explicitly. This can be used to reset a consumer to a particular offset when
the consumer instances are offline. Note that consumer instances only fetch committed offsets when they are starting
to consumer, or when rejoining.

*)


Consumer.commitOffsets consumer [| 0, 1L |]
|> Async.RunSynchronously


Consumer.commitOffsetsToTime consumer Time.EarliestOffset
|> Async.RunSynchronously



(**

Fetch committed consumer offsets:

*)

let consumerOffsets =
  Consumer.fetchOffsets conn "consumer-group" [||]
  |> Async.RunSynchronously

for (t,os) in consumerOffsets do
  for (p,o) in os do
    printfn "topic=%s partition=%i offset=%i" t p o



(**

## Consuming Fixed Ranges

Kafunk provides a helper to consumer a fixed range of offsets. For example, to read the range of
messages in the topic at the time of invocation:

*)

/// Read the current offset range.
let offsetRange = 
  Offsets.offsetRange conn "my-topic" [] 
  |> Async.RunSynchronously

/// Read messages corresponding to the offset range.
let messageRange = 
  Consumer.streamRange consumer offsetRange
  |> Async.RunSynchronously




(**

## Rolling Updates

Kafka supports rolling updates of consumer group member instances. This can be done by having a newever version of
a consumer support both new and old versions of consumer group assignment strategies. The assignment strategy itself can
change, but the code path invoked by the consumer can also change. The Kafka group coordinator ensures that all members
of the group support the same protocol version. Once new versions have been deployed, all consumer instances will support
the new version and Kafka will select the first version in the list.

*)

/// This configuration specifies two versions of an assignment strategy.
let consumerConfigVersioned = 
  ConsumerConfig.create (
    groupId = "consumer-group", 
    topic = "absurd-topic",
    assignmentStrategies = [|
        "range/v2", ConsumerGroup.AssignmentStratgies.Range
        "range/v1", ConsumerGroup.AssignmentStratgies.Range |])

/// The handler accepts a consumer group member state object, which contains
/// the selected protocol version. This can be used to invoke different code paths.
let handle (s:GroupMemberState) (ms:ConsumerMessageSet) = async {
  match s.protocolName with
  | "range/v2" ->
    printfn "running version 2"
  
  | "range/v1" ->
    printfn "running version 1"

  | v -> failwithf "unknown protocol_version=%s" v }




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
