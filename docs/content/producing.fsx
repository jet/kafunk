(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../src/kafunk/bin/Release"

(**
Producing
======================

This example demonstrates how to produce using Kafunk.
*)


(**

Creating the producer. 
See also: (ProducerConfig)[https://jet.github.io/kafunk/reference/kafunk-producerconfig.html]

*)


#r "kafunk.dll"
#r "FSharp.Control.AsyncSeq.dll"

open Kafunk
open System

let conn = Kafka.connHost "existential-host"

/// Configuration.
let producerConfig = 
  ProducerConfig.create (
    
    /// The topic to produce to.
    topic = "absurd-topic", 

    /// The partition function to use.
    partition = Partitioner.roundRobin,

    /// The required acks setting.
    requiredAcks = RequiredAcks.AllInSync,

    /// The per-broker in-memory buffer size, in bytes.
    bufferSizeBytes = ProducerConfig.DefaultBufferSizeBytes,

    /// The maximum size, in bytes, of an individual produce request.
    batchSizeBytes = ProducerConfig.DefaultBatchSizeBytes,
    
    /// The maximum time to wait for a batch.
    batchLingerMs = ProducerConfig.DefaultBatchLingerMs)


/// Create a producer.
let producer = 
  Producer.create conn producerConfig


/// Create a message.
let m = 
  ProducerMessage.ofString (
    value = "hello value", 
    key = "hello key")


/// Produce a single message.
let prodRes = 
  Producer.produce producer m 
  |> Async.RunSynchronously

printfn "partition=%i offset=%i" prodRes.partition prodRes.offset

(**

## Buffering

The function `Producer.produce` accepts a single message, but the producer batches messages internally by partition and broker based on
batching settings specified in the configuration. Bigger batches mean fewer round-trips to the broker, and therefore, greater throughput 
at the cost of increased latency.

The batching workflow is as follows. A message is assigned to a partition using the configured partition function. (Note that this operation
depends on cluster state and may change, albeit infrequently). Then, a message is placed into the queue of the broker currently responsible for
the partition along with a reply channel. An independent process consumes the broker queue, buffering to form batches and then sends the 
batched produce request to the broker. Once a response is received, all of the outstanding reply channels are acknowledged. The offsets in the
`ProducerResponse` correspond to the first offsets for the entire batch.

In this way, batching allows many concurrent produce operations to be invoked independently, while keeping the number of network operations low.
Note that care must be taken to ensure message ordering requirements aren't violated.

*)



(**

## Explicit Batching

It is also possible to explicitly batch messages on the client side:

*)

let producerResults =
  Producer.produceBatched 
    producer 
    [| ProducerMessage.ofString ("message1") ; ProducerMessage.ofString ("message2") |]
  |> Async.RunSynchronously


(**

The `Producer.produceBatched` takes a collection of messages, groups them by partition and produces in parallel 
across partitions but maintaining the input order within partitions. The operation returns an array of ProducerResult
values, one for each partition produced to. The producer result value contains the partition, the offset of the first
message written to the partition and a count of messages written to that partition as part of the batch. Note that this
count doesn't necessarily correspond to the count of messages provided to the operation due to buffering. See above for more
details.

*)



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
