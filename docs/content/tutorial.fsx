(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin/kafunk/"

(**
Kafunk - F# Kafka client
======================

Set up Zookeeper and Kafka
--------------------------

The [Apache Kafka documentation](http://kafka.apache.org/documentation.html) contains an excellent [Quick Start guide](http://kafka.apache.org/documentation.html#quickstart) to set up Kafka.
For this tutorial we will use the default parameters for Zookeeper and Kafka which means we will connect against localhost and port 9092.

Connecting to Kafka and retrieving meta data
--------------------------------------------

This example demonstrates a few uses Kafunk. 
We start by referencing the Kafunk F# Kafka client and  ceate a connection to a host:
*)

#r "kafunk.dll"
open Kafunk

let conn = Kafka.connHost "localhost"
// [fsi:val conn : KafkaConn]

let topicName = "absurd-topic"

// Let's connect to a specific topic in kafka
let metadata = 
    Kafka.metadata conn (Metadata.Request [|topicName|])
    |> Async.RunSynchronously

// Let's see what brokers are running
for b in metadata.brokers do
    printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

// There is more metadata available
for t in metadata.topicMetadata do
    printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
    for p in t.partitionMetadata do
        printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId

(**

Creating a producer
-------------------

Producers allow to publish data to topics. So let's create a producer for our topic and post a message:

*)

let producerCfg = 
    ProducerCfg.create ([|topicName|], Partitioner.konst 0, requiredAcks=RequiredAcks.Local)
// [fsi:val producerCfg : ProducerCfg = {topics = [|"absurd-topic"|];]
// [fsi:                                 requiredAcks = 1s;]
// [fsi:                                 compression = 0uy;]
// [fsi:                                 timeout = 0;]
// [fsi:                                 partition = <fun:konst@34>;}]

let producer = 
    Producer.createAsync conn producerCfg 
    |> Async.RunSynchronously
// [fsi:val producer : Producer]

let prodRes =
    Producer.produceSingle producer (topicName, [| ProducerMessage.ofBytes ("hello world"B) |])
    |> Async.RunSynchronously

(**

Now we look at the metadata again:

*)

for tn,offsets in prodRes.topics do
    printfn "topic_name=%s" tn
    for p,ec,offset in offsets do
        printfn "partition=%i error_code=%i offset=%i" p ec offset


let fetchRes = 
    Kafka.fetch conn (FetchRequest.ofTopicPartition topicName 0 0L 0 0 1000) 
    |> Async.RunSynchronously

for tn,pmds in fetchRes.topics do
    for p,ec,hmo,mss,ms in pmds do
        printfn "topic=%s partition=%i error=%i" tn p ec

(**

Creating a consumer
-------------------

Consumers allow us to subscribe on topics via a consumer group

*)

let consumerGroup = "absurd-consumers"

let consumerCfg = 
    Consumer.ConsumerConfig.create (consumerGroup, [| topicName |])
// [fsi:val consumerCfg : Consumer.ConsumerConfig =]
// [fsi:  {groupId = "absurd-consumers";]
// [fsi:   topics = [|"absurd-topic"|];]
// [fsi:   sessionTimeout = 10000;]
// [fsi:   heartbeatFrequency = 4;]
// [fsi:   autoOffsetReset = Anything;]
// [fsi:   fetchMinBytes = 0;]
// [fsi:   fetchMaxWaitMs = 0;]
// [fsi:   metadataFetchTimeoutMs = 0;]
// [fsi:   totalBufferMemory = 10000;]
// [fsi:   fetchBuffer = 1000;]
// [fsi:   clientId = "283b41ed6807454d9d0edfef10f8ce0d";]
// [fsi:   socketReceiveBuffer = 1000;]
// [fsi:   reconnectBackoffMs = 0;]
// [fsi:   offsetRetentionTime = 0L;}]

Consumer.consume conn consumerCfg
|> AsyncSeq.iterAsync (fun (generationId,memberId,topics) ->
    // the outer AsyncSeq yield on every generation of the consumer groups protocol
    topics
    |> Array.map (fun (topic,partition,stream) ->
        // the inner AsyncSeqs correspond to individual topic-partitions
        stream
        |> AsyncSeq.iterAsync (fun (ms,commit) -> async {
            for (offset,_,msg) in ms.messages do
                printfn "processing topic=%s partition=%i offset=%i key=%s" topic partition offset (Message.keyString msg)
            do! commit }))
    |> Async.Parallel
    |> Async.Ignore)
|> Async.RunSynchronously
