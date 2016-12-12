# Kafunk - F# Kafka client

**Kafunk** is a Kafka client written in F#.

See [the home page](http://jet.github.io/kafunk) for details.

Please also join the [F# Open Source Group](http://fsharp.github.com)

# Status

This is a work in progress and not yet published as a packge. See the issue list for the remaining items. At the point, the public API and internals may undergo several changes before stabilizing. Still, contributions are welcome!

| Feature         | Status   |
| ----------------|----------|
| Wire Protocol   | Complete |
| Base API        | Complete |
| Compression     | GZip     |
| Routing         | Complete |
| Consumer Groups | Complete |
| Producers       | Complete |
| v0.9            | Complete |
| v0.10           | Partial  |

# Hello World

```fsharp
open Kafunk


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

for (tn,offsets) in prodRes.topics do
  printfn "topic_name=%s" tn
  for (p,ec,offset) in offsets do
    printfn "partition=%i error_code=%i offset=%i" p ec offset


// consumer

let consumerCfg = 
  ConsumerConfig.create ("consumer-group", [|"absurd-topic"|])

let consumer =
  Consumer.create conn consumerCfg

consumer
|> Consumer.consume (fun tn p ms commit -> async {
  printfn "topic=%s partition=%i" tn p
  do! commit })
|> Async.RunSynchronously



// consumer offsets

Consumer.commitOffsets consumer [| "absurd-topic", [| 0, 1L |] |]
|> Async.RunSynchronously



// offsets

let offsets = 
  Kafka.Composite.offsets conn "absurd-topic" [ Time.EarliestOffset ; Time.LatestOffset ] 1
  |> Async.RunSynchronously

for kvp in offsets do
  for (tn,offsets) in kvp.Value.topics do
    for p in offsets do
      printfn "time=%i topic=%s partition=%i offsets=%A" kvp.Key tn p.partition p.offsets
```

# Maintainer(s)

- [@strmpnk](https://github.com/strmpnk)
- [@eulerfx](https://github.com/eulerfx)
