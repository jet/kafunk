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
| Routing         | Partial  |
| Consumer Groups | Partial  |
| Producers       | Partial  |
| Fault Tolerance | Partial  |
| v0.9            | Partial  |
| v0.10           | Partial  |

# Hello World

```fsharp

open Kafunk

let conn = Kafka.connHost "existentialhost"


let metadata = Kafka.metadata conn (Metadata.Request([|"absurd-topic"|])) |> Async.RunSynchronously

for b in metadata.brokers do
  printfn "broker|host=%s port=%i nodeId=%i" b.host b.port b.nodeId

for t in metadata.topicMetadata do
  printfn "topic|topic_name=%s topic_error_code=%i" t.topicName t.topicErrorCode
  for p in t.partitionMetadata do
    printfn "topic|topic_name=%s|partition|partition_id=%i" t.topicName p.partitionId



let producerCfg = 
  ProducerCfg.create ([|"absurd-topic"|], Partitioner.konst 0, requiredAcks=RequiredAcks.Local)

let producer = 
  Producer.createAsync conn producerCfg 
  |> Async.RunSynchronously

let prodRes =
  Producer.produceSingle producer ("absurd-topic", [| ProducerMessage.ofBytes ("hello world"B) |])
  |> Async.RunSynchronously

for (tn,offsets) in prodRes.topics do
  printfn "topic_name=%s" tn
  for (p,ec,offset) in offsets do
    printfn "partition=%i error_code=%i offset=%i" p ec offset



let fetchRes = 
  Kafka.fetch conn (FetchRequest.ofTopicPartition "absurd-topic" 0 0L 0 0 1000) 
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i" tn p ec

```

# Maintainer(s)

- [@strmpnk](https://github.com/strmpnk)
- [@eulerfx](https://github.com/eulerfx)

# License

This project is subject to the Apache Licence, Version 2.0. A copy of the license can be found in [LICENSE.txt](LICENSE.txt) at the root of this repo.

# Code of Conduct 

This project has adopted the code of conduct defined by the [Contributor Covenant](http://contributor-covenant.org/) to clarify expected behavior in our community. This code of conduct has been [adopted by many other projects](http://contributor-covenant.org/adopters/). For more information see the [Code of conduct](https://github.com/jet/kafunk/wiki/Code-of-Conduct).

