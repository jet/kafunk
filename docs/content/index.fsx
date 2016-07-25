(*** hide ***)
// This block of code is omitted in the generated HTML documentation. Use 
// it to define helpers that you do not want to show in the documentation.
#I "../../bin"

(**
Kafunk - F# Kafka client
======================

Example
-------

This example demonstrates a few uses of the Kafka client.

*)

#r "kafunk.dll"
open Kafunk

let conn = Kafka.connHost "existentialhost"


let metadata = 
  Kafka.metadata conn (Metadata.Request([|"absurd-topic"|])) 
  |> Async.RunSynchronously

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



let consumerCfg = 
  Consumer.ConsumerConfig.create ("consumer-group", [|"absurd-topic"|])

Consumer.consume conn consumerCfg
|> AsyncSeq.iterAsync (fun (generationId,memberId,topics) ->
  // the outer AsyncSeq yield on every generation of the consumer groups protocol
  topics
  |> Seq.map (fun (topic,partition,stream) ->
    // the inner AsyncSeqs correspond to individual topic-partitions
    stream
    |> AsyncSeq.iterAsync (fun (ms,commit) -> async {
      for (offset,_,msg) in ms.messages do          
        printfn "processing topic=%s partition=%i offset=%i key=%s" topic partition offset (Message.keyString msg)
      do! commit }))
  |> Async.Parallel
  |> Async.Ignore)
|> Async.RunSynchronously



(**
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

The library is available under Apache 2.0. For more information see the 
[License file][license] in the GitHub repository. 

  [content]: https://github.com/jet/kafunk/tree/master/docs/content
  [gh]: https://github.com/jet/kafunk
  [issues]: https://github.com/jet/kafunk/issues
  [readme]: https://github.com/jet/kafunk/blob/master/README.md
  [license]: https://github.com/jet/kafunk/blob/master/LICENSE.txt
  
*)
