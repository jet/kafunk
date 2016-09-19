#r "bin/Release/kafunk.dll"
#time "on"

open Kafunk

// Replace this with an initial broker you wish to use.
let conn = Kafka.connHostAndPort "127.0.0.1" 9092

let producerCfg =
  ProducerCfg.create ([|"test-topic"|], Partitioner.konst 0, requiredAcks=RequiredAcks.Local)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let prodRes =
  Producer.produceSingle producer ("test-topic", [| ProducerMessage.ofBytes ("hello world"B) |])
  |> Async.RunSynchronously

printfn "receiving produce response"

for (tn,offsets) in prodRes.topics do
  printfn "topic_name=%s" tn
  for (p,ec,offset) in offsets do
    printfn "partition=%i error_code=%i offset=%i" p ec offset