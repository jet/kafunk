#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let conn = Kafka.connHost "localhost"

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