#r "bin/Release/kafunk.dll"
#time "on"

open Kafunk

// Replace this with an initial broker you wish to use.
let conn = Kafka.connHostAndPort "127.0.0.1" 9092

let producerCfg =
  ProducerCfg.create ("test-topic", Partitioner.konst 0, requiredAcks=RequiredAcks.Local)

let producer =
  Producer.createAsync conn producerCfg
  |> Async.RunSynchronously

let N = 10

Seq.init N id
|> Seq.map (fun i -> async {

  let payload = Array.zeroCreate 5000

  printfn "sending request=%i" i

  let! prodRes =
    Producer.produceSingle producer ("test-topic", [| ProducerMessage.ofBytes payload |])

  printfn "received produce response=%i" i

  for (tn,offsets) in prodRes.topics do
    printfn "topic_name=%s" tn
    for (p,ec,offset) in offsets do
      printfn "partition=%i error_code=%i offset=%i" p ec offset

})
|> Async.ParallelIgnore 10
|> Async.RunSynchronously