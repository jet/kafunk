#r "bin/release/kafunk.dll"
#time "on"

open Kafunk
open System

let host = "guardians-kafka-cluster.qa.jet.com:9092"
let topic = "nova-retailskus-profx"
//let topic = "test-topic"
//let host = "localhost"
let conn = Kafka.connHost host

let offsetRes =
  Kafka.Composite.topicOffsets conn (-2L, 1) topic
  |> Async.RunSynchronously

printfn "offset response topics=%i" offsetRes.topics.Length
for (tn,ps) in offsetRes.topics do
  for p in ps do
    for o in p.offsets do
      printfn "topic=%s partition=%i offset=%i" tn p.partition o


let (tn,ps) = offsetRes.topics.[0]

let fetchRes = 
  Kafka.fetch conn (FetchRequest.ofTopicPartition tn (ps.[0].partition) (ps.[0].offsets.[0]) 0 0 20000) 
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i hwm=%i message_set_size=%i messages=%i" tn p ec hmo mss ms.messages.Length
    for (o,ms,m) in ms.messages do
      printfn "message offset=%i size=%i message=%s" o ms (m.value |> Binary.toString)

