#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let conn = Kafka.connHost "localhost"

let fetchRes = 
  Kafka.fetch conn (FetchRequest.ofTopicPartition "test-topic" 0 0L 0 0 1000) 
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i" tn p ec
