#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

// TODO: empty fetch response

let conn = Kafka.connHost "localhost"

let fetchRes = 
  Kafka.fetch conn (FetchRequest.ofTopicPartition "test-topic" 0 0L 0 0 6000) 
  |> Async.RunSynchronously

for (tn,pmds) in fetchRes.topics do
  for (p,ec,hmo,mss,ms) in pmds do
    printfn "topic=%s partition=%i error=%i hwm=%i message_set_size=%i messages=%i" tn p ec hmo mss ms.messages.Length
    for (o,ms,m) in ms.messages do
      printfn "message offset=%i size=%i message=%s" o ms (m.value |> Binary.toString)

