#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let host = "guardians-kafka-cluster.qa.jet.com:9092"
//let host = "localhost"
let topic = "nova-retailskus-profx"
//let topic = "test-topic2"
let conn = Kafka.connHost host
let group = "leo_test5"

let consumerCfg = 
  ConsumerConfig.create (group, [|topic|], initialFetchTime=Time.LatestOffset)

Consumer.consume conn consumerCfg
|> AsyncSeq.iterAsync (fun (gen,topicPartitions) ->
  topicPartitions
  |> Seq.map (fun (tn,p,stream) -> 
    stream
    |> AsyncSeq.iterAsync (fun (ms,commitOffset) -> async {
      printfn "consuming_message_set|offsets=%s" (String.concat "," (ms.messages |> Seq.map (fun (o,ms,m) -> sprintf "offset=%i" o)))
      do! commitOffset
      return () }))
  |> Async.Parallel
  |> Async.Ignore)
|> Async.RunSynchronously

