#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let conn = Kafka.connHostAndPort "127.0.0.1" 9092

let consumerCfg = 
  ConsumerConfig.create ("consumer-group-A", [|"test-topic2"|])

Consumer.consume conn consumerCfg
|> AsyncSeq.iterAsync (fun (gen,topicPartitions) ->
  topicPartitions
  |> Seq.map (fun (tn,p,stream) -> 
    stream
    |> AsyncSeq.iterAsync (fun (ms,commitOffset) -> async {
      printfn "consuming message set|offsets=%s" (String.concat "," (ms.messages |> Seq.map (fun (o,ms,m) -> sprintf "offset=%i" o)))
      return () }))
  |> Async.Parallel
  |> Async.Ignore)
|> Async.RunSynchronously