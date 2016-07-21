#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let conn = Kafka.connHostAndPort "127.0.0.1" 9092

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
