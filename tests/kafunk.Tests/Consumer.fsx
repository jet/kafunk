#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let host = "guardians-kafka-cluster.qa.jet.com:9092"
//let host = "localhost"
let topic = "nova-retailskus-profx"
//let topic = "test-topic2"
let group = "leo_test10"



let go = async {

  use! conn = Kafka.connHostAsync host

  let consumerCfg = 
    ConsumerConfig.create (group, [|topic|], initialFetchTime=Time.EarliestOffset, fetchBufferBytes=100000)

  let! stream = Consumer.consume conn consumerCfg

  return!
    stream
    |> AsyncSeq.iterAsync (fun (gen,topicPartitions) ->
      topicPartitions
      |> Seq.map (fun (tn,p,stream) -> 
        printfn "streaming|topic=%s partition=%i" tn p
        stream
        |> AsyncSeq.iterAsync (fun (ms,commitOffset) -> async {
          printfn "consuming_message_set|count=%i size=%i first_offset=%i"
            (ms.messages.Length)
            (ms.messages |> Seq.sumBy (fun (_,s,_) -> s))
            (if ms.messages.Length > 0 then ms.messages |> Seq.map (fun (o,_,_) -> o) |> Seq.min else -1L)
          do! commitOffset
          return () }))
      |> Async.Parallel
      |> Async.Ignore)
  
}

Async.RunSynchronously go