#r "bin/release/kafunk.dll"
#time "on"

open Kafunk

let host = "guardians-kafka-cluster.qa.jet.com:9092"
//let host = "localhost"
let topic = "nova-retailskus-profx"
//let topic = "test-topic2"
let group = "leo_test13"


let go = async {
  let! conn = Kafka.connHostAsync host
  let consumerCfg = 
    ConsumerConfig.create (group, [|topic|], initialFetchTime=Time.EarliestOffset, fetchBufferBytes=100000)
  return!
    Consumer.consume conn consumerCfg
    |> AsyncSeq.iterAsync (fun (generationId,topics) -> async {      
      return!
        topics
        |> Seq.map (fun (tn,p,stream) -> 
          printfn "streaming|topic=%s partition=%i" tn p
          stream
          |> AsyncSeq.iterAsync (fun (ms,commitOffset) -> async {
            printfn "consuming_message_set|count=%i size=%i first_offset=%i"
              (ms.messages.Length)
              (ms.messages |> Seq.sumBy (fun (_,s,_) -> s))
              (if ms.messages.Length > 0 then ms.messages |> Seq.map (fun (o,_,_) -> o) |> Seq.min else -1L)
            do! commitOffset
            //Async.Start commitOffset
            //do! Async.Sleep 5000
            return () })) 
        |> Async.Parallel
        |> Async.Ignore })
}

Async.RunSynchronously go