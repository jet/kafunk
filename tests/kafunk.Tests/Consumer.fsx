#r "bin/release/kafunk.dll"
#time "on"

open Kafunk


let topic = "test-topic"
let host = "localhost"
let group = "leo_test16"

//let conn = Kafka.connHost host
//let consumerCfg = ConsumerConfig.create (group, [|topic|], initialFetchTime=Time.EarliestOffset, fetchBufferBytes=100000)
//Consumer.consume conn consumerCfg
//|> Consumer.callbackCommitAfter 
//  (fun ms -> async {    
//    return () })

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
            return () })) 
        |> Async.Parallel
        |> Async.Ignore })
}

Async.RunSynchronously go