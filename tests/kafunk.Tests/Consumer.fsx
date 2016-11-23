#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"

open FSharp.Control
open Kafunk

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "test-topic_1019"
let group = argiDefault 3 "leo_test16"

let go = async {
  let! conn = Kafka.connHostAsync host
  let consumerCfg = 
    ConsumerConfig.create (group, [|topic|], initialFetchTime=Time.EarliestOffset, fetchBufferBytes=100000)
  return!
    Consumer.create conn consumerCfg
    |> Consumer.consume (fun tn p ms commit -> async {
      Log.info "consuming_message_set|topic=%s partition=%i count=%i size=%i first_offset=%i"
        tn
        p
        (ms.messages.Length)
        (ms.messages |> Seq.sumBy (fun (_,s,_) -> s))
        (if ms.messages.Length > 0 then ms.messages |> Seq.map (fun (o,_,_) -> o) |> Seq.min else -1L)
      do! commit
    })
}

Async.RunSynchronously go