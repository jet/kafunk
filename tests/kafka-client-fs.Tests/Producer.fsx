#r "bin/release/KafkaFs.dll"
#time "on"

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Kafunk

let Log = Log.create __SOURCE_FILE__

let conn = Kafka.connHost "localhost"

let cfg = ProducerCfg.create ([|"test"|], Partitioner.konst 0, requiredAcks=RequiredAcks.None)

let producer =
  Producer.createAsync conn cfg |> Async.RunSynchronously

let res =
  Producer.produceSingle producer ("test", [| ProducerMessage.ofBytes ("hello world"B) |])
  |> Async.RunSynchronously

Log.info "received produce response"

res.topics
|> Array.iter (fun (tn,offsets) ->
  Log.info "topic_name=%s" tn
  offsets
  |> Array.iter (fun (p,ec,offset) ->
    Log.info "partition=%i error_code=%i offset=%i" p ec offset
  )
)