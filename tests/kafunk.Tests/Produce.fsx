#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Diagnostics
open System.Threading
open Refs

Log.MinLevel <- LogLevel.Trace
let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"
let topic = argiDefault 2 "absurd-topic"

let key = "k"B |> Binary.ofArray
let value = "v"B |> Binary.ofArray

let magicByte = 2y
let ts = 0L
  //if magicByte > 1y then int64 (DateTime.UtcNow - UnixEpoch).TotalMilliseconds
  //else 0L
let m = Message(0, magicByte, 0y, ts, key, value)

let ms' = 
  if magicByte > 1y then Message.SizeRecord 0L 0 m
  else Message.Size m

let ms = MessageSet([| MessageSetItem(0L, ms', m) |])

let mss =  
  if magicByte > 1y then MessageSet.SizeRecordBatch ms
  else MessageSet.Size ms

let req = ProduceRequest(RequiredAcks.AllInSync, 10000, [|ProduceRequestTopicMessageSet(topic, [| ProduceRequestPartitionMessageSet(1, mss, ms) |]) |], null)

let kafkaConfig = 
  KafkaConfig.create (
    [KafkaUri.parse host], 
    version = Versions.V_0_10_1,
    autoApiVersions = true,
    clientId = "rdkafka"
  )

let conn = Kafka.conn kafkaConfig

let res = Kafka.produce conn req |> Async.RunSynchronously

for t in res.topics do
  for p in t.partitions do
    printfn "t=%s p=%i o=%i ec=%i" t.topic p.partition p.offset p.errorCode

