#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics


//let file = @"C:\code\kafunk\tests\kafunk.Tests\FetchResponse_RecordBatch.bin"
let file = @"C:\code\kafunk\tests\kafunk.Tests\fetch_p=0_p=3_daccd6f71ae945f5afe7cb57a3a7ee28.bin"
let buf = System.IO.File.ReadAllBytes(file) |> Binary.ofArray |> BinaryZipper
let res = FetchResponse.Read (5s, buf)

for (t,ps) in res.topics do
  for (p,_,hwo,_,_,_,_,ms) in ps do
    let cms = ConsumerMessageSet(t,p,ms,hwo)
    let lastOffset = ConsumerMessageSet.lastOffset cms
    let nextOffset = MessageSet.nextOffset ms hwo
    printfn "p=%i message_count=%i last_offset=%i next_offset=%i" p ms.messages.Length lastOffset nextOffset
    for m in ms.messages do
      printfn "key=%s value=%s" (Binary.toString m.message.value) (Binary.toString m.message.value)

