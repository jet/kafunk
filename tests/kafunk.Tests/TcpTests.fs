module TcpTests

open NUnit.Framework

open System
open System.Text
open Kafunk


let shouldEqual (expected:'a) (actual:'a) (msg:string option) =
  if expected <> actual then
    let msg = 
      match msg with
      | Some msg -> sprintf "expected=%A\nactual=%A message=%s" expected actual msg
      | None -> sprintf "expected=%A\nactual=%A" expected actual
    Assert.Fail msg


[<Test>]
let ``framing should work`` () =
  
  for msgSize in [1..100] do
  
    let msg : byte[] = Array.zeroCreate msgSize
  
    let rng = Random()
    rng.NextBytes msg

    let msgList = msg |> Array.toList
  
    let framed = 
      Binary.Segment(msg)
      |> Framing.LengthPrefix.frame
      |> Array.map Binary.toArray
      |> Array.concat

    for chunks in [1..framed.Length] do
      let chunkedList =
        framed
        |> Array.groupInto chunks
      let chunked =
        chunkedList
        |> Array.map (fun c -> Binary.Segment(c))    
        |> AsyncSeq.ofSeq
      let unframed = 
        Framing.LengthPrefix.unframe chunked
        |> AsyncSeq.toList
        |> Async.RunSynchronously
        |> List.map (Binary.toArray >> Array.toList)
        |> List.concat
      shouldEqual msgList unframed (Some (sprintf "(message_size=%i chunks=%i)" msgSize chunks))
