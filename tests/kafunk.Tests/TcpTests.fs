module TcpTests

open NUnit.Framework
open FSharp.Control
open System
open System.Text
open Kafunk


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
      |> Array.collect Binary.toArray

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
        |> List.collect (Binary.toArray >> Array.toList)
      shouldEqual msgList unframed (Some (sprintf "(message_size=%i chunks=%i)" msgSize chunks))
