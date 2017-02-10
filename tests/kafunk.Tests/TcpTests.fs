module TcpTests

open NUnit.Framework
open FSharp.Control
open System
open System.Text
open Kafunk

let groupInto (groups:int) (a:'a[]) : 'a[][] =
  if groups < 1 then invalidArg "groups" "must be positive"
  let f = float groups / float a.Length
  let groups = Array.init groups (fun _ -> ResizeArray<_>())
  for i in [0..a.Length - 1] do
    let j = int (floor (float i * f))
    groups.[j].Add(a.[i])
  groups |> Array.map (fun g -> g.ToArray())

[<Test>]
let ``EndPoint should be comparable``() =
  let ep1 = EndPoint.parse ("127.0.0.1", 9092)
  let ep2 = EndPoint.parse ("127.0.0.1", 9092)
  let actual = ep1.Equals (ep2)
  shouldEqual true actual None

[<Test>]
let ``Framing should work`` () =
  for msgSize in [1..256] do
  
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
        |> groupInto chunks
      let chunked =
        chunkedList
        |> Array.map (fun c -> Binary.Segment(c))
        |> AsyncSeq.ofSeq
      let unframed = 
        Framing.LengthPrefix.unframe chunked
        |> AsyncSeq.toList
        |> List.collect (Binary.toArray >> Array.toList)
      shouldEqual msgList unframed (Some (sprintf "(message_size=%i chunks=%i)" msgSize chunks))
