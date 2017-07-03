module SVarTests

open System
open Kafunk
open Kafunk.Testing
open Kafunk.SVar
open FSharp.Control
open NUnit.Framework

let asyncSeqToList (s:AsyncSeq<'a>) =
  let ls = 
    s
    |> AsyncSeq.toListAsync 
    |> Async.timeoutOption (TimeSpan.FromMilliseconds 100.0) 
    |> Async.RunSynchronously
  match ls with
  | Some ls -> ls
  | None -> failwith "AsyncSeq timed out!"

[<Test>]
let ``SVar.tap should return stream of all values put after tap`` () =   
  for put in [ [] ; [1] ; [1;2;3;4] ] do
    let v = SVar.create ()
    let stream = SVar.tap v
    async {
      for x in put do
        do! SVar.put x v } |> Async.RunSynchronously
    let actual = stream |> AsyncSeq.take put.Length |> asyncSeqToList
    shouldEqual put actual None

[<Test>]
let ``SVar.tap should return current value`` () = 
  let v = SVar.create ()
  let expected = 1
  SVar.put expected v |> Async.RunSynchronously
  let expected = [expected]
  let actual = SVar.tap v |> AsyncSeq.take 1 |> asyncSeqToList
  shouldEqual expected actual None

[<Test>]
let ``SVar.tap should return current and subsequent values`` () = 
  for expected in [ [1] ; [1;2] ; [1;2;3;4;5] ] do
    let v = SVar.create ()
    SVar.put 0 v |> Async.RunSynchronously // skipped
    SVar.put expected.Head v |> Async.RunSynchronously
    let tapped = SVar.tap v
    async {
      for x in expected.Tail do
        do! SVar.put x v } |> Async.RunSynchronously
    let actual = tapped |> AsyncSeq.take expected.Length |> asyncSeqToList
    shouldEqual expected actual None


  
  
