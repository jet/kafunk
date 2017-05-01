#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System
open System.Threading
open System.Threading.Tasks
open System.Diagnostics

let N = 10000

Seq.init N id
//|> Seq.map Choice1Of2
|> Seq.map (fun i -> 
  if i = 420 then Choice2Of2 (exn("error"))
  else Choice1Of2 i)
|> Seq.map (fun x -> async {
  match x with
  | Choice1Of2 _ -> do! Async.Sleep 1
  | Choice2Of2 ex -> return raise ex })
|> Async.parallelThrottledIgnore 100
|> Async.RunSynchronously