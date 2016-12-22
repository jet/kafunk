#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/release/kafunk.dll"
#time "on"


open Kafunk
open System

let N = 100000

//let choose i = async {
//  return! Async.choose2 (Async.Sleep 1) (Async.Sleep 1) }

let choose i = async {
  return! Async.choose (Async.empty) (Async.empty) }

Seq.init N choose
|> Async.ParallelThrottledIgnore Int32.MaxValue
//|> Async.Parallel
|> Async.RunSynchronously
|> ignore


// Real: 00:00:02.463, CPU: 00:00:04.531, GC gen0: 57, gen1: 54, gen2: 2
// Real: 00:00:01.807, CPU: 00:00:03.156, GC gen0: 48, gen1: 43, gen2: 2