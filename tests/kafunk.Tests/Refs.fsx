#r "bin/release/fsharp.control.asyncseq.dll"
#r "bin/Release/kafunk.dll"
#r "bin/Release/kafunk.Tests.dll"
#load "Prelude.fs"
#load "Async.fs"

open Kafunk
open System

let argiDefault i def = Environment.GetCommandLineArgs() |> Seq.tryItem (i + 1) |> Option.getOr def