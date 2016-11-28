namespace Kafunk

open System
open System.Collections.Concurrent

type LogLevel =
  | Trace = 0
  | Info  = 1
  | Warn  = 2
  | Error = 3
  | Fatal = 4
//  with
//    override x.ToString () =
//      match x with
//      | Trace -> "TRACE" | Info -> "INFO" | Warn -> "WARN" | Error -> "ERROR" | Fatal -> "FATAL"

type Logger = {
  name : string
  buffer : BlockingCollection<string>
}


[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Log =

  open System.Collections.Concurrent
  open FSharp.Control

  let private buffer = new BlockingCollection<string>()

  Async.Start (async {
    return!
      buffer.GetConsumingEnumerable()
      |> AsyncSeq.ofSeq
      |> AsyncSeq.bufferByCountAndTime 1000 500
      |> AsyncSeq.iter (fun lines ->
        for line in lines do
          System.Console.Out.WriteLine line) })

  let create name = { name = name ; buffer = buffer }

  let mutable MinLevel = LogLevel.Info



[<AutoOpen>]
module LoggerEx =

  type Logger with

    member inline ts.log (format, level:LogLevel) =
      let inline trace (m:string) =
        if level >= Log.MinLevel then
          ts.buffer.Add (String.Format("{0:yyyy-MM-dd hh:mm:ss:ffff}|{1}|{2}|{3}", DateTime.UtcNow, (level.ToString().ToUpperInvariant()), ts.name, m))
      Printf.kprintf trace format

    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)

    member inline ts.error format = ts.log (format, LogLevel.Error)

    member inline ts.warn format = ts.log (format, LogLevel.Warn)

    member inline ts.info format = ts.log (format, LogLevel.Info)

    member inline ts.trace format = ts.log (format, LogLevel.Trace)