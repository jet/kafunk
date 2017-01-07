namespace Kafunk

open System
open System.Collections.Concurrent
open System.Threading

type LogLevel =
  | Trace = 0
  | Info  = 1
  | Warn  = 2
  | Error = 3
  | Fatal = 4

type Logger = {
  name : string
  buffer : BlockingCollection<string>
}


[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Log =

  open System
  open System.Collections.Concurrent
  open FSharp.Control
  open System.IO
  open System.Text

  let private buffer = new BlockingCollection<string>()

  let private consume () =
    let bufferSize = 4096
    use stdout = Console.OpenStandardOutput (bufferSize)
    use sw = new StreamWriter(stdout, Encoding.Default, bufferSize)
    sw.AutoFlush <- true
    for line in buffer.GetConsumingEnumerable() do
      sw.WriteLine line
    
  (let t = new Thread(ThreadStart(consume)) in t.Start())

  let create name = { name = name ; buffer = buffer }

  let mutable MinLevel = LogLevel.Info



[<AutoOpen>]
module LoggerEx =

  type Logger with

    member inline ts.log (format, level:LogLevel) =
      let inline trace (m:string) =
        if level >= Log.MinLevel then
          ts.buffer.Add (String.Format("{0:yyyy-MM-dd HH:mm:ss:ffff}|{1}|{2}|{3}", DateTime.UtcNow, (level.ToString().ToUpperInvariant()), ts.name, m))
      Printf.kprintf trace format

    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)

    member inline ts.error format = ts.log (format, LogLevel.Error)

    member inline ts.warn format = ts.log (format, LogLevel.Warn)

    member inline ts.info format = ts.log (format, LogLevel.Info)

    member inline ts.trace format = ts.log (format, LogLevel.Trace)