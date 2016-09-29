namespace Kafunk

// log

// TODO: implement!

type LogLevel =
  | Trace | Info | Warn | Error | Fatal
  with
    override x.ToString () =
      match x with
      | Trace -> "TRACE" | Info -> "INFO" | Warn -> "WARN" | Error -> "ERROR" | Fatal -> "FATAL"

type Logger = {
  name : string
}


[<AutoOpen>]
module LoggerEx =
  open System

  type Logger with

    member inline ts.log (format, level:LogLevel) =
      let inline trace (m:string) =
        Console.WriteLine(String.Format("{0:yyyy-MM-dd hh:mm:ss:ffff}|{1}|{2}|{3}", DateTime.Now, (level.ToString()), ts.name, m))
//        match level with
//        | LogLevel.Trace -> ()
//        | _ -> Console.WriteLine(String.Format("{0:yyyy-MM-dd hh:mm:ss:ffff}|{1}|{2}|{3}", DateTime.Now, (level.ToString()), ts.name, m))
      Printf.kprintf trace format

    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)

    member inline ts.error format = ts.log (format, LogLevel.Error)

    member inline ts.warn format = ts.log (format, LogLevel.Warn)

    member inline ts.info format = ts.log (format, LogLevel.Info)

    member inline ts.trace format = ts.log (format, LogLevel.Trace)

[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Log =

  let create name = { name = name }