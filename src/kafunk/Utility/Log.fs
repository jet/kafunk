namespace Kafunk

open System
open System.Collections.Concurrent
open System.Threading
open System.IO

/// An asynchronous implementation of IEvent<_>.
/// Triggers are placed into a buffer, and published on a separate thread.
type internal AsyncEvent<'a> (bufferSize:int) =
  let mutable st = 0
  let buf = new BlockingCollection<'a> (bufferSize)
  let evt = new Event<'a>()
  let trigger () =
    for a in buf.GetConsumingEnumerable () do
      evt.Trigger a
  do (let t = new Thread(ThreadStart(trigger)) in t.IsBackground <- true ; t.Start())
  
  /// Puts an event into a buffer to be published asyncrhonously.
  member __.Trigger (a:'a) =
    if st = 0 then
      buf.Add a
  
  /// Publishes the observation.
  member __.Publish = evt.Publish
  
  /// Stops the event and the publishing thread.
  member __.Stop () = 
    if Interlocked.CompareExchange (&st, 1, 0) = 0 then
      buf.CompleteAdding ()

/// The log level.
type LogLevel =
  | Trace = 0
  | Info  = 1
  | Warn  = 2
  | Error = 3
  | Fatal = 4

/// A log entry.
type LogEntry =
  struct
    val dt : DateTimeOffset
    val level : LogLevel
    val logger : string
    val message : string
    val event : string
    val kvps : (string * obj) seq
    new (dt,level,logger,message,event) = 
      { dt = dt ; level = level ; logger = logger ; message = message ; event = event ; kvps = Seq.empty }
    new (dt,logger,event,kvps) = 
      { dt = dt ; level = LogLevel.Info ; logger = logger ; message = null ; event = event ; kvps = kvps }
  end
  with
    
    static member internal PrintKvps (kvps:(string * obj) seq, sw:StreamWriter) =
      for (k,v) in kvps do
        sw.Write("{0}={1} ; ", k, v)

    static member internal Print (e:LogEntry, sw:StreamWriter) =
      if isNull e.message then
        sw.Write ("{0:yyyy-MM-dd HH:mm:ss:ffff}|{1}|{2}|", e.dt, e.logger, e.event)
        LogEntry.PrintKvps (e.kvps, sw)
        sw.WriteLine ()
      else
        sw.WriteLine("{0:yyyy-MM-dd HH:mm:ss:ffff}|{1}|{2}|{3}", e.dt, (e.level.ToString().ToUpperInvariant()), e.logger, e.message)

/// A logger.
type Logger = private {
  name : string
  publisher : AsyncEvent<LogEntry>
}

/// Logging operations.
[<RequireQualifiedAccess>]
[<Compile(Module)>]
module Log =

  open System
  open System.Collections.Concurrent
  open FSharp.Control
  open System.IO
  open System.Text

  let private event = new AsyncEvent<LogEntry>(1000000)
  
  /// Publishes all log events.
  let Event = event.Publish

  /// Creates a logger with the specified name.
  let create name = { name = name ; publisher = event }

  /// Gets/sets the minimum logging level for the Console log event printer.
  let mutable MinLevel = LogLevel.Info

  /// Creates and subscribes a Console log event printer.
  let private consolePrinter (bufferSize:int) =
    let stdout = Console.OpenStandardOutput (bufferSize)
    let sw = new StreamWriter(stdout, Encoding.Default, bufferSize)
    sw.AutoFlush <- true
    let dispose = Disposable.ofFun (fun () -> sw.Dispose () )
    let subs = Event.Subscribe (fun e -> 
      if e.level >= MinLevel then
        LogEntry.Print (e, sw))
    Disposable.ofDisposables [ subs ; dispose ]
    
  /// When disposed, unsubscribes the Console log event printer.
  let ConsolePrinterSubscription = 
    consolePrinter 4096


[<AutoOpen>]
module LoggerEx =

  type Logger with

    member logger.event eventName (kvps:(string * obj) seq) =
      logger.publisher.Trigger (LogEntry(DateTimeOffset.UtcNow, logger.name, eventName, kvps))

    member logger.log (format, level:LogLevel) =
      if level >= Log.MinLevel then
        let dt = DateTimeOffset.UtcNow
        let trace (m:string) = logger.publisher.Trigger (LogEntry(dt, level, logger.name, m, null))
        Printf.kprintf trace format
      else
        Printf.kprintf ignore format

    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)
    member inline ts.error format = ts.log (format, LogLevel.Error)
    member inline ts.warn format = ts.log (format, LogLevel.Warn)
    member inline ts.info format = ts.log (format, LogLevel.Info)
    member inline ts.trace format = ts.log (format, LogLevel.Trace)


