namespace Kafunk

open Kafunk
open System
open System.Threading
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics

// *** WARNING ***: PLEASE DON'T LOOK AT THIS CODE

module private Comparer =

  let rev (c:IComparer<'a>) : IComparer<'a> =
    { new IComparer<'a> with member __.Compare(x, y) = c.Compare(y, x) }


type internal SortedBag<'a>(?c:IComparer<'a>) =
  let comp = defaultArg c (Comparer<_>.Default :> IComparer<_>)
  let list = new List<'a>()
  member __.Item i = list.[i]
  member __.Count = list.Count
  member __.Add (a) =
    let mutable i = list.BinarySearch(a, comp)
    if i < 0 then i <- ~~~i
    list.Insert(i, a)
    i
  member __.AddRange xs =
    for x in xs do
      __.Add x |> ignore
  member __.Clear () = 
    list.Clear()
  member __.RemoveAt index =
    list.RemoveAt index
  member __.RemoveRange (index,count) =
    list.RemoveRange (index,count)
  member __.RemoveUpToInclusive (a:'a) =
    let mutable i = list.BinarySearch(a, comp)
    if i >= 0 then 
      list.RemoveRange(0, i + 1)
  interface IEnumerable with
    member __.GetEnumerator() = list.GetEnumerator() :> IEnumerator
  interface IEnumerable<'a> with
    member __.GetEnumerator() = list.GetEnumerator() :> IEnumerator<'a>


module private Util =

  let inline getQuantile (percentile:float) (ms:SortedBag<_>) =
    let i = int (Math.Ceiling(float ms.Count * percentile)) - 1
    if i >= 0 && i < ms.Count then ms.[i]
    else Unchecked.defaultof<_>

  let inline write (log:Logger option) : string * [<ParamArray>]obj[] -> unit =
    match log with
    | Some log -> fun (msg,args) -> log.info "%s" msg
    | None -> Console.WriteLine


type TimedCounter internal (?log:Logger, ?periodMs:float) =

  let periodMs = defaultArg periodMs 1000.0
  let periodSs = periodMs / 1000.0
  let periodDenom = periodMs * periodSs

  let mutable count = 0
  let mutable tickCount = 0
  let [<VolatileField>] mutable lastCnt = 0
  let [<VolatileField>] mutable rateSum = 0.0
  let rates = new SortedBag<float>(Comparer.rev Comparer<_>.Default)

  let write : string * [<ParamArray>]obj[] -> unit =
    match log with
    | Some log -> fun (msg,args) -> log.info "%s" (String.Format(msg,args))
    | None -> Console.WriteLine

  let cb _ =
    let count = count
    if count = 0 then ()
    else
      let ticks = Interlocked.Increment &tickCount
      let delta = count - lastCnt
      lastCnt <- count
      let rateSec = (float delta) / periodDenom

      rates.Add rateSec |> ignore

      let percentile50 = rates |> Util.getQuantile 0.5
      let percentile90 = rates |> Util.getQuantile 0.9
      let percentile99 = rates |> Util.getQuantile 0.99
      let max = rates.[0]

      let rateSum' = rateSum + rateSec
      rateSum <- rateSum'
      let avgRate = rateSum' / (float ticks)

      write(
        "last_sec={0,-10} | avg_sec={1,-10} | tp50_sec={2,-10} | tp90_sec={3,-10} | tp99_sec={4,-10} | max_sec={5,-10} | count={6,-10}",
        [|
          Math.Round(rateSec * periodMs, 0)
          Math.Round(avgRate * periodMs, 0)
          Math.Round(percentile50 * periodMs, 0)
          Math.Round(percentile90 * periodMs, 0)
          Math.Round(percentile99 * periodMs, 0)
          Math.Round(max * periodMs, 0)
          count
        |])

  let timer = new Timer(cb, null, 0, int periodMs)

  member __.CountMany (cnt) =
    Interlocked.Add(&count, cnt) |> ignore

  member inline x.Count () = x.CountMany 1

  member __.Stop () = timer.Dispose()

  interface IDisposable with
    member x.Dispose() = x.Stop()


type IntHistogram internal (?log:Logger, ?periodMs:float) =
  let periodMs = defaultArg periodMs 1000.0
  let write = Util.write log
  let mutable count = 0
  let mutable sum = 0
  let values = new SortedBag<int>(Comparer.rev Comparer<_>.Default)
  let queue = new BlockingCollection<_>()
  let consume (_:obj) =
    use queue = queue
    queue.GetConsumingEnumerable()
    |> Seq.iter (fun x ->
      values.Add x |> ignore
      Interlocked.Increment(&count) |> ignore
      Interlocked.Add(&sum, x) |> ignore)
  do (new Thread(ThreadStart(consume), IsBackground=true)).Start()
  let emit (_:obj) =
    let count = count
    if count > 0 && values.Count > 0 then
      let tp50 = values |> Util.getQuantile 0.50 |> float
      let tp90 = values |> Util.getQuantile 0.10 |> float
      let tp99 = values |> Util.getQuantile 0.01 |> float
      let max = values.[0] |> float
      let min = values.[values.Count - 1] |> float
      let count = float count
      let avg = (float sum) / count
      write(
        "avg={0,-10} | tp50={1,-10} | tp90={2,-10} | tp99={3,-10} | min={4,-10} | max={5,-10} | count={6,-10}",
        [| Math.Round(avg, 0) ; Math.Round(tp50, 0) ; Math.Round(tp90, 0) ; Math.Round(tp99, 0) ; Math.Round(min, 0) ; Math.Round(max, 0) ; Math.Round(count, 0) |])

  let timer = new Timer(emit, null, 0, int periodMs)
  
  member __.Record (x:int) =
    queue.Add x

  member __.Stop () =
    timer.Dispose()
    queue.CompleteAdding()

  interface IDisposable with
    member x.Dispose() = x.Stop()

[<Compile(Module)>]
module Metrics =

  let counter (log:Logger) (periodMs:int) =
    new TimedCounter (log, float periodMs)

  let timer (log:Logger) (periodMs:int) =
    new IntHistogram (log, float periodMs)

  let throughputAsyncTo (tc:TimedCounter) (count:'a * 'b -> int) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      let! b = f a
      tc.CountMany (count (a,b))
      return b }

  let throughputAsync2To (tc:TimedCounter) (count:'a * 'b * 'c -> int) (f:'a -> 'b -> Async<'c>) : 'a -> 'b -> Async<'c> =
    fun a b -> async {
      let! c = f a b
      tc.CountMany (count (a,b,c))
      return c }
  
  let throughputAsync (log:Logger) (periodMs:int) (count:'a * 'b -> int) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    let tc = new TimedCounter(log, float periodMs)
    throughputAsyncTo tc count f

  let throughputAsync2 (log:Logger) (periodMs:int) (count:'a * 'b * 'c -> int) (f:'a -> 'b -> Async<'c>) : 'a -> 'b -> Async<'c> =
    let tc = new TimedCounter(log, float periodMs)
    throughputAsync2To tc count f




  let latencyAsyncTo (hist:IntHistogram) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    fun a -> async {
      let t1 = Stopwatch.GetTimestamp ()
      let! b = f a
      let t2 = Stopwatch.GetTimestamp ()
      hist.Record (int (TimeSpan(t2 - t1).TotalMilliseconds))
      return b }

  let latencyAsync (log:Logger) (periodMs:int) (f:'a -> Async<'b>) : 'a -> Async<'b> =
    let hist = new IntHistogram(log, float periodMs)
    latencyAsyncTo hist f
    