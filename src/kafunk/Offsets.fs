#nowarn "40"
namespace Kafunk

open System
open System.Threading

type private PeriodicCommitQueueMsg =
  | Enqueue of offsets:(Partition * Offset) seq
  | Commit of AsyncReplyChannel<unit> option

/// A queue for offsets to be periodically committed.
type PeriodicCommitQueue internal (interval:TimeSpan, assignedPartitions:Async<Partition[]>, commit:(Partition * Offset)[] -> Async<unit>) =
  
  let cts = new CancellationTokenSource()

  let rec enqueueLoop (commits:Map<Partition, Offset>) (mb:Mb<_>) = async {
    let! msg = mb.Receive ()
    match msg with
    | Enqueue (os) ->
      let commits' =
        (commits,os) 
        ||> Seq.fold (fun m (p,o) -> Map.add p o m)
      return! enqueueLoop commits' mb
    | Commit rep ->
      // only commit currently assigned partitions
      let! assignedPartitions = assignedPartitions
      let commits = 
        commits
        |> Map.onlyKeys assignedPartitions
      let offsets =
        commits
        |> Map.toSeq
        |> Seq.map (fun (p,o) -> p,o)
        |> Seq.toArray
      if offsets.Length > 0 then
        do! commit offsets
      rep |> Option.iter (fun r -> r.Reply())
      return! enqueueLoop commits mb }

  let mbp = Mb.Start (enqueueLoop Map.empty, cts.Token)
  
  let rec commitLoop = async {
    do! Async.sleep interval
    mbp.Post (Commit None)
    return! commitLoop }

  do Async.Start (commitLoop, cts.Token)

  member internal __.Enqueue (os:(Partition * Offset) seq) =
    mbp.Post (Enqueue (os))

  member internal __.Flush () =
    mbp.PostAndAsyncReply (fun ch -> Commit (Some ch))

  interface IDisposable with
    member __.Dispose () =
      cts.Cancel ()
      (mbp :> IDisposable).Dispose ()

/// Operations on offsets.
module Offsets =
  
  /// Gets available offsets for the specified topic, at the specified times.
  /// Returns a map of times to offset responses.
  /// If empty is passed in for Partitions, will return information for all partitions.
  let offsets (conn:KafkaConn) (topic:TopicName) (partitions:Partition seq) (times:Time seq) (maxOffsets:MaxNumberOfOffsets) : Async<Map<Time, OffsetResponse>> = async {
    let! partitions = async {
      if Seq.isEmpty partitions then
        let! meta = conn.GetMetadata [|topic|]
        return
          meta
          |> Seq.collect (fun kvp -> kvp.Value)
          |> Seq.toArray
      else
        return partitions |> Seq.toArray }
    return!
      times
      |> Seq.map (fun time -> async {
        let offsetReq = OffsetRequest(-1, [| topic, partitions |> Array.map (fun p -> p,time,maxOffsets) |]) 
        let! offsetRes = Kafka.offset conn offsetReq
        return time,offsetRes })
      |> Async.Parallel
      |> Async.map (Map.ofArray) }

  /// Gets the offset range (Time.EarliestOffset,Time.LatestOffset) for a topic, for the specified partitions.
  /// If empty is passed in for Partitions, will return information for all partitions.
  let offsetRange (conn:KafkaConn) (topic:TopicName) (partitions:Partition seq) : Async<Map<Partition, Offset * Offset>> = async {
    let! offsets = offsets conn topic partitions [Time.EarliestOffset;Time.LatestOffset] 1
    
    let filter (res:OffsetResponse) =
      res.topics
      |> Seq.collect (fun (t,ps) -> 
        if t = topic then ps |> Seq.map (fun p -> p.partition, p.offsets.[0])
        else Seq.empty)
      |> Map.ofSeq

    let earliest = offsets |> Map.find Time.EarliestOffset |> filter
    let latest = offsets |> Map.find Time.LatestOffset |> filter

    let offsets =
      (earliest,latest)
      ||> Map.mergeChoice (fun _ -> function
        | Choice1Of3 (e,l) -> (e,l)
        | Choice2Of3 e -> (e,-1L)
        | Choice3Of3 l -> (-1L,l))
    
    return offsets }

  /// Creates a periodic offset commit queue which commits enqueued commits at the specified interval.
  let createPeriodicCommitQueue interval = 
    new PeriodicCommitQueue (interval)

  /// Asynchronously enqueues offsets to commit, replacing any existing commits for the specified topic-partitions.
  let enqueuePeriodicCommit (q:PeriodicCommitQueue) (os:(Partition * Offset) seq) =
    q.Enqueue (os)
    
  /// Commits whatever remains in the queue.
  let flushPeriodicCommit (q:PeriodicCommitQueue) =
    q.Flush ()