#nowarn "40"
namespace Kafunk

open System
open FSharp.Control

/// A periodic offset commit queue.
type PeriodicCommitQueue = private {
  queuedOffsets : Mb<(Partition * Offset)[]>
  flushes : Mb<IVar<unit>>
  proc : Async<unit>
}

/// Operations on periodic commit queues.
[<Compile(Module)>]
module PeriodicCommitQueue =

  let private Log = Log.create "Kafunk.Offsets"

  /// Creates a period offset committer, which commits at the specified interval (even if no new offsets are enqueued).
  /// Commits the current offsets assigned to the consumer upon creation.
  /// Returns a pair consisting of the commit queue and a process which commits offsets and reacts to rebalancing.
  let create 
    (interval:TimeSpan) 
    (rebalance:AsyncSeq<(Partition * Offset)[]>)
    (commit:(Partition * Offset)[] -> Async<unit>) : Async<PeriodicCommitQueue> = async {
    let queuedOffsetsMb = Mb.create ()
    let flushesMb = Mb.create ()
    let queuedOffsets = AsyncSeq.replicateInfiniteAsync (Mb.take queuedOffsetsMb)
    let flushes = AsyncSeq.replicateInfiniteAsync (Mb.take flushesMb)
    let ticks = AsyncSeq.intervalMs (int interval.TotalMilliseconds)
    let commitProc =
      AsyncSeq.mergeChoice4 ticks queuedOffsets rebalance flushes
      |> AsyncSeq.foldAsync (fun offsets event -> async {
        match event with
        | Choice1Of4 _ ->
          let os = offsets |> Map.toArray |> Array.filter (fun (_,o) -> o <> -1L)
          if os.Length > 0 then
            do! commit os
          return offsets
        | Choice2Of4 queued ->
          //Log.trace "updating_offsets|state=%A queued=%A" offsets queued
          return offsets |> Map.updateMany queued
        | Choice3Of4 assigned ->
          //Log.info "offsets_reassigned|previous=%s assigned=%s" 
          //  (Printers.partitionOffsetPairs (offsets |> Map.toSeq)) (Printers.partitionOffsetPairs assigned)
          return assigned |> Map.ofArray
        | Choice4Of4 (rep:IVar<unit>) ->
          try
            let os = offsets |> Map.toArray
            if os.Length > 0 then
              do! commit os
            IVar.put () rep
          with ex ->
            IVar.error ex rep
          return offsets
        }) Map.empty  
      |> Async.Ignore
    return { proc = commitProc ; queuedOffsets = queuedOffsetsMb ; flushes = flushesMb } }

  /// Asynchronously enqueues offsets to commit, replacing any existing commits for the specified topic-partitions.
  let enqueue (q:PeriodicCommitQueue) (os:(Partition * Offset) seq) =
    q.queuedOffsets.Post (os |> Seq.toArray)
    
  /// Commits whatever remains in the queue.
  let flush (q:PeriodicCommitQueue) = async {
    let rep = IVar.create ()
    q.flushes.Post rep
    return! rep |> IVar.get }

  /// Returns the commit proccess, which must be explicitly started by the caller.
  /// This way, the caller can handle exceptions in the commit process.
  let proccess (q:PeriodicCommitQueue) =
    q.proc

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