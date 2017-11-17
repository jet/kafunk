#nowarn "40"
namespace Kafunk

open System
open FSharp.Control

/// Operations on offsets.
module Offsets =
  
  /// Creates a period offset committer, which commits at the specified interval (even if no new offsets are enqueued).
  /// Commits the current offsets assigned to the consumer upon creation.
  /// Returns a pair consisting of the commit queue and a process which commits offsets and reacts to rebalancing.
  let periodicOffsetCommitter 
    (interval:TimeSpan) 
    (rebalance:AsyncSeq<(Partition * Offset)[]>)
    (commit:(Partition * Offset)[] -> Async<unit>) : Async<((Partition * Offset)[] -> unit) * Async<unit>> = async {
    let mb = Mb.create ()
    let queuedOffsets = AsyncSeq.replicateInfiniteAsync (Mb.take mb)
    let ticks = AsyncSeq.intervalMs (int interval.TotalMilliseconds)
    let commitProc =
      AsyncSeq.mergeChoice3 ticks queuedOffsets rebalance
      |> AsyncSeq.scanAsync (fun offsets event -> async {
        match event with
        | Choice1Of3 _ -> 
          do! commit (offsets |> Map.toArray)
          return offsets
        | Choice2Of3 queued ->
          return offsets |> Map.addMany queued
        | Choice3Of3 assigned ->          
          return assigned |> Map.ofArray
        }) Map.empty  
      |> AsyncSeq.iter ignore
    return mb.Post,commitProc }

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