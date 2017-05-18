namespace Kafunk

/// Progress information for a consumer in a group.
type ConsumerProgressInfo = {

  /// The consumer group id.
  group : GroupId

  /// The topic.
  topic : TopicName

  /// Progress info for each partition.
  partitions : ConsumerPartitionProgressInfo[]

  /// The total lag across all partitions.
  totalLag : int64

  /// The minimum lead across all partitions.
  minLead : int64

}

/// Progress information for a consumer in a group, for a specific topic-partition.
and ConsumerPartitionProgressInfo = {

  /// The partition.
  partition : Partition

  /// The consumer's current offset.
  consumerOffset : Offset

  /// The offset at the current start of the topic.
  earliestOffset : Offset

  /// The offset at the current end of the topic.
  highWatermarkOffset : Offset

  /// The distance between the high watermark offset and the consumer offset.
  lag : int64

  /// The distance between the consumer offset and the earliest offset.
  lead : int64

  /// The number of messages in the partition.
  messageCount : int64

}

/// Operations for providing consumer progress information.
[<Compile(Module)>]
module ConsumerInfo =

  /// Returns consumer progress information.
  /// Passing empty set of partitions returns information for all partitions.
  let progress (conn:KafkaConn) (groupId:GroupId) (topic:TopicName) (ps:Partition[]) = async {
    let! topicOffsets,consumerOffsets =
      Async.parallel2 (
        Offsets.offsetRange conn topic ps,
        Consumer.fetchOffsets conn groupId [|topic,ps|])
    let consumerOffsets =
      consumerOffsets
      |> Seq.collect (fun (t,os) ->
        if t = topic then os
        else [||])
      |> Map.ofSeq
    let partitions =
      (topicOffsets, consumerOffsets)
      ||> Map.mergeChoice (fun p -> function
        | Choice1Of3 ((e,l),o) ->
          // Consumer offset of -1 indicates that no consumer offset is present.  In this case, we should calculate lag as the high water mark minus earliest offset
          let lag, lead =
            match o with
            | -1L -> l - e, 0L
            | _ -> l - o, o - e
          { partition = p ; consumerOffset = o ; earliestOffset = e ; highWatermarkOffset = l ; lag = lag ; lead = lead ; messageCount = l - e }
        | Choice2Of3 (e,l) ->
          // in the event there is no consumer offset present, lag should be calculated as high watermark minus earliest
          // this prevents artifically high lags for partitions with no consumer offsets
          let o = -1L
          { partition = p ; consumerOffset = o ; earliestOffset = e ; highWatermarkOffset = l ; lag = l - e ; lead = 0L ; messageCount = l - e }
          //failwithf "unable to find consumer offset for topic=%s partition=%i" topic p
        | Choice3Of3 o -> failwithf "unable to find topic offset for topic=%s partition=%i [consumer_offset=%i]" topic p o)
      |> Seq.map (fun kvp -> kvp.Value)
      |> Seq.toArray
    return {
      topic = topic ; group = groupId ; partitions = partitions ;
      totalLag = partitions |> Seq.sumBy (fun p -> p.lag)
      minLead =
        if partitions.Length > 0 then partitions |> Seq.map (fun p -> p.lead) |> Seq.min
        else -1L } }

  /// Returns consumer progress information for the partitions currently assigned to the consumer.
  let consumerProgress (c:Consumer) = async {
    let! state = Consumer.state c
    return! progress c.conn c.config.groupId c.config.topic state.assignments }

