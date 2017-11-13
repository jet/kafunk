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

/// Information about a consumer group.
type ConsumerGroupInfo = {
  
  /// The consumer group id.
  group_id : GroupId
  
  /// The protocol.
  protocol : Protocol
  
  protocol_type : ProtocolType
  
  /// State associated with the group.
  state : State
  
  /// The members in the consumer group.
  members : ConsumerGroupMemberInfo[]
} 
  with 
    /// Returns a list of members subscribed to the specified topic.
    static member subscribedToTopic (topic:TopicName) (x:ConsumerGroupInfo) : ConsumerGroupMemberInfo[] =
      x.members
      |> Seq.choose (fun m -> 
        if m.assignments |> Seq.exists (fun (t,_) -> t = topic) then Some m
        else None)
      |> Seq.toArray
    
/// Information about a consumer group member.
and ConsumerGroupMemberInfo = {
  
  /// The unique member id.
  member_id : MemberId
  
  /// The client-provided id.
  client_id : ClientId
  
  /// The client's host name.
  client_host : ClientHost
  
  /// Member specific metadata.
  member_metadata : MemberMetadata
  
  /// The topics and partitions assigned to this member.
  assignments : (TopicName * Partition[])[]
}


/// Operations for providing consumer progress information.
[<Compile(Module)>]
module ConsumerInfo =

  /// Returns consumer progress information.
  /// Passing empty set of partitions returns information for all partitions.
  let progress (conn:KafkaConn) (groupId:GroupId) (topic:TopicName) (ps:Partition[]) = async {    
    let! partitions = 
      if ps |> Array.isEmpty then
        async {
        let! metaData = conn.GetMetadata [|topic|]
        return metaData.Item topic }
      else 
        async { return ps }

    let! topicOffsets,consumerOffsets =
      Async.parallel2 (
        Offsets.offsetRange conn topic partitions,
        Consumer.fetchOffsets conn groupId [|topic,partitions|])
        
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

  /// Returns information about all consumer groups.
  let consumerGroups (conn:KafkaConn) : Async<ConsumerGroupInfo[]> = async {
    let! res = Kafka.listGroups conn (ListGroupsRequest())  
    let groupIds = 
      res.groups 
      |> Seq.choose (fun (g,pt) -> 
        if pt = "consumer" then Some g
        else None) 
      |> Seq.toArray
    let! res = Kafka.describeGroups conn (DescribeGroupsRequest(groupIds))
    return seq {
      for (ec,gid,state,protoType,proto,gm) in res.groups do
        if ec = ErrorCode.NoError then
          let members = seq {
            for (mid,cid,ch,md,ma) in gm.members do
              let assignments = ConsumerGroup.decodeMemberAssignments ma
              yield { ConsumerGroupMemberInfo.assignments = assignments ; client_id = cid ; client_host = ch ; member_metadata = md ; member_id = mid } }
          let groupInfo = 
            { ConsumerGroupInfo.group_id = gid ; protocol = proto ; protocol_type = protoType ; state = state ; members = members |> Seq.toArray }
          yield groupInfo } |> Seq.toArray }

  /// Given topic names, returns all of the consumer groups consuming that topic.
  let consumerGroupByTopics (conn:KafkaConn) (topics:TopicName seq) : Async<Map<TopicName, ConsumerGroupInfo[]>> = async {  
    let! groups = consumerGroups conn
    let topics = set topics
    return
      groups
      |> Seq.collect (fun g -> 
        let assignedTopics = g.members |> Seq.collect (fun m -> m.assignments |> Seq.map fst) |> set
        topics
        |> Seq.choose (fun t -> 
          if Set.contains t assignedTopics then Some (t,g)
          else None))
      |> Seq.groupBy fst
      |> Seq.map (fun (t,xs) -> t, xs |> Seq.map snd |> Seq.toArray)
      |> Map.ofSeq }
