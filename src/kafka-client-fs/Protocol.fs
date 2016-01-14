namespace KafkaFs

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Text
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

/// The Kafka RPC protocol.
/// https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
[<AutoOpen>]
module Protocol =
  
  // -------------------------------------------------------------------------------------------------------------------------------------
  // Shared

  type ApiKey =
    | ProduceRequest = 0s    
    | FetchRequest = 1s
    | OffsetRequest = 2s
    | MetadataRequest = 3s
    | OffsetCommitRequest = 8s
    | OffsetFetchRequest = 9s
    | GroupCoordinatorRequest = 10s
    | JoinGroupRequest = 11s
    | HeartbeatRequest = 12s
    | LeaveGroupRequest = 13s
    | SyncGroupRequest = 14s
    | DescribeGroupsRequest = 15s
    | ListGroupsRequest = 16s

  type ApiVersion = int16

  type CorrelationId = int32

  type ClientId = string

  type Crc = int32

  type MagicByte = int8

  type Attributes = int8

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Compression =
    
    let None = 0uy
    let GZIP = 1uy
    let Snappy = 2uy

  type Key = ArraySeg<byte>

  type Value = ArraySeg<byte>

  type TopicName = string

  /// This field indicates how many acknowledgements the servers should receive before responding to the request.
  type RequiredAcks = int16
 
  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>] 
  module RequiredAcks =
    
    let None : RequiredAcks = 0s
    let Local : RequiredAcks = 1s
    let AllInSync : RequiredAcks = -1s

  /// This provides a maximum time in milliseconds the server can await the receipt of the number of acknowledgements in RequiredAcks.
  type Timeout = int32

  type Partition = int32

  /// The size, in bytes, of the message set that follows.
  type MessageSetSize = int32
     
  type MessageSize = int32

  type Offset = int64

  type NodeId = int32

  type Host = string

  type Port = int32

  type TopicErrorCode = int16

  type PartitionErrorCode = int16

  //type PartitionId = int32

  type Leader = int32

  type Replicas = int32[]

  /// In-sync replicas.
  type Isr = int32[]

  type ErrorCode = int16

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]     
  module ErrorCode =

    let [<Literal>] NoError = 0s
    let [<Literal>] Unknown = -1s
    let [<Literal>] OffsetOutOfRange = 1s
    let [<Literal>] InvalidMessage = 2s
    let [<Literal>] UnknownTopicOrPartition = 3s
    let [<Literal>] InvalidMessageSize = 4s
    let [<Literal>] LeaderNotAvailable = 5s
    let [<Literal>] NotLeaderForPartition = 6s
    let [<Literal>] RequestTimedOut = 7s
    let [<Literal>] BrokerNotAvailable = 8s
    let [<Literal>] ReplicaNotAvailable = 9s
    let [<Literal>] MessageSizeTooLarge = 9s
    let [<Literal>] StaleControllerEpochCode = 11s
    let [<Literal>] OffsetMetadataTooLargeCode = 12s
    let [<Literal>] GroupLoadInProgressCode = 14s
    let [<Literal>] GroupCoordinatorNotAvailableCode = 15s
    let [<Literal>] NotCoordinatorForGroupCode = 16s
    let [<Literal>] InvalidTopicCode = 17s
    let [<Literal>] RecordListTooLargeCode = 18s
    let [<Literal>] NotEnoughReplicasCode = 19s
    let [<Literal>] NotEnoughReplicasAfterAppendCode = 20s
    let [<Literal>] InvalidRequiredAcksCode = 21s
    let [<Literal>] IllegalGenerationCode = 22s
    let [<Literal>] InconsistentGroupProtocolCode = 23s
    let [<Literal>] InvalidGroupIdCode = 24s
    let [<Literal>] UnknownMemberIdCode = 25s
    let [<Literal>] InvalidSessionTimeoutCode = 26s
    let [<Literal>] RebalanceInProgressCode = 27s
    let [<Literal>] InvalidCommitOffsetSizeCode = 28s
    let [<Literal>] TopicAuthorizationFailedCode = 29s
    let [<Literal>] GroupAuthorizationFailedCode = 30s
    let [<Literal>] ClusterAuthorizationFailedCode = 31s

  /// The replica id indicates the node id of the replica initiating this request.
  /// Normal client consumers should always specify this as -1.
  type ReplicaId = int32

  /// The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
  type MaxWaitTime = int32

  /// This is the minimum number of bytes of messages that must be available to give a response.
  type MinBytes = int32

  /// The offset to begin this fetch from.
  type FetchOffset = int64

  /// The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
  type MaxBytes = int32

  /// The offset at the end of the log for this partition. 
  /// This can be used by the client to determine how many messages behind the end of the log they are.
  type HighwaterMarkOffset = int64

  type Time = int64

  type MaxNumberOfOffsets = int32

  type GroupId = string

  type CoordinatorId = int32

  type CoordinatorHost = string

  type CoordinatorPort = int32

  type ConsumerGroup = string

  type ConsumerGroupGenerationId = int32

  type ConsumerId = string

  type RetentionTime = int64

  type Metadata = string

  type MemberId = string

  type ProtocolName = string

  type ProtocolMetadata = ArraySeg<byte>

  type GenerationId = int32

  type GroupProtocol = string

  type LeaderId = string

  type MemberMetadata = ArraySeg<byte>

  type MemberAssignment = ArraySeg<byte>


  type Message =
    struct
      val crc : Crc
      val magicByte : MagicByte
      val attributes : Attributes
      val key : Key
      val value : Value
      new (crc,magicByte,attributes,key,value) = { crc = crc ; magicByte = magicByte ; attributes = attributes ; key = key ; value = value }
    end

  and MessageSet = 
    struct
      val messages : (Offset * MessageSize * Message)[]
      new (set) = { messages = set } 
    end
  

  // --------------------------------------------------------------------------------------------------------------
  // Metadata API

  type MetadataRequest =
    struct
      val topicNames : TopicName[]
      new (topicNames) = { topicNames = topicNames }
    end

  and MetadataResponse =
    struct
      val brokers : Broker[]
      val topicMetadata : TopicMetadata[]
      new (brokers, topicMetadata) =  { brokers = brokers ; topicMetadata = topicMetadata }
    end

  and Broker = 
    struct
      val nodeId : NodeId
      val host : Host
      val port : Port
      new (nodeId,host,port) = { nodeId = nodeId ; host = host ; port = port }
    end

  and TopicMetadata =
    struct
      val topicErrorCode : TopicErrorCode
      val topicName : TopicName
      val partitionMetadata : PartitionMetadata[]
      new (topicErrorCode,topicName,partitionMetadata) = { topicErrorCode = topicErrorCode ; topicName = topicName ; partitionMetadata = partitionMetadata }
    end

  and PartitionMetadata =
    struct
      val partitionErrorCode : PartitionErrorCode
      val partitionId : Partition
      val leader : Leader
      val replicas : Replicas
      val isr : Isr
      new (partitionErrorCode,partitionId,leader,replicas,isr) = { partitionErrorCode = partitionErrorCode ; partitionId = partitionId ; leader = leader ; replicas = replicas ; isr = isr }
    end

  
  // --------------------------------------------------------------------------------------------------------------
  // Produce API

  type ProduceRequest =
    struct
      val requiredAcks : RequiredAcks
      val timeout : Timeout
      val topics : (TopicName * (Partition * MessageSetSize * MessageSet)[])[]
      new (requiredAcks,timeout,topics) = { requiredAcks = requiredAcks ; timeout = timeout ; topics = topics }
    end

  and ProduceResponse = 
    struct
      val topics : (TopicName * (Partition * ErrorCode * Offset)[])[]
      new (topics) = { topics = topics }
    end


  // --------------------------------------------------------------------------------------------------------------
  // Fetch API

  type FetchRequest =
    struct
      val replicaId : ReplicaId
      val maxWaitTime : MaxWaitTime
      val minBytes : MinBytes
      val topics : (TopicName * (Partition * FetchOffset * MaxBytes)[])[]
      new (replicaId, maxWaitTime, minBytes, topics) = { replicaId = replicaId ; maxWaitTime = maxWaitTime ; minBytes = minBytes ; topics = topics }
    end

  and FetchResponse = 
    struct
      val topics : (TopicName * (Partition * ErrorCode * HighwaterMarkOffset * MessageSetSize * MessageSet)[])[]
      new (topics) = { topics = topics } 
    end


  // --------------------------------------------------------------------------------------------------------------
  // Offset API

  type OffsetRequest =
    struct
      val replicaId : ReplicaId
      val topics : (TopicName * (Partition * Time * MaxNumberOfOffsets)[])[]
      new (replicaId, topics) = { replicaId = replicaId ; topics = topics }
    end
  
  and OffsetResponse =
    struct
      val topics : (TopicName * PartitionOffsets[])[]
      new (topics) = { topics = topics }
    end

  and PartitionOffsets =
    struct
      val partition : Partition
      val errorCode : ErrorCode
      val offsets : Offset[]
      new (partition,errorCode,offsets) = { partition = partition ; errorCode = errorCode ; offsets = offsets }
    end
  

  // --------------------------------------------------------------------------------------------------------------
  // Offset Commit/Fetch API

  type OffsetCommitRequest =
    struct
      val consumerGroup : ConsumerGroup
      val consumerGroupGenerationId : ConsumerGroupGenerationId
      val consumerId : ConsumerId
      val retentionTime : RetentionTime
      val topics : (TopicName * (Partition * Offset * Metadata)[])[]
      new (consumerGroup,consumerGroupGenerationId,consumerId,retentionTime,topics) =
        { consumerGroup = consumerGroup ; consumerGroupGenerationId = consumerGroupGenerationId ; consumerId = consumerId ; retentionTime = retentionTime ; topics = topics }
    end

  and OffsetCommitResponse = 
    struct
      val topics : (TopicName * (Partition * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end


  type OffsetFetchRequest =
    struct
      val consumerGroup : ConsumerGroup
      val topics : (TopicName * Partition[])[]
      new (consumerGroup, topics) = { consumerGroup = consumerGroup ; topics = topics }
    end

  and OffsetFetchResponse =
    struct
      val topics : (TopicName * (Partition * Offset * Metadata * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end


  // --------------------------------------------------------------------------------------------------------------
  // Group Membership API

  /// The offsets for a given consumer group are maintained by a specific broker called the group coordinator. i.e., a consumer needs to issue its offset commit and fetch requests to this specific broker. 
  /// It can discover the current coordinator by issuing a group coordinator request.
  type GroupCoordinatorRequest =
    struct
      val groupId : GroupId
      new (groupId) = { groupId = groupId }
    end

  and GroupCoordinatorResponse =
    struct
      val errorCode : ErrorCode
      val coordinatorId : CoordinatorId
      val coordinatorHost : CoordinatorHost
      val coordinatorPort : CoordinatorPort
      new (errorCode,coordinatorId,coordinatorHost,coordinatorPort) = 
        { errorCode = errorCode ; coordinatorId = coordinatorId ; coordinatorHost = coordinatorHost ; coordinatorPort = coordinatorPort }
    end


  /// The join group request is used by a client to become a member of a group.
  type JoinGroupRequest =
    struct
      val groupId : GroupId
      val sessionTimeout : SessionTimeout
      val memberId : MemberId
      val protocolType : ProtocolType
      val groupProtocols : GroupProtocols
      new (groupId,sessionTimeout,memberId,protocolType,groupProtocols) =
        { groupId = groupId ; sessionTimeout = sessionTimeout ; memberId = memberId ; protocolType = protocolType ; groupProtocols = groupProtocols }
    end

  and SessionTimeout = int32

  and GroupProtocols =
    struct
      val protocols : (ProtocolName * ProtocolMetadata)[]
      new (protocols) = { protocols = protocols }
    end

  and JoinGroupResponse =
    struct
      val errorCode : ErrorCode
      val generationId : GenerationId
      val groupProtocol : GroupProtocol
      val leaderId : LeaderId
      val memberId : MemberId
      val members : Members
      new (errorCode,generationId,groupProtocol,leaderId,memberId,members) = 
        { errorCode = errorCode ; generationId = generationId ; groupProtocol = groupProtocol ; leaderId = leaderId ; memberId = memberId ; members = members }
    end

  and Members =
    struct
      val members : (MemberId * MemberMetadata)[]
      new (members) = { members = members }
    end

  and ProtocolType = string

  /// The sync group request is used by the group leader to assign state (e.g. partition assignments) to all members of the current generation.
  /// All members send SyncGroup immediately after joining the group, but only the leader provides the group's assignment.
  type SyncGroupRequest =
    struct
      val groupId : GroupId
      val generationId : GenerationId
      val memberId : MemberId
      val groupAssignment : GroupAssignment
      new (groupId, generationId, memberId, groupAssignment) = 
        { groupId = groupId ; generationId = generationId ; memberId = memberId ; groupAssignment = groupAssignment }
    end

  and GroupAssignment =
    struct
      val members : (MemberId * MemberAssignment)[]
      new (members) = { members = members }
    end

  and SyncGroupResponse =
    struct
      val errorCode : ErrorCode
      val memberAssignment : MemberAssignment
      new (errorCode,memberAssignment) = { errorCode = errorCode ; memberAssignment = memberAssignment }
    end


  type HeartbeatRequest =
    struct
      val groupId : GroupId
      val generationId : GenerationId
      val memberId : MemberId
      new (groupId,generationId,memberId) = { groupId = groupId ; generationId = generationId ; memberId = memberId }
    end

  and HeartbeatResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end


  type LeaveGroupRequest =
    struct
      val groupId : GroupId
      val memberId : MemberId
      new (groupId,memberId) = { groupId = groupId ; memberId = memberId }
    end

  and LeaveGroupResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end


  // --------------------------------------------------------------------------------------------------------------  
  // Consumer groups
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
  // http://people.apache.org/~nehanarkhede/kafka-0.9-consumer-javadoc/doc/  

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module ProtocolType =
    
    let consumer = "consumer"
    
  
  /// ProtocolMetadata for the consumer group protocol.
  type ConsumerGroupProtocolMetadata =
    struct
      val version : Version
      val subscription : TopicName[]
      val userData : UserData
      new (version,subscription,userData) = { version = version ; subscription = subscription ; userData = userData }
    end

  and Version = int16

  /// User data sent as part of protocol metadata.
  and UserData = ArraySeg<byte>

  and AssignmentStrategy = string

  /// MemberAssignment for the consumer group protocol.
  /// Each member in the group will receive the assignment from the leader in the sync group response. 
  type ConsumerGroupMemberAssignment =
    struct
      val version : Version
      val partitionAssignment : PartitionAssignment
      new (version,partitionAssignment) = { version = version ; partitionAssignment = partitionAssignment }
    end

  and PartitionAssignment =
    struct
      val assignments : (TopicName * Partition[])[]
      new (assignments) = { assignments = assignments }
    end


  // --------------------------------------------------------------------------------------------------------------   
  // Administrative API

  type ListGroupsRequest =
    struct
    end
  
  and ListGroupsResponse =
    struct
      val errorCode : ErrorCode
      val groups : (GroupId * ProtocolType)[]
      new (errorCode,groups) = { errorCode = errorCode ; groups = groups }
    end

  
  type DescribeGroupsRequest =
    struct
      val groupIds : GroupId[]
      new (groupIds) = { groupIds = groupIds }
    end

  and DescribeGroupsResponse =
    struct
      val groups : (ErrorCode * GroupId * State * ProtocolType * Protocol * GroupMembers)[]
      new (groups) = { groups = groups }
    end 

  and GroupMembers =
    struct
      val members : (MemberId * ClientId * ClientHost * MemberMetadata * MemberAssignment)[]
      new (members) = { members = members }
    end

  and State = string

  and Protocol = string

  and ClientHost = string



  // --------------------------------------------------------------------------------------------------------------   
  // Envelopes

  /// A Kafka request envelope.
  type Request =
    struct
      val apiKey : ApiKey
      val apiVersion : ApiVersion
      val correlationId : CorrelationId
      val clientId : ClientId
      val message : RequestMessage
      new (apiVersion,correlationId,clientId,message:RequestMessage) = 
        { apiKey = message.ApiKey ; apiVersion = apiVersion ; correlationId = correlationId ; clientId = clientId ; message = message }
    end

  /// A Kafka request message.
  and RequestMessage =     
    | Metadata of MetadataRequest
    | Fetch of FetchRequest
    | Produce of ProduceRequest
    | Offset of OffsetRequest
    | GroupCoordinator of GroupCoordinatorRequest
    | OffsetCommit of OffsetCommitRequest
    | OffsetFetch of OffsetFetchRequest
    | JoinGroup of JoinGroupRequest
    | SyncGroup of SyncGroupRequest
    | Heartbeat of HeartbeatRequest
    | LeaveGroup of LeaveGroupRequest
    | ListGroups of ListGroupsRequest
    | DescribeGroups of DescribeGroupsRequest
  with
    member x.ApiKey =
      match x with      
      | Metadata _ -> ApiKey.MetadataRequest
      | Fetch _ -> ApiKey.FetchRequest
      | Produce _ -> ApiKey.ProduceRequest
      | Offset _ -> ApiKey.OffsetRequest
      | GroupCoordinator _ -> ApiKey.GroupCoordinatorRequest
      | OffsetCommit _ -> ApiKey.OffsetCommitRequest
      | OffsetFetch _ -> ApiKey.OffsetFetchRequest
      | JoinGroup _ -> ApiKey.JoinGroupRequest
      | SyncGroup _ -> ApiKey.SyncGroupRequest
      | Heartbeat _ -> ApiKey.HeartbeatRequest
      | LeaveGroup _ -> ApiKey.LeaveGroupRequest
      | ListGroups _ -> ApiKey.ListGroupsRequest
      | DescribeGroups _ -> ApiKey.DescribeGroupsRequest


  /// A Kafka response envelope.
  type Response =
    struct
      val correlationId : CorrelationId
      val message : ResponseMessage
      new (correlationId,message) = { correlationId = correlationId ; message = message }
    end

  /// A Kafka response message.
  and ResponseMessage =    
    | MetadataResponse of MetadataResponse
    | FetchResponse of FetchResponse
    | ProduceResponse of ProduceResponse
    | OffsetResponse of OffsetResponse
    | GroupCoordinatorResponse of GroupCoordinatorResponse
    | OffsetCommitResponse of OffsetCommitResponse
    | OffsetFetchResponse of OffsetFetchResponse
    | JoinGroupResponse of JoinGroupResponse
    | SyncGroupResponse of SyncGroupResponse
    | HeartbeatResponse of HeartbeatResponse
    | LeaveGroupResponse of LeaveGroupResponse
    | ListGroupsResponse of ListGroupsResponse
    | DescribeGroupsResponse of DescribeGroupsResponse
//  with
//    member x.ErrorCodes =
//      match x with
//      | ResponseMessage.MetadataResponse x -> 
//        x.topicMetadata 
//        |> Seq.choose (fun t -> seq {
//          if t.topicErrorCode <> ErrorCode.NoError then 
//            yield Some t.topicErrorCode  
//          yield!
//            t.partitionMetadata
//            |> Seq.choose (fun p ->
//              if p.partitionErrorCode <> ErrorCode.NoError then Some t.partitionErrorCode
//              else None)
//        })
//      | _ -> Seq.empty

  // -------------------------------------------------------------------------------------------------------------------------------------








  // -------------------------------------------------------------------------------------------------------------------------------------
  // codecs

  type Codecs = Codecs

  let CodecsInst = Codecs

  let inline _write< ^a, ^b when (^a or ^b) : (static member write : ArraySeg<byte> * ^a -> ArraySeg<byte>)> (buf:ArraySeg<_>) (x:^a) (_:^b) =
    let buf = ((^a or ^b) : (static member write : ArraySeg<byte> -> ^a -> ArraySeg<byte>) (buf,x))
    buf

  let inline write buf x = _write buf x (CodecsInst)

  let inline _read< ^a, ^b when (^a or ^b) : (static member read : ArraySeg<byte> * ^a -> ^a * ArraySeg<byte>)> (buf:ArraySeg<_>) (x:^a) (_:^b) =
    let a,buf = ((^a or ^b) : (static member read : ArraySeg<byte> * ^a -> ^a * ArraySeg<byte>) (buf,x))
    a,buf

  let inline read buf = _read buf Unchecked.defaultof<_> (CodecsInst)

  let inline _size< ^a, ^b when (^a or ^b) : (static member size : ^a -> int)> (x:^a) (_:^b) =
    let size = ((^a or ^b) : (static member size : ^a -> int) x)
    size

  let inline size x = _size x (CodecsInst)

  let inline toArraySeg x =
    let size = size x
    let buf = ArraySeg.ofCount size
    write buf x |> ignore
    buf


  /// Codecs for primitive and generic types.
  type Codecs with
    
    // numbers

    static member write (buf:ArraySeg<byte>, x:int8) =
      buf.Array.[buf.Offset] <- byte x
      buf |> ArraySeg.shiftOffset 1
    static member read (data:ArraySeg<byte>, _:int8) =
      let n = int8 data.Array.[data.Offset]
      n, data |> ArraySeg.shiftOffset 1
    static member inline size (_:int8) = 1

    static member write (buf:ArraySeg<byte>, x:int16) =
      BitConverter.GetBytesBigEndian(x, buf.Array, buf.Offset)
      buf |> ArraySeg.shiftOffset 2
    static member read (data:ArraySeg<byte>, _:int16) =
      let n = BitConverter.ToInt16BigEndian (data.Array, data.Offset)
      n, data |> ArraySeg.shiftOffset 2
    static member inline size (_:int16) = 2
    
    static member write (buf:ArraySeg<byte>, x:int32) =
      BitConverter.GetBytesBigEndian(x, buf.Array, buf.Offset)
      buf |> ArraySeg.shiftOffset 4
    static member read (data:ArraySeg<byte>, _:int32) =
      let n = BitConverter.ToInt32BigEndian (data.Array, data.Offset)
      n, data |> ArraySeg.shiftOffset 4
    static member inline size (_:int32) = 4
    
    static member write (buf:ArraySeg<byte>, x:int64) =
      BitConverter.GetBytesBigEndian(x, buf.Array, buf.Offset)
      buf |> ArraySeg.shiftOffset 8
    static member read (data:ArraySeg<byte>, _:int64) =
      let n = BitConverter.ToInt64BigEndian (data.Array, data.Offset)
      n, data |> ArraySeg.shiftOffset 8
    static member inline size (_:int64) = 8
       
    
    // strings

    static member size (str:string) =
      if isNull str then 2
      else 2 + str.Length
    static member write (buf:ArraySeg<byte>, str:string) =
      if isNull str then
        write buf (int16 -1)
      else
        let buf = write buf (int16 str.Length)
        let read = Encoding.UTF8.GetBytes(str, 0, str.Length, buf.Array, buf.Offset)
        buf |> ArraySeg.shiftOffset read
    static member read (data:ArraySeg<byte>, _:string) =
      let (length:int16),data = read data
      let length = int length
      if length = -1 then null,data
      else
        let str = Encoding.UTF8.GetString (data.Array, data.Offset, length)
        str, data |> ArraySeg.shiftOffset length


    // byte arrays

    static member write (buf, bytes:ArraySeg<byte>) =
      if isNull bytes.Array then
        let buf = write buf -1
        buf
      else
        let buf = write buf bytes.Count
        Array.Copy (bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
        buf |> ArraySeg.shiftOffset bytes.Count
    static member read (data:ArraySeg<byte>, _:ArraySeg<byte>) =
      let length,data = read data
      if length = -1 then ArraySeg<_>(),data
      else 
        let arr = ArraySeg<_>(data.Array, data.Offset, length)
        arr,data |> ArraySeg.shiftOffset length
    static member inline size (bytes:ArraySeg<byte>) =
      (size bytes.Count) + bytes.Count


    // tuples

    static member inline write (data:ArraySeg<byte>, x:'a * 'b) =
      let a,b = x in
      let data = write data a
      let data = write data b
      data
    static member inline read (buf:ArraySeg<byte>, _:_ * _) =
      let a,buf = read buf
      let b,buf = read buf
      (a,b),buf
    static member inline size (x:_ * _) =
      let a,b = x in
      size a + size b

    static member inline write (data:ArraySeg<byte>, x:_ * _ * _) =
      let a,b,c = x in
      let data = write data a
      let data = write data b
      let data = write data c
      data
    static member inline read (buf:ArraySeg<byte>, _:_ * _ * _) =
      let a,buf = read buf
      let b,buf = read buf
      let c,buf = read buf
      (a,b,c),buf
    static member inline size (x:_ * _ * _) =
      let a,b,c = x in
      size a + size b + size c

    static member inline write (data:ArraySeg<byte>, x:_ * _ * _ * _) =
      let a,b,c,d = x in
      let data = write data a
      let data = write data b
      let data = write data c
      let data = write data d
      data
    static member inline read (buf:ArraySeg<byte>, _:_ * _ * _ * _) =
      let a,buf = read buf
      let b,buf = read buf
      let c,buf = read buf
      let d,buf = read buf
      (a,b,c,d),buf
    static member inline size (x:_ * _ * _ * _) =
      let a,b,c,d = x in
      size a + size b + size c + size d

    static member inline write (data:ArraySeg<byte>, x:_ * _ * _ * _ * _) =
      let a,b,c,d,e = x in
      let data = write data a
      let data = write data b
      let data = write data c
      let data = write data d
      let data = write data e
      data
    static member inline read (buf:ArraySeg<byte>, _:_ * _ * _ * _ * _) =
      let a,buf = read buf
      let b,buf = read buf
      let c,buf = read buf
      let d,buf = read buf
      let e,buf = read buf
      (a,b,c,d,e),buf
    static member inline size (x:_ * _ * _ * _ * _) =
      let a,b,c,d,e = x in
      size a + size b + size c + size d + size e

    static member inline write (data:ArraySeg<byte>, x:_ * _ * _ * _ * _ * _) =
      let a,b,c,d,e,f = x in
      let data = write data a
      let data = write data b
      let data = write data c
      let data = write data d
      let data = write data e
      let data = write data f
      data
    static member inline read (buf:ArraySeg<byte>, _:_ * _ * _ * _ * _ * _) =
      let a,buf = read buf
      let b,buf = read buf
      let c,buf = read buf
      let d,buf = read buf
      let e,buf = read buf
      let f,buf = read buf
      (a,b,c,d,e,f),buf
    static member inline size (x:_ * _ * _ * _ * _ * _) =
      let a,b,c,d,e,f = x in
      size a + size b + size c + size d + size e + size f
            



  let writeArrayNoSize (buf:ArraySeg<byte>) (arr:'a[]) (writeElem:ArraySeg<byte> -> 'a -> ArraySeg<byte>) =
    let mutable buf = buf
    for a in arr do
      buf <- writeElem buf a
    buf

  let writeArray (buf:ArraySeg<byte>) (arr:'a[]) (writeElem:ArraySeg<byte> -> 'a -> ArraySeg<byte>) =
    if isNull arr then
      let buf = write buf -1
      buf
    else
      let n = arr.Length
      let mutable buf = write buf n
      writeArrayNoSize buf arr writeElem

  let readArraySize (size:int) (data:ArraySeg<byte>) (readElem:ArraySeg<byte> -> 'a * ArraySeg<byte>) =
    let mutable data = data
    let mutable read = 0
    let arr = [|
      while read < size do
        let elem,data' = readElem data
        yield elem
        read <- read + (data'.Offset - data.Offset)
        data <- data' |]
    arr,data

  let readArray (data:ArraySeg<byte>) (readElem:ArraySeg<byte> -> 'a * ArraySeg<byte>) =
    let n = BitConverter.ToInt32BigEndian (data.Array, data.Offset)
    let mutable data = data |> ArraySeg.shiftOffset 4
    let arr = [|
      for i = 0 to n - 1 do
        let message,data' = readElem data
        yield message
        data <- data' |]
    arr,data

  let getArraySize (arr:'a[]) (elementSize:'a -> int) =
    (size arr.Length) + (arr |> Array.sumBy elementSize)




  type Codecs with

    // arrays

    static member inline read (data:ArraySeg<byte>, _:'a[]) : 'a[] * ArraySeg<byte> =
      let n = BitConverter.ToInt32BigEndian (data.Array, data.Offset)
      let mutable data = data |> ArraySeg.shiftOffset 4
      let arr = [|
        for i = 0 to n - 1 do
          let message,data' = read data
          yield message
          data <- data' |]
      arr,data
    static member inline write (data:ArraySeg<byte>, arr:'a[]) =
      writeArray data arr write
    static member inline size (arr:'a[]) =
      getArraySize arr size
  


  type Message with
    static member size (m:Message) =
      size m.crc + size m.magicByte + size m.attributes + size m.key + size m.value
    static member write (buf:ArraySeg<byte>, m:Message) =      
      let buf = buf |> ArraySeg.shiftOffset 4 // crc32
      let offset = buf.Offset
      let buf = write buf m.magicByte
      let buf = write buf m.attributes
      let buf = write buf m.key
      let buf = write buf m.value
      let crc = Crc.crc32 (buf.Array, offset, buf.Offset - offset)
      BitConverter.GetBytesBigEndian (int crc, buf.Array, offset - 4)
      buf
    static member read (data, _:Message) =
      let crc,data = read data
      let offset = data.Offset
      let magicByte,data = read data
      let attrs,data = read data
      let key,data = read data
      let value,data = read data
      let crc' = int (Crc.crc32 (data.Array, offset, data.Offset - offset))
      if crc <> crc' then
        failwith (sprintf "Corrupt message data. Computed CRC32=%i received CRC32=%i" crc' crc)
      Message(crc,magicByte,attrs,key,value),data
      

  type MessageSet with
    static member size (x:MessageSet) =
      x.messages |> Array.sumBy size
    static member write (buf, ms:MessageSet) =
      writeArrayNoSize buf ms.messages 
        (fun buf (o,ms,m) ->
          let buf = write buf o
          let buf = write buf ms
          let buf = write buf m
          buf)
    /// Reads a message set given the size in bytes.
    static member read (data, size:int) =
      let set,data =
        readArraySize size data
          (fun data ->
            let os,data = read data
            let ms,data = read data
            let m,data = read data
            (os,ms,m),data)
      MessageSet(set),data


        



  type MetadataRequest with
    static member size (x:MetadataRequest) = 
      size x.topicNames
    static member write (buf, x:MetadataRequest) = 
      write buf x.topicNames
          
  type Broker with
    static member read (data:ArraySeg<_>, _:Broker) =
      let nodeId,data = read data
      let host,data = read data
      let port,data = read data
      Broker(nodeId,host,port),data

  type PartitionMetadata with
    static member read (data:ArraySeg<_>, _:PartitionMetadata) =
      let partitionErrorCode,data = read data
      let partitionId,data = read data
      let leader,data = read data
      let replicas,data = read data
      let isr,data = read data
      PartitionMetadata(partitionErrorCode,partitionId,leader,replicas,isr),data

  type TopicMetadata with
    static member read (data:ArraySeg<_>, _:TopicMetadata) =
      let errorCode,data = read data
      let topicName,data = read data
      let partitionMetadata,data = readArray data read
      TopicMetadata(errorCode,topicName,partitionMetadata),data

  type MetadataResponse with
    static member read (data:ArraySeg<_>, _:MetadataResponse) =
      let brokers,data = read data
      let topicMetadata,data = read data
      MetadataResponse(brokers,topicMetadata) ,data


 

  type ProduceRequest with
    static member size (x:ProduceRequest) =
      (size x.requiredAcks) 
      + (size x.timeout)
      + (getArraySize x.topics (fun (tn,ps) -> size tn + (getArraySize ps (fun (p,mss,ms) -> (size p) + 4 + mss))))
    static member write (buf, x:ProduceRequest) =
      let buf = write buf x.requiredAcks
      let buf = write buf x.timeout
      let buf =
        writeArray buf x.topics 
          (fun buf (tn,ps) ->
            let buf = write buf tn
            let buf = 
              writeArray buf ps 
                (fun buf (p,mss,ms) -> 
                  let buf = write buf p
                  let buf = write buf mss
                  let buf = write buf ms
                  buf)
            buf)
      buf
      
  type ProduceResponse with
    static member read (data, _:ProduceResponse) =
      let topics,data =
        readArray data
          (fun data ->
            let topicName,data = read data
            let ps,data =
              readArray data
                (fun data ->
                  let partition,data = read data
                  let errorCode,data = read data
                  let offset,data = read data
                  (partition,errorCode,offset),data)
            (topicName,ps),data) 
      ProduceResponse(topics),data

 

  type FetchRequest with
    static member size (x:FetchRequest) =
      (size x.replicaId) + (size x.maxWaitTime) + (size x.minBytes) + (size x.topics)
    static member write (buf, x:FetchRequest) =
      let buf = write buf x.replicaId
      let buf = write buf x.maxWaitTime
      let buf = write buf x.minBytes
      let buf = write buf x.topics
      buf

  type FetchResponse with
    static member read (data, _:FetchResponse) =
      let topics,data =
        readArray data 
          (fun data ->
            let topicName,data = read data
            let ps,data =
              readArray data
                (fun data ->
                  let partition,data = read data
                  let errorCode,data = read data
                  let hwo,data = read data
                  let mss,data = read data
                  let ms,data = MessageSet.read (data,mss)
                  (partition,errorCode,hwo,mss,ms),data)
            (topicName,ps),data)
      FetchResponse(topics),data

      

  type OffsetRequest with
    static member size (x:OffsetRequest) =
      (size x.replicaId) + (size x.topics)
    static member write (buf, x:OffsetRequest) =
      let buf = write buf x.replicaId
      let buf = write buf x.topics
      buf

  type PartitionOffsets with
    static member read (buf, _:PartitionOffsets) =
      let p,buf = read buf
      let ec,buf = read buf
      let offs,buf = read buf
      PartitionOffsets(p,ec,offs),buf

  type OffsetResponse with
    static member read (buf, _:OffsetResponse) =
      let topics,buf = read buf
      OffsetResponse(topics),buf



  type GroupCoordinatorRequest with
    static member size (x:GroupCoordinatorRequest) =
      (size x.groupId)
    static member write (buf, x:GroupCoordinatorRequest) =
      let buf = write buf x.groupId
      buf

  type GroupCoordinatorResponse with
    static member read (buf, _:GroupCoordinatorResponse) =
      let ec,buf = read buf
      let cid,buf = read buf
      let ch,buf = read buf
      let cp,buf = read buf
      GroupCoordinatorResponse(ec,cid,ch,cp),buf



  type OffsetCommitRequest with
    static member size (x:OffsetCommitRequest) =
      (size x.consumerGroup) + (size x.consumerGroupGenerationId) + (size x.consumerId) + (size x.retentionTime) + (size x.topics)
    static member write (buf, x:OffsetCommitRequest) =
      let buf = write buf x.consumerGroup
      let buf = write buf x.consumerGroupGenerationId
      let buf = write buf x.consumerId
      let buf = write buf x.retentionTime
      let buf = write buf x.topics
      buf

  type OffsetCommitResponse with
    static member read (buf, _:OffsetCommitResponse) =
      let topics,buf = read buf
      OffsetCommitResponse(topics),buf


  type OffsetFetchRequest with
    static member size (x:OffsetFetchRequest) =
      (size x.consumerGroup) + (size x.topics)
    static member write (buf, x:OffsetFetchRequest) =
      let buf = write buf x.consumerGroup
      let buf = write buf x.topics
      buf

  type OffsetFetchResponse with
    static member read (buf, _:OffsetFetchResponse) =
      let topics,buf = read buf
      OffsetFetchResponse(topics),buf
  


  type HeartbeatRequest with
    static member size (x:HeartbeatRequest) =
      (size x.groupId) + (size x.generationId) + (size x.memberId)
    static member write (buf, x:HeartbeatRequest) =
      let buf = write buf x.groupId
      let buf = write buf x.generationId
      let buf = write buf x.memberId
      buf

  type HeartbeatResponse with
    static member read (buf, _:HeartbeatResponse) =
      let errorCode,buf = read buf
      HeartbeatResponse(errorCode),buf



  type GroupProtocols with
    static member size (x:GroupProtocols) =
      (size x.protocols)
    static member write (buf, x:GroupProtocols) =
      let buf = write buf x.protocols
      buf
    
  type JoinGroupRequest with
    static member size (x:JoinGroupRequest) =
      (size x.groupId) + (size x.sessionTimeout) + (size x.memberId) + (size x.protocolType) + (size x.groupProtocols)
    static member write (buf, x:JoinGroupRequest) =
      let buf = write buf x.groupId
      let buf = write buf x.sessionTimeout
      let buf = write buf x.memberId
      let buf = write buf x.protocolType
      let buf = write buf x.groupProtocols
      buf

  type Members with
    static member read (buf, _:Members) =
      let xs,buf = read buf
      Members(xs),buf

  type JoinGroupResponse with
    static member read (buf, _:JoinGroupResponse) =
      let errorCode,buf = read buf
      let gid,buf = read buf
      let gp,buf = read buf
      let lid,buf = read buf
      let mid,buf = read buf
      let ms,buf = read buf
      JoinGroupResponse(errorCode,gid,gp,lid,mid,ms),buf
      

  type LeaveGroupRequest with
    static member size (x:LeaveGroupRequest) =
      (size x.groupId) + (size x.memberId)
    static member write (buf, x:LeaveGroupRequest) =
      let buf = write buf x.groupId
      let buf = write buf x.memberId
      buf
  
  type LeaveGroupResponse with
    static member read (buf, _:LeaveGroupResponse) =
      let errorCode,buf = read buf
      LeaveGroupResponse(errorCode),buf


  type GroupAssignment with
    static member size (x:GroupAssignment) =
      (size x.members)
    static member write (buf, x:GroupAssignment) =
      let buf = write buf x.members
      buf

  type SyncGroupRequest with
    static member size (x:SyncGroupRequest) =
      (size x.groupId) + (size x.generationId) + (size x.memberId) + (size x.groupAssignment)
    static member write (buf, x:SyncGroupRequest) =
      let buf = write buf x.groupId
      let buf = write buf x.generationId
      let buf = write buf x.memberId
      let buf = write buf x.groupAssignment
      buf
  
  type SyncGroupResponse with
    static member read (buf, _:SyncGroupResponse) =
      let errorCode,buf = read buf
      let ma,buf = read buf
      SyncGroupResponse(errorCode,ma),buf


  type ListGroupsRequest with
    static member size (x:ListGroupsRequest) = 0
    static member write (buf, x:ListGroupsRequest) = buf
  
  type ListGroupsResponse with
    static member read (buf, _:ListGroupsResponse) =
      let errorCode,buf = read buf
      let gs,buf = read buf
      ListGroupsResponse(errorCode,gs),buf


  type DescribeGroupsRequest with
    static member size (x:DescribeGroupsRequest) = 
      (size x.groupIds)
    static member write (buf, x:DescribeGroupsRequest) = 
      let buf = write buf x.groupIds
      buf
  
  type GroupMembers with
    static member read (buf, _:GroupMembers) =
      let xs,buf = read buf
      GroupMembers(xs),buf    

  type DescribeGroupsResponse with
    static member read (buf, _:DescribeGroupsResponse) =
      let xs,buf = read buf
      DescribeGroupsResponse(xs),buf




  type RequestMessage with
    static member size (x:RequestMessage) =
      match x with
      | Heartbeat x -> size x
      | Metadata x -> size x
      | Fetch x -> size x
      | Produce x -> size x
      | Offset x -> size x
      | GroupCoordinator x -> size x
      | OffsetCommit x -> size x
      | OffsetFetch x -> size x
      | JoinGroup x -> size x
      | SyncGroup x -> size x
      | LeaveGroup x -> size x
      | ListGroups x -> size x
      | DescribeGroups x -> size x

    static member write (buf, x:RequestMessage) =
      match x with
      | Heartbeat x -> write buf x
      | Metadata x -> write buf x
      | Fetch x -> write buf x
      | Produce x -> write buf x
      | Offset x -> write buf x
      | GroupCoordinator x -> write buf x
      | OffsetCommit x -> write buf x
      | OffsetFetch x -> write buf x
      | JoinGroup x -> write buf x
      | SyncGroup x -> write buf x
      | LeaveGroup x -> write buf x
      | ListGroups x -> write buf x
      | DescribeGroups x -> write buf x


  type ResponseMessage with

    /// Decodes the response given the specified ApiKey corresponding to the request.
    static member inline readApiKey (buf:ArraySeg<byte>, apiKey:ApiKey) : ResponseMessage =
      match apiKey with
      | ApiKey.HeartbeatRequest -> let x,buf = read buf in (ResponseMessage.HeartbeatResponse x)
      | ApiKey.MetadataRequest -> let x,buf = read buf in (ResponseMessage.MetadataResponse x)
      | ApiKey.FetchRequest -> let x,buf = read buf in (ResponseMessage.FetchResponse x)
      | ApiKey.ProduceRequest -> let x,buf = read buf in (ResponseMessage.ProduceResponse x)
      | ApiKey.OffsetRequest -> let x,buf = read buf in (ResponseMessage.OffsetResponse x)
      | ApiKey.GroupCoordinatorRequest -> let x,buf = read buf in (ResponseMessage.GroupCoordinatorResponse x)
      | ApiKey.OffsetCommitRequest -> let x,buf = read buf in (ResponseMessage.OffsetCommitResponse x)
      | ApiKey.OffsetFetchRequest -> let x,buf = read buf in (ResponseMessage.OffsetFetchResponse x)
      | ApiKey.JoinGroupRequest -> let x,buf = read buf in (ResponseMessage.JoinGroupResponse x)
      | ApiKey.SyncGroupRequest -> let x,buf = read buf in (ResponseMessage.SyncGroupResponse x)
      | ApiKey.LeaveGroupRequest -> let x,buf = read buf in (ResponseMessage.LeaveGroupResponse x)
      | ApiKey.ListGroupsRequest -> let x,buf = read buf in (ResponseMessage.ListGroupsResponse x)
      | ApiKey.DescribeGroupsRequest -> let x,buf = read buf in (ResponseMessage.DescribeGroupsResponse x)
      | x -> failwith (sprintf "Unsupported ApiKey=%A" x)



  type Request with
    static member size (x:Request) =
      (size (int16 x.apiKey)) + (size x.apiVersion) + (size x.correlationId) + (size x.clientId) + (size x.message)
    static member inline write (buf, x:Request) =
      let buf = write buf (int16 x.apiKey)
      let buf = write buf x.apiVersion
      let buf = write buf x.correlationId
      let buf = write buf x.clientId
      let buf = write buf x.message
      buf
      
  // -------------------------------------------------------------------------------------------------------------------------------------

  type ConsumerGroupProtocolMetadata with
    static member size (x:ConsumerGroupProtocolMetadata) =
      (size x.version) + (size x.subscription) + (size x.userData)
    static member write (buf, x:ConsumerGroupProtocolMetadata) =
      let buf = write buf x.version
      let buf = write buf x.subscription
      let buf = write buf x.userData
      buf

  type PartitionAssignment with
    static member size (x:PartitionAssignment) =
      (size x.assignments)
    static member write (buf, x:PartitionAssignment) =
      let buf = write buf x.assignments
      buf
    static member read (data, _:PartitionAssignment) =
      let assignments,data = read data
      PartitionAssignment(assignments),data

  type ConsumerGroupMemberAssignment with
    static member size (x:ConsumerGroupMemberAssignment) =
      (size x.version) + (size x.partitionAssignment)
    static member write (buf, x:ConsumerGroupMemberAssignment) =
      let buf = write buf x.version
      let buf = write buf x.partitionAssignment
      buf
    static member read (data, _:ConsumerGroupMemberAssignment) =
      let version,data = read data
      let assignments,data = read data
      ConsumerGroupMemberAssignment(version,assignments),data