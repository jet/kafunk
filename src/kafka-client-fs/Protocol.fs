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

    let [<Literal>] None = 0uy
    let [<Literal>] GZIP = 1uy
    let [<Literal>] Snappy = 2uy

  type CompressionCodecs =
    NoCompressionCodec = 0y
    | DefaultCompressoinCodec = 1y
    | GZIPCompressionCodec = 2y
    | SnappyCompressionCodec = 3y

  type Key = ArraySeg<byte>

  type Value = ArraySeg<byte>

  type TopicName = string

  /// This field indicates how many acknowledgements the servers should receive
  /// before responding to the request.
  type RequiredAcks = int16

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module RequiredAcks =

    let None : RequiredAcks = 0s
    let Local : RequiredAcks = 1s
    let AllInSync : RequiredAcks = -1s

  /// This provides a maximum time in milliseconds the server can await the
  /// receipt of the number of acknowledgements in RequiredAcks.
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

  /// The replica id indicates the node id of the replica initiating this
  /// request. Normal client consumers should always specify this as -1.
  type ReplicaId = int32

  /// The max wait time is the maximum amount of time in milliseconds to block
  /// waiting if insufficient data is available at the time the request is
  /// issued.
  type MaxWaitTime = int32

  /// This is the minimum number of bytes of messages that must be available to
  /// give a response.
  type MinBytes = int32

  /// The offset to begin this fetch from.
  type FetchOffset = int64

  /// The maximum bytes to include in the message set for this partition. This
  /// helps bound the size of the response.
  type MaxBytes = int32

  /// The offset at the end of the log for this partition. This can be used by
  /// the client to determine how many messages behind the end of the log they
  /// are.
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
      new (crc,magicByte,attributes,key,value) =
        { crc = crc ; magicByte = magicByte ; attributes = attributes ; key = key ; value = value }
    end

  and MessageSet =
    struct
      val messages : (Offset * MessageSize * Message)[]
      new (set) = { messages = set }
    end

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
      new (topicErrorCode,topicName,partitionMetadata) =
        { topicErrorCode = topicErrorCode ; topicName = topicName ; partitionMetadata = partitionMetadata }
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

  // Group Membership API

  /// The offsets for a given consumer group are maintained by a specific
  /// broker called the group coordinator. i.e., a consumer needs to issue its
  /// offset commit and fetch requests to this specific broker. It can discover
  /// the current coordinator by issuing a group coordinator request.
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

  /// The sync group request is used by the group leader to assign state (e.g.
  /// partition assignments) to all members of the current generation. All
  /// members send SyncGroup immediately after joining the group, but only the
  /// leader provides the group's assignment.
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
      new (groupId,generationId,memberId) =
        { groupId = groupId ; generationId = generationId ; memberId = memberId }
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
      new (version,subscription,userData) =
        { version = version ; subscription = subscription ; userData = userData }
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

  type Codecs = Codecs

  let CodecsInst = Codecs

  let inline _write< ^a, ^b when (^b or ^a) : (static member write : ArraySeg<byte> * ^a -> ArraySeg<byte>)> (buf:ArraySeg<_>) (x:^a) (_:^b) =
    let buf = ((^a or ^b) : (static member write : ArraySeg<byte> -> ^a -> ArraySeg<byte>) (buf,x))
    buf

  let inline write buf x = _write buf x (CodecsInst)

  let inline _read< ^a, ^b when (^b or ^a) : (static member read : ArraySeg<byte> * ^a -> ^a * ArraySeg<byte>)> (buf:ArraySeg<_>) (x:^a) (_:^b) =
    let a,buf = ((^a or ^b) : (static member read : ArraySeg<byte> * ^a -> ^a * ArraySeg<byte>) (buf,x))
    a,buf

  let inline read buf = _read buf Unchecked.defaultof<_> (CodecsInst)

  let inline _size< ^a, ^b when (^b or ^a) : (static member size : ^a -> int)> (x:^a) (_:^b) =
    let size = ((^a or ^b) : (static member size : ^a -> int) x)
    size

  let inline size x = _size x (CodecsInst)

  let inline toArraySeg x =
    let size = size x
    let buf = ArraySeg.ofCount size
    write buf x |> ignore
    buf

  let inline writeInt8 b (buf : ArraySeg<byte>) =
    buf.Array.[buf.Offset] <- byte b
    buf |> ArraySeg.shiftOffset 1

  let inline readInt8 (buf : ArraySeg<byte>) : int8 * ArraySeg<byte> =
    let n = int8 buf.Array.[buf.Offset]
    (n, buf |> ArraySeg.shiftOffset 1)

  let inline writeInt16 (s : int16) (buf : ArraySeg<byte>) =
    BitConverter.GetBytesBigEndian(s, buf.Array, buf.Offset)
    buf |> ArraySeg.shiftOffset 2

  let inline readInt16 (buf : ArraySeg<byte>) : int16 * ArraySeg<byte> =
    let n = BitConverter.ToInt16BigEndian(buf.Array, buf.Offset)
    (n, buf |> ArraySeg.shiftOffset 2)

  let inline writeInt32 (i : int32) (buf : ArraySeg<byte>) : ArraySeg<byte> =
    BitConverter.GetBytesBigEndian(i, buf.Array, buf.Offset)
    buf |> ArraySeg.shiftOffset 4

  let inline readInt32 (buf : ArraySeg<byte>) : int32 * ArraySeg<byte> =
    let n = BitConverter.ToInt32BigEndian(buf.Array, buf.Offset)
    (n, buf |> ArraySeg.shiftOffset 4)

  let inline writeInt64 (w : int64) (buf : ArraySeg<byte>) =
    BitConverter.GetBytesBigEndian(w, buf.Array, buf.Offset)
    buf |> ArraySeg.shiftOffset 8

  let inline readInt64 (buf : ArraySeg<byte>) : int64 * ArraySeg<byte> =
    let n = BitConverter.ToInt64BigEndian(buf.Array, buf.Offset)
    (n, buf |> ArraySeg.shiftOffset 8)

  let inline writeFramedBytes (bytes : ArraySeg<byte>) buf =
    if isNull bytes.Array then
      writeInt32 -1 buf
    else
      let buf = writeInt32 bytes.Count buf
      Array.Copy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      buf |> ArraySeg.shiftOffset bytes.Count

  let inline readFramedBytes (buf : ArraySeg<byte>) : ArraySeg<byte> * ArraySeg<byte> =
    let length, buf = readInt32 buf
    if length = -1 then ArraySeg<_>(), buf
    else
      let arr = ArraySeg<_>(buf.Array, buf.Offset, length)
      (arr, buf |> ArraySeg.shiftOffset length)

  let writeString (str : string) (buf : ArraySeg<byte>) =
    if isNull str then
      writeInt16 -1s buf
    else
      let buf = writeInt16 (int16 str.Length) buf
      let read = Encoding.UTF8.GetBytes(str, 0, str.Length, buf.Array, buf.Offset)
      buf |> ArraySeg.shiftOffset read

  let readString (data : ArraySeg<byte>) : string * ArraySeg<byte> =
    let length, data = readInt16 data
    let length = int length
    if length = -1 then
      (null, data)
    else
      let str = Encoding.UTF8.GetString (data.Array, data.Offset, length)
      (str, data |> ArraySeg.shiftOffset length)

  let writeArray (arr : 'a[]) (write : 'a -> ArraySeg<byte> -> ArraySeg<byte>) (buf : ArraySeg<byte>) : ArraySeg<byte> =
    if isNull arr then
      let buf = writeInt32 -1 buf
      buf
    else
      let n = arr.Length
      let buf = writeInt32 n buf
      Array.fold (fun buf elem -> write elem buf) buf arr

  let readArray (read : ArraySeg<byte> -> 'a * ArraySeg<byte>) (buf : ArraySeg<byte>) : 'a[] * ArraySeg<byte> =
    let n = BitConverter.ToInt32BigEndian(buf.Array, buf.Offset)
    let mutable buf = buf |> ArraySeg.shiftOffset 4
    let arr = [|
      for i = 0 to n - 1 do
        let elem, buf' = read buf
        yield elem
        buf <- buf' |]
    (arr, buf)

  type Reader<'a> = ArraySeg<byte> -> 'a * ArraySeg<byte>

  type Writer<'a> = 'a -> ArraySeg<byte> -> ArraySeg<byte>

  let inline read2 (readA : Reader<'a>) (readB : Reader<'b>) (buf : ArraySeg<byte>) : ('a * 'b) * ArraySeg<byte> =
    let a, buf = readA buf
    let b, buf = readB buf
    ((a, b), buf)

  let inline read3 (readA : Reader<'a>) (readB : Reader<'b>) (readC : Reader<'c>) (buf : ArraySeg<byte>) : ('a * 'b * 'c) * ArraySeg<byte> =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    ((a, b, c), buf)

  let inline read4 (readA : Reader<'a>) (readB : Reader<'b>) (readC : Reader<'c>) (readD : Reader<'d>) (buf : ArraySeg<byte>) : ('a * 'b * 'c * 'd) * ArraySeg<byte> =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    let d, buf = readD buf
    ((a, b, c, d), buf)

  let inline write2 (writeA : Writer<'a>) (writeB : Writer<'b>) ((a, b) : ('a * 'b)) (buf : ArraySeg<byte>) : ArraySeg<byte> =
    buf |> writeA a |> writeB b

  let inline write3 (writeA : Writer<'a>) (writeB : Writer<'b>) (writeC : Writer<'c>) ((a, b, c) : ('a * 'b * 'c)) (buf : ArraySeg<byte>) : ArraySeg<byte> =
    buf |> writeA a |> writeB b |> writeC c

  let inline write4 (writeA : Writer<'a>) (writeB : Writer<'b>) (writeC : Writer<'c>) (writeD : Writer<'d>) ((a, b, c, d) : ('a * 'b * 'c * 'd)) (buf : ArraySeg<byte>) : ArraySeg<byte> =
    buf |> writeA a |> writeB b |> writeC c |> writeD d

  /// Codecs for primitive and generic types.
  type Codecs with

    // numbers

    static member inline size (_:int8) = 1
    static member inline size (_:int16) = 2
    static member inline size (_:int32) = 4
    static member inline size (_:int64) = 8

    // strings

    static member size (str:string) =
      if isNull str then 2
      else 2 + str.Length // TODO: Do we need to support non-ascii values here?

    // byte arrays

    static member inline size (bytes:ArraySeg<byte>) =
      (size bytes.Count) + bytes.Count

    // tuples

    static member inline size (x:_ * _) =
      let a,b = x in
      size a + size b

    static member inline size (x:_ * _ * _) =
      let a,b,c = x in
      size a + size b + size c

    static member inline size (x:_ * _ * _ * _) =
      let a,b,c,d = x in
      size a + size b + size c + size d

    static member inline size (x:_ * _ * _ * _ * _) =
      let a,b,c,d,e = x in
      size a + size b + size c + size d + size e

    static member inline size (x:_ * _ * _ * _ * _ * _) =
      let a,b,c,d,e,f = x in
      size a + size b + size c + size d + size e + size f

  let writeArrayNoSize (buf:ArraySeg<byte>) (arr:'a[]) (writeElem:ArraySeg<byte> -> 'a -> ArraySeg<byte>) =
    let mutable buf = buf
    for a in arr do
      buf <- writeElem buf a
    buf

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

  let getArraySize (arr:'a[]) (elementSize:'a -> int) =
    (size arr.Length) + (arr |> Array.sumBy elementSize)


  // arrays
  type Codecs with
    static member inline size (arr:'a[]) =
      getArraySize arr size

  type Message with
    static member size (m:Message) =
      size m.crc + size m.magicByte + size m.attributes + size m.key + size m.value
    static member write (buf:ArraySeg<byte>, m:Message) =
      let buf = buf |> ArraySeg.shiftOffset 4 // crc32
      let offset = buf.Offset
      let buf =
        buf
        |> writeInt8 m.magicByte
        |> writeInt8 m.attributes
        |> writeFramedBytes m.key
        |> writeFramedBytes m.value
      let crc = Crc.crc32 (buf.Array, offset, buf.Offset - offset)
      BitConverter.GetBytesBigEndian (int crc, buf.Array, offset - 4)
      buf
    static member read (data) =
      let crc,data = readInt32 data
      let offset = data.Offset
      let magicByte,data = readInt8 data
      let attrs,data = readInt8 data
      let key,data = readFramedBytes data
      let value,data = readFramedBytes data
      let crc' = int (Crc.crc32 (data.Array, offset, data.Offset - offset))
      if crc <> crc' then
        failwith (sprintf "Corrupt message data. Computed CRC32=%i received CRC32=%i" crc' crc)
      (Message(crc, magicByte, attrs, key, value), data)
    static member read (data, _:Message) =
      Message.read(data)

  type MessageSet with
    static member size (x:MessageSet) =
      x.messages |> Array.sumBy size
    static member write (buf, ms:MessageSet) =
      writeArrayNoSize buf ms.messages (fun buf elem -> write3 writeInt64 writeInt32 (fun message buf -> write buf message) elem buf)
    /// Reads a message set given the size in bytes.
    static member read (data, size:int) =
      let offset = readInt64
      let messageSize = readInt32
      let message : ArraySeg<byte> -> Message * ArraySeg<byte> = Message.read
      let set, data = readArraySize size data (read3 offset messageSize message)
      (MessageSet(set), data)

  type MetadataRequest with
    static member size (x:MetadataRequest) =
      size x.topicNames
    static member write (buf, x:MetadataRequest) =
      buf |> writeArray x.topicNames writeString

  type Broker with
    static member read (data:ArraySeg<_>, _:Broker) =
      let (nodeId, host, port), data = read3 readInt32 readString readInt32 data
      (Broker(nodeId, host, port), data)

  type PartitionMetadata with
    static member read (data:ArraySeg<_>, _:PartitionMetadata) =
      let partitionErrorCode, data = readInt16 data
      let partitionId, data = readInt32 data
      let leader, data = readInt32 data
      let replicas, data = readArray readInt32 data
      let isr, data = readArray readInt32 data
      (PartitionMetadata(partitionErrorCode, partitionId, leader, replicas, isr), data)

  type TopicMetadata with
    static member read (data:ArraySeg<_>, _:TopicMetadata) =
      let errorCode, data = readInt16 data
      let topicName, data = readString data
      let partitionMetadata, data = readArray read data
      (TopicMetadata(errorCode, topicName, partitionMetadata), data)

  type MetadataResponse with
    static member read (data:ArraySeg<_>, _:MetadataResponse) =
      let brokers, data = readArray read data
      let topicMetadata, data = readArray read data
      (MetadataResponse(brokers, topicMetadata), data)

  type ProduceRequest with
    static member size (x:ProduceRequest) =
      (size x.requiredAcks)
      + (size x.timeout)
      + (getArraySize x.topics (fun (tn,ps) -> size tn + (getArraySize ps (fun (p,mss,ms) -> (size p) + 4 + mss))))
    static member write (buf, x:ProduceRequest) =
      let writePartition =
        write3 writeInt32 writeInt32 (fun msgSet buf -> write buf msgSet)
      let writeTopic =
        write2 writeString (fun ps -> writeArray ps writePartition)
      buf
      |> writeInt16 x.requiredAcks
      |> writeInt32 x.timeout
      |> writeArray x.topics writeTopic

  type ProduceResponse with
    static member read (data, _:ProduceResponse) =
      let topics, data =
        data |> readArray (fun data ->
            let topicName, data = readString data
            let ps,data =
              data |> readArray (fun data ->
                  let partition, data = readInt32 data
                  let errorCode, data = readInt16 data
                  let offset, data = readInt64 data
                  ((partition, errorCode, offset), data))
            (topicName, ps), data)
      (ProduceResponse(topics), data)

  type FetchRequest with
    static member size (x:FetchRequest) =
      (size x.replicaId) + (size x.maxWaitTime) + (size x.minBytes) + (size x.topics)
    static member write (buf, x:FetchRequest) =
      let writePartition =
        write3 writeInt32 writeInt64 writeInt32
      let writeTopic =
        write2 writeString (fun ps -> writeArray ps writePartition)
      buf
      |> writeInt32 x.replicaId
      |> writeInt32 x.maxWaitTime
      |> writeInt32 x.minBytes
      |> writeArray x.topics writeTopic

  type FetchResponse with
    static member read (data, _:FetchResponse) =
      let topics, data =
        data |> readArray
          (fun data ->
            let topicName, data = readString data
            let ps, data =
              data |> readArray
                (fun data ->
                  let partition, data = readInt32 data
                  let errorCode, data = readInt16 data
                  let hwo, data = readInt64 data
                  let mss, data = readInt32 data
                  let ms, data = MessageSet.read (data, mss)
                  ((partition, errorCode, hwo, mss, ms), data))
            (topicName, ps), data)
      (FetchResponse(topics), data)

  type OffsetRequest with
    static member size (x:OffsetRequest) =
      (size x.replicaId) + (size x.topics)
    static member write (buf, x:OffsetRequest) =
      let writePartition =
        write3 writeInt32 writeInt64 writeInt32
      let writeTopic =
        write2 writeString (fun ps -> writeArray ps writePartition)
      buf
      |> writeInt32 x.replicaId
      |> writeArray x.topics writeTopic

  type PartitionOffsets with
    static member read (buf, _:PartitionOffsets) =
      let p,buf = readInt32 buf
      let ec,buf = readInt16 buf
      let offs,buf = readArray readInt64 buf
      (PartitionOffsets(p, ec, offs), buf)

  type OffsetResponse with
    static member read (buf, _:OffsetResponse) =
      let topics, buf = buf |> readArray (fun buf ->
        let topicName, buf = readString buf
        let partitionOffsets, buf = buf |> readArray (fun buf ->
          let partition, buf = readInt32 buf
          let errorCode, buf = readInt16 buf
          let offsets, buf = readArray readInt64 buf
          (PartitionOffsets(partition, errorCode, offsets), buf))
        ((topicName, partitionOffsets), buf))
      (OffsetResponse(topics), buf)

  type GroupCoordinatorRequest with
    static member size (x:GroupCoordinatorRequest) =
      (size x.groupId)
    static member write (buf, x:GroupCoordinatorRequest) =
      writeString x.groupId buf

  type GroupCoordinatorResponse with
    static member read (buf, _:GroupCoordinatorResponse) =
      let ec, buf = readInt16 buf
      let cid, buf = readInt32 buf
      let ch, buf = readString buf
      let cp, buf = readInt32 buf
      (GroupCoordinatorResponse(ec, cid, ch, cp), buf)

  type OffsetCommitRequest with
    static member size (x:OffsetCommitRequest) =
      (size x.consumerGroup) + (size x.consumerGroupGenerationId) + (size x.consumerId) + (size x.retentionTime) + (size x.topics)
    static member write (buf, x:OffsetCommitRequest) =
      let writePartition =
        write3 writeInt32 writeInt64 writeString
      let writeTopic =
        write2 writeString (fun ps -> writeArray ps writePartition)
      buf
      |> writeString x.consumerGroup
      |> writeInt32 x.consumerGroupGenerationId
      |> writeString x.consumerId
      |> writeInt64 x.retentionTime
      |> writeArray x.topics writeTopic

  type OffsetCommitResponse with
    static member read (buf, _:OffsetCommitResponse) =
      let topics, buf = buf |> readArray (fun buf ->
        let topicName, buf = readString buf
        let partitions, buf = buf |> readArray (fun buf ->
          let partition, buf = readInt32 buf
          let errorCode, buf = readInt16 buf
          ((partition, errorCode), buf))
        ((topicName, partitions), buf))
      (OffsetCommitResponse(topics), buf)

  type OffsetFetchRequest with
    static member size (x:OffsetFetchRequest) =
      (size x.consumerGroup) + (size x.topics)
    static member write (buf, x:OffsetFetchRequest) =
      let writeTopic =
        write2 writeString (fun ps -> writeArray ps writeInt32)
      buf
      |> writeString x.consumerGroup
      |> writeArray x.topics writeTopic

  type OffsetFetchResponse with
    static member read (buf, _:OffsetFetchResponse) =
      let topics, buf = buf |> readArray (fun buf ->
        let topicName, buf = readString buf
        let partitions, buf = buf |> readArray (fun buf ->
          let partition, buf = readInt32 buf
          let offset, buf = readInt64 buf
          let metadata, buf = readString buf
          let errorCode, buf = readInt16 buf
          ((partition, offset, metadata, errorCode), buf))
        ((topicName, partitions), buf))
      (OffsetFetchResponse(topics), buf)

  type HeartbeatRequest with
    static member size (x:HeartbeatRequest) =
      (size x.groupId) + (size x.generationId) + (size x.memberId)
    static member write (buf, x:HeartbeatRequest) =
      buf
      |> writeString x.groupId
      |> writeInt32 x.generationId
      |> writeString x.memberId

  type HeartbeatResponse with
    static member read (buf, _:HeartbeatResponse) =
      let errorCode,buf = readInt16 buf
      (HeartbeatResponse(errorCode), buf)

  type GroupProtocols with
    static member size (x:GroupProtocols) =
      (size x.protocols)
    static member write (buf, x:GroupProtocols) =
      buf |> writeArray x.protocols (write2 writeString writeFramedBytes)

  type JoinGroupRequest with
    static member size (x:JoinGroupRequest) =
      (size x.groupId) + (size x.sessionTimeout) + (size x.memberId) + (size x.protocolType) + (size x.groupProtocols)
    static member write (buf, x:JoinGroupRequest) =
      buf
      |> writeString x.groupId
      |> writeInt32 x.sessionTimeout
      |> writeString x.memberId
      |> writeString x.protocolType
      |> writeArray x.groupProtocols.protocols (write2 writeString writeFramedBytes)

  type Members with
    static member read (buf, _:Members) =
      let xs, buf = buf |> readArray (fun buf ->
        let memberId, buf = readString buf
        let memberMeta, buf = readFramedBytes buf
        ((memberId, memberMeta), buf))
      (Members(xs), buf)

  type JoinGroupResponse with
    static member read (buf, _:JoinGroupResponse) =
      let errorCode,buf = readInt16 buf
      let gid,buf = readInt32 buf
      let gp,buf = readString buf
      let lid,buf = readString buf
      let mid,buf = readString buf
      let ms,buf = read buf
      (JoinGroupResponse(errorCode, gid, gp, lid, mid, ms), buf)

  type LeaveGroupRequest with
    static member size (x:LeaveGroupRequest) =
      (size x.groupId) + (size x.memberId)
    static member write (buf, x:LeaveGroupRequest) =
      buf |> writeString x.groupId |> writeString x.memberId

  type LeaveGroupResponse with
    static member read (buf, _:LeaveGroupResponse) =
      let errorCode,buf = readInt16 buf
      (LeaveGroupResponse(errorCode), buf)

  type GroupAssignment with
    static member size (x:GroupAssignment) =
      (size x.members)
    static member write (buf, x:GroupAssignment) =
      buf |> writeArray x.members (write2 writeString writeFramedBytes)

  type SyncGroupRequest with
    static member size (x:SyncGroupRequest) =
      (size x.groupId) + (size x.generationId) + (size x.memberId) + (size x.groupAssignment)
    static member write (buf, x:SyncGroupRequest) =
      let buf =
        buf
        |> writeString x.groupId
        |> writeInt32 x.generationId
        |> writeString x.memberId
      write buf x.groupAssignment

  type SyncGroupResponse with
    static member read (buf, _:SyncGroupResponse) =
      let errorCode, buf = readInt16 buf
      let ma, buf = readFramedBytes buf
      (SyncGroupResponse(errorCode, ma), buf)

  type ListGroupsRequest with
    static member size (x:ListGroupsRequest) = 0
    static member write (buf, x:ListGroupsRequest) = buf

  type ListGroupsResponse with
    static member read (buf, _:ListGroupsResponse) =
      let errorCode,buf = readInt16 buf
      let gs, buf = buf |> readArray (fun buf ->
        let groupId, buf = readString buf
        let protoType, buf = readString buf
        ((groupId, protoType), buf))
      (ListGroupsResponse(errorCode, gs), buf)

  type DescribeGroupsRequest with
    static member size (x:DescribeGroupsRequest) =
      (size x.groupIds)
    static member write (buf, x:DescribeGroupsRequest) =
      buf |> writeArray x.groupIds writeString

  type GroupMembers with
    static member read (buf, _:GroupMembers) =
      let xs, buf = buf |> readArray (fun buf ->
        let memberId, buf = readString buf
        let clientId, buf = readString buf
        let clientHost, buf = readString buf
        let memberMeta, buf = readFramedBytes buf
        let memberAssignment, buf = readFramedBytes buf
        ((memberId, clientId, clientHost, memberMeta, memberAssignment), buf))
      (GroupMembers(xs), buf)

  type DescribeGroupsResponse with
    static member read (buf, _:DescribeGroupsResponse) =
      let xs, buf = buf |> readArray (fun buf ->
        let errorCode, buf = readInt16 buf
        let groupId, buf = readString buf
        let state, buf = readString buf
        let protoType, buf = readString buf
        let protocol, buf = readString buf
        let groupMembers, buf = read buf
        ((errorCode, groupId, state, protoType, protocol, groupMembers), buf))
      (DescribeGroupsResponse(xs), buf)

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
      let buf =
        buf
        |> writeInt16 (int16 x.apiKey)
        |> writeInt16 x.apiVersion
        |> writeInt32 x.correlationId
        |> writeString x.clientId
      write buf x.message

  type ConsumerGroupProtocolMetadata with
    static member size (x:ConsumerGroupProtocolMetadata) =
      (size x.version) + (size x.subscription) + (size x.userData)
    static member write (buf, x:ConsumerGroupProtocolMetadata) =
      buf
      |> writeInt16 x.version
      |> writeArray x.subscription writeString
      |> writeFramedBytes x.userData

  type PartitionAssignment with
    static member size (x:PartitionAssignment) =
      (size x.assignments)
    static member write (buf, x:PartitionAssignment) =
      let writePartitions partitions = writeArray partitions writeInt32
      buf |> writeArray x.assignments (write2 writeString writePartitions)
    static member read (data, _:PartitionAssignment) =
      let assignments, data = data |> readArray (fun buf ->
        let topicName, buf = readString buf
        let partitions, buf = buf |> readArray readInt32
        ((topicName, partitions), buf))
      (PartitionAssignment(assignments), data)

  type ConsumerGroupMemberAssignment with
    static member size (x:ConsumerGroupMemberAssignment) =
      (size x.version) + (size x.partitionAssignment)
    static member write (buf, x:ConsumerGroupMemberAssignment) =
      let buf = writeInt16 x.version buf
      write buf x.partitionAssignment
    static member read (data, _:ConsumerGroupMemberAssignment) =
      let version, data = readInt16 data
      let assignments, data = read data
      (ConsumerGroupMemberAssignment(version, assignments), data)