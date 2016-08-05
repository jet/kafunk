namespace Kafunk

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

  /// A correlation id of a Kafka request-response transaction.
  type CorrelationId = int32

  /// A client id.
  type ClientId = string

  /// Crc digest of a Kafka message.
  type Crc = int32

  type MagicByte = int8

  /// Kafka message attributes.
  type Attributes = int8

  module CompressionCodec =

    [<Literal>]
    let Mask = 7uy

    [<Literal>]
    let None = 0uy

    [<Literal>]
    let GZIP = 1uy

    [<Literal>]
    let Snappy = 2uy

  /// A Kafka message key (bytes).
  type Key = Binary.Segment

  /// A Kafka message value (bytes).
  type Value = Binary.Segment

  /// A name of a Kafka topic.
  type TopicName = string

  /// This field indicates how many acknowledgements the servers should receive
  /// before responding to the request.
  type RequiredAcks = int16

  /// Required acks options.
  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module RequiredAcks =

    /// No acknoweldgement required.
    let None : RequiredAcks = 0s

    /// Acknowledged after the destination broker acknowledges.
    let Local : RequiredAcks = 1s

    /// Acknowledged after all in-sync replicas acknowledges.
    let AllInSync : RequiredAcks = -1s

  /// This provides a maximum time in milliseconds the server can await the
  /// receipt of the number of acknowledgements in RequiredAcks.
  type Timeout = int32

  type Partition = int32

  /// The size, in bytes, of the message set that follows.
  type MessageSetSize = int32

  /// The size of a Kafka message.
  type MessageSize = int32

  /// A Kafka topic offset.
  type Offset = int64

  /// An id of a Kafka node.
  type NodeId = int32

  /// A Kafka host name.
  type Host = string

  /// A Kafka host port number
  type Port = int32

  /// A Kafka error code.
  type ErrorCode = int16

  type TopicErrorCode = ErrorCode

  type PartitionErrorCode = ErrorCode

  /// The id of the leader node.
  type Leader = NodeId

  /// Node ids of replicas.
  type Replicas = NodeId[]

  /// Node ids of in-sync replicas.
  type Isr = NodeId[]

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module ErrorCode =

    [<Literal>]
    let NoError = 0s

    [<Literal>]
    let Unknown = -1s

    [<Literal>]
    let OffsetOutOfRange = 1s

    [<Literal>]
    let InvalidMessage = 2s

    [<Literal>]
    let UnknownTopicOrPartition = 3s

    [<Literal>]
    let InvalidMessageSize = 4s

    [<Literal>]
    let LeaderNotAvailable = 5s

    [<Literal>]
    let NotLeaderForPartition = 6s

    [<Literal>]
    let RequestTimedOut = 7s

    [<Literal>]
    let BrokerNotAvailable = 8s

    [<Literal>]
    let ReplicaNotAvailable = 9s

    [<Literal>]
    let MessageSizeTooLarge = 9s

    [<Literal>]
    let StaleControllerEpochCode = 11s

    [<Literal>]
    let OffsetMetadataTooLargeCode = 12s

    [<Literal>]
    let GroupLoadInProgressCode = 14s

    [<Literal>]
    let GroupCoordinatorNotAvailableCode = 15s

    [<Literal>]
    let NotCoordinatorForGroupCode = 16s

    [<Literal>]
    let InvalidTopicCode = 17s

    [<Literal>]
    let RecordListTooLargeCode = 18s

    [<Literal>]
    let NotEnoughReplicasCode = 19s

    [<Literal>]
    let NotEnoughReplicasAfterAppendCode = 20s

    [<Literal>]
    let InvalidRequiredAcksCode = 21s

    [<Literal>]
    let IllegalGenerationCode = 22s

    [<Literal>]
    let InconsistentGroupProtocolCode = 23s

    [<Literal>]
    let InvalidGroupIdCode = 24s

    [<Literal>]
    let UnknownMemberIdCode = 25s

    [<Literal>]
    let InvalidSessionTimeoutCode = 26s

    [<Literal>]
    let RebalanceInProgressCode = 27s

    [<Literal>]
    let InvalidCommitOffsetSizeCode = 28s

    [<Literal>]
    let TopicAuthorizationFailedCode = 29s

    [<Literal>]
    let GroupAuthorizationFailedCode = 30s

    [<Literal>]
    let ClusterAuthorizationFailedCode = 31s

    let isError (ec:ErrorCode) = 
      ec <> NoError

    /// Determines whether the error code indicates a stable condition
    /// which will not change and should not be retried.
    let isStable (ec:ErrorCode) =
      match ec with
      | NoError -> true
      | Unknown -> true
      | MessageSizeTooLarge -> true
      | OffsetMetadataTooLargeCode -> true
      | RecordListTooLargeCode -> true
      | InvalidRequiredAcksCode -> true
      | IllegalGenerationCode -> true
      | _ -> false
      


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

  /// A Kafka group id.
  type GroupId = string

  /// A Kafka group coordinator id.
  type CoordinatorId = int32

  /// A Kafka group coordinator host name.
  type CoordinatorHost = Host

  /// A Kafka group coordinator TCP port.
  type CoordinatorPort = Port

  ///
  type ConsumerGroup = string

  type ConsumerGroupGenerationId = int32

  type ConsumerId = string

  type RetentionTime = int64

  type Meta = string

  type MemberId = string

  type ProtocolName = string

  type ProtocolMetadata = Binary.Segment

  /// An id of a Kafka group protocol generation.
  type GenerationId = int32

  type GroupProtocol = string

  /// The id of a group leader.
  type LeaderId = string

  /// Metadata associated with a Kafka group member.
  type MemberMetadata = Binary.Segment

  /// A byte[] representing member assignment of a particular Kafka group protocol.
  type MemberAssignment = Binary.Segment

  // TODO: Finish moving the static members inline with the type definition.
  // It's a lot easier to deal with having the definition in one place. It
  // could be that we auto-generate this code from the type definition alone
  // so this could remove a bunch of the obvious code below. Some of these
  // will still need some functions to properly calculate things like crc.
  // NOTE: I really don't like that we've conflated our wire types with
  // what we expose in the API. An example is the CRC32 checksum on messages.
  // These should be properly calculated on serialization or checked on read
  // yet we opt-for representing it in our type rather than the protocol's
  // octet stream exclusively. </rant> I don't have easy answers so my
  // current strategy is to reduce the exposed surface area of certain
  // modules.

  /// A Kafka message type used for producing and fetching.
  type Message =
    struct
      val crc : Crc
      val magicByte : MagicByte
      val attributes : Attributes
      val key : Key
      val value : Value
      new (crc, magicByte, attributes, key, value) =
        { crc = crc; magicByte = magicByte; attributes = attributes; key = key; value = value }
    end
  with

    static member size (m:Message) =
      Binary.sizeInt32 m.crc +
      Binary.sizeInt8 m.magicByte +
      Binary.sizeInt8 m.attributes +
      Binary.sizeBytes m.key +
      Binary.sizeBytes m.value

    static member write (m:Message) buf =
      let crcBuf = buf
      let buf = crcBuf |> Binary.shiftOffset 4
      let offset = buf.Offset
      let buf =
        buf
        |> Binary.writeInt8 m.magicByte
        |> Binary.writeInt8 m.attributes
        |> Binary.writeBytes m.key
        |> Binary.writeBytes m.value
      let crc = Crc.crc32 buf.Array offset (buf.Offset - offset)
      // We're sharing the array backing both buffers here.
      crcBuf |> Binary.writeInt32 (int crc) |> ignore
      buf

    static member read buf =
      let crc, buf = Binary.readInt32 buf
      let offset = buf.Offset
      let magicByte, buf = Binary.readInt8 buf
      let attrs, buf = Binary.readInt8 buf
      let key, buf = Binary.readBytes buf
      let value, buf = Binary.readBytes buf
      let crc' = int <| Crc.crc32 buf.Array offset (buf.Offset - offset)
      if crc <> crc' then
        failwithf "Corrupt message data. Computed CRC32=%i received CRC32=%i" crc' crc
      (Message(crc, magicByte, attrs, key, value), buf)

  type MessageSet =
    struct
      val messages : (Offset * MessageSize * Message)[]
      new (set) = { messages = set }
    end
  with

    static member size (x:MessageSet) =
      x.messages |> Array.sumBy (fun (offset, messageSize, message) ->
        Binary.sizeInt64 offset + Binary.sizeInt32 messageSize + Message.size message)

    static member write (ms:MessageSet) buf =
      Binary.writeArrayNoSize buf ms.messages (
        Binary.write3 Binary.writeInt64 Binary.writeInt32 Message.write)

    /// Reads a message set given the size in bytes.
    static member read size buf =
      let offset = Binary.readInt64
      let messageSize = Binary.readInt32
      let message : Binary.Segment -> Message * Binary.Segment = Message.read
      let set, buf = Binary.readArrayByteSize size buf (Binary.read3 offset messageSize message)
      (MessageSet(set), buf)

  // Metadata API
  module Metadata =

    /// Request metadata on all or a specific set of topics.
    /// Can be routed to any node in the bootstrap list.
    type Request =
      struct
        val topicNames : TopicName[]
        new (topicNames) = { topicNames = topicNames }
      end

    let sizeRequest (x:Request) =
        Binary.sizeArray x.topicNames Binary.sizeString

    let writeRequest (x:Request) =
        Binary.writeArray x.topicNames Binary.writeString

  /// A Kafka broker consists of a node id, host name and TCP port.
  type Broker =
    struct
      val nodeId : NodeId
      val host : Host
      val port : Port
      new (nodeId, host, port) = { nodeId = nodeId; host = host; port = port }
    end
  with

    static member read buf =
      let (nodeId, host, port), buf = Binary.read3 Binary.readInt32 Binary.readString Binary.readInt32 buf
      (Broker(nodeId, host, port), buf)

  type PartitionMetadata =
    struct
      val partitionErrorCode : PartitionErrorCode
      val partitionId : Partition
      val leader : Leader
      val replicas : Replicas
      val isr : Isr
      new (partitionErrorCode, partitionId, leader, replicas, isr) =
        { partitionErrorCode = partitionErrorCode; partitionId = partitionId;
          leader = leader; replicas = replicas; isr = isr }
    end
  with

    static member read buf =
      let partitionErrorCode, buf = Binary.readInt16 buf
      let partitionId, buf = Binary.readInt32 buf
      let leader, buf = Binary.readInt32 buf
      let replicas, buf = Binary.readArray Binary.readInt32 buf
      let isr, buf = Binary.readArray Binary.readInt32 buf
      (PartitionMetadata(partitionErrorCode, partitionId, leader, replicas, isr), buf)

  /// Metadata for a specific topic consisting of a set of partition-to-broker assignments.
  type TopicMetadata =
    struct
      val topicErrorCode : TopicErrorCode
      val topicName : TopicName
      val partitionMetadata : PartitionMetadata[]
      new (topicErrorCode, topicName, partitionMetadata) =
        { topicErrorCode = topicErrorCode; topicName = topicName; partitionMetadata = partitionMetadata }
    end
  with

    static member read buf =
      let errorCode, buf = Binary.readInt16 buf
      let topicName, buf = Binary.readString buf
      let partitionMetadata, buf = Binary.readArray PartitionMetadata.read buf
      (TopicMetadata(errorCode, topicName, partitionMetadata), buf)

  /// Contains a list of all brokers (node id, host, post) and assignment of topic/partitions to brokers.
  /// The assignment consists of a leader, a set of replicas and a set of in-sync replicas.
  /// - UnknownTopic
  /// - LeaderNotAvailable
  /// - InvalidTopic
  /// - TopicAuthorizationFailed
  type MetadataResponse =
    struct
      val brokers : Broker[]
      val topicMetadata : TopicMetadata[]
      new (brokers, topicMetadata) =  { brokers = brokers; topicMetadata = topicMetadata }
    end
  with

    static member read buf =
      let brokers, buf = Binary.readArray Broker.read buf
      let topicMetadata, buf = Binary.readArray TopicMetadata.read buf
      (MetadataResponse(brokers, topicMetadata), buf)

  // Produce API

  type ProduceRequest =
    struct
      val requiredAcks : RequiredAcks
      val timeout : Timeout
      val topics : (TopicName * (Partition * MessageSetSize * MessageSet)[])[]
      new (requiredAcks, timeout, topics) =
        { requiredAcks = requiredAcks; timeout = timeout; topics = topics }
    end
  with

    static member size (x:ProduceRequest) =
      let sizePartition (p, mss, _ms) =
        Binary.sizeInt32 p + 4 + mss
      let sizeTopic (tn, ps) =
        Binary.sizeString tn + Binary.sizeArray ps sizePartition
      Binary.sizeInt16 x.requiredAcks + Binary.sizeInt32 x.timeout + Binary.sizeArray x.topics sizeTopic

    static member write (x:ProduceRequest) buf =
      let writePartition =
        Binary.write3 Binary.writeInt32 Binary.writeInt32 MessageSet.write
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps writePartition)
      buf
      |> Binary.writeInt16 x.requiredAcks
      |> Binary.writeInt32 x.timeout
      |> Binary.writeArray x.topics writeTopic

  /// A reponse to a produce request.
  /// - UnknownTopicOrPartition
  /// - InvalidMessageSize
  /// - LeaderNotAvailable
  /// - NotLeaderForPartition
  /// - RequestTimedOut
  /// - MessageSizeTooLarge
  /// - RecordListTooLargeCode
  /// - NotEnoughReplicasCode
  /// - NotEnoughReplicasAfterAppendCode
  /// - InvalidRequiredAcksCode
  /// - TopicAuthorizationFailedCode
  and ProduceResponse =
    struct
      val topics : (TopicName * (Partition * ErrorCode * Offset)[])[]
      new (topics) = { topics = topics }
    end
  with

    static member read buf =
      let readPartition =
        Binary.read3 Binary.readInt32 Binary.readInt16 Binary.readInt64
      let readTopic =
        Binary.read2 Binary.readString (Binary.readArray readPartition)
      let topics, buf = buf |> Binary.readArray readTopic
      (ProduceResponse(topics), buf)

  // Fetch API

  type FetchRequest =
    struct
      val replicaId : ReplicaId
      val maxWaitTime : MaxWaitTime
      val minBytes : MinBytes
      val topics : (TopicName * (Partition * FetchOffset * MaxBytes)[])[]
      new (replicaId, maxWaitTime, minBytes, topics) =
        { replicaId = replicaId; maxWaitTime = maxWaitTime; minBytes = minBytes; topics = topics }
    end
  with

    static member size (x:FetchRequest) =
      let partitionSize (partition, offset, maxBytes) =
        Binary.sizeInt32 partition + Binary.sizeInt64 offset + Binary.sizeInt32 maxBytes
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeInt32 x.replicaId +
      Binary.sizeInt32 x.maxWaitTime +
      Binary.sizeInt32 x.minBytes +
      Binary.sizeArray x.topics topicSize

    static member write (x:FetchRequest) buf =
      let writePartition =
        Binary.write3 Binary.writeInt32 Binary.writeInt64 Binary.writeInt32
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps writePartition)
      buf
      |> Binary.writeInt32 x.replicaId
      |> Binary.writeInt32 x.maxWaitTime
      |> Binary.writeInt32 x.minBytes
      |> Binary.writeArray x.topics writeTopic

  type PartitionFetchMetadata = Partition * ErrorCode * HighwaterMarkOffset * MessageSetSize * MessageSet

  type FetchResponse =
    struct
      val topics : (TopicName * PartitionFetchMetadata[])[]
      new (topics) = { topics = topics }
    end
  with

    static member read buf =
      let readPartition buf =
        let partition, buf = Binary.readInt32 buf
        let errorCode, buf = Binary.readInt16 buf
        let hwo, buf = Binary.readInt64 buf
        let mss, buf = Binary.readInt32 buf
        let ms, buf = MessageSet.read mss buf
        ((partition, errorCode, hwo, mss, ms), buf)
      let readTopic =
        Binary.read2 Binary.readString (Binary.readArray readPartition)
      let topics, buf = buf |> Binary.readArray readTopic
      (FetchResponse(topics), buf)

  // Offset API

  type PartitionOffsets =
    struct
      val partition : Partition
      val errorCode : ErrorCode
      val offsets : Offset[]
      new (partition, errorCode, offsets) =
        { partition = partition; errorCode = errorCode; offsets = offsets }
    end
  with

    static member read buf =
      let p, buf = Binary.readInt32 buf
      let ec, buf = Binary.readInt16 buf
      let offs, buf = Binary.readArray Binary.readInt64 buf
      (PartitionOffsets(p, ec, offs), buf)

  /// A request to return offset information for a set of topics on a specific replica.
  type OffsetRequest =
    struct
      val replicaId : ReplicaId
      val topics : (TopicName * (Partition * Time * MaxNumberOfOffsets)[])[]
      new (replicaId, topics) = { replicaId = replicaId; topics = topics }
    end
  with

    static member size (x:OffsetRequest) =
      let partitionSize (part, time, maxNumOffsets) =
        Binary.sizeInt32 part + Binary.sizeInt64 time + Binary.sizeInt32 maxNumOffsets
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeInt32 x.replicaId + Binary.sizeArray x.topics topicSize

    static member write (x:OffsetRequest) buf =
      let writePartition =
        Binary.write3 Binary.writeInt32 Binary.writeInt64 Binary.writeInt32
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps writePartition)
      buf
      |> Binary.writeInt32 x.replicaId
      |> Binary.writeArray x.topics writeTopic

  type OffsetResponse =
    struct
      val topics : (TopicName * PartitionOffsets[])[]
      new (topics) = { topics = topics }
    end
  with

    static member read buf =
      let readPartition buf =
        let (partition, errorCode, offsets), buf =
          buf |> Binary.read3 Binary.readInt32 Binary.readInt16 (Binary.readArray Binary.readInt64)
        (PartitionOffsets(partition, errorCode, offsets), buf)
      let readTopic =
        Binary.read2 Binary.readString (Binary.readArray readPartition)
      let topics, buf = buf |> Binary.readArray readTopic
      (OffsetResponse(topics), buf)

  // Offset Commit/Fetch API

  type OffsetCommitRequest =
    struct
      val consumerGroup : ConsumerGroup
      val consumerGroupGenerationId : ConsumerGroupGenerationId
      val consumerId : ConsumerId
      val retentionTime : RetentionTime
      val topics : (TopicName * (Partition * Offset * Meta)[])[]
      new (consumerGroup, consumerGroupGenerationId, consumerId, retentionTime, topics) =
        { consumerGroup = consumerGroup; consumerGroupGenerationId = consumerGroupGenerationId;
          consumerId = consumerId; retentionTime = retentionTime; topics = topics }
    end
  with

    static member size (x:OffsetCommitRequest) =
      let partitionSize (part, offset, metadata) =
        Binary.sizeInt32 part + Binary.sizeInt64 offset + Binary.sizeString metadata
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeString x.consumerGroup +
      Binary.sizeInt32 x.consumerGroupGenerationId +
      Binary.sizeString x.consumerId +
      Binary.sizeInt64 x.retentionTime +
      Binary.sizeArray x.topics topicSize

    static member write (x:OffsetCommitRequest) buf =
      let writePartition =
        Binary.write3 Binary.writeInt32 Binary.writeInt64 Binary.writeString
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps writePartition)
      buf
      |> Binary.writeString x.consumerGroup
      |> Binary.writeInt32 x.consumerGroupGenerationId
      |> Binary.writeString x.consumerId
      |> Binary.writeInt64 x.retentionTime
      |> Binary.writeArray x.topics writeTopic

  type OffsetCommitResponse =
    struct
      val topics : (TopicName * (Partition * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end
  with

    static member read buf =
      let readPartition =
        Binary.read2 Binary.readInt32 Binary.readInt16
      let readTopic =
        Binary.read2 Binary.readString (Binary.readArray readPartition)
      let topics, buf = buf |> Binary.readArray readTopic
      (OffsetCommitResponse(topics), buf)

  type OffsetFetchRequest =
    struct
      val consumerGroup : ConsumerGroup
      val topics : (TopicName * Partition[])[]
      new (consumerGroup, topics) = { consumerGroup = consumerGroup; topics = topics }
    end
  with

    static member size (x:OffsetFetchRequest) =
      let topicSize (name, parts) =
        Binary.sizeString name + Binary.sizeArray parts Binary.sizeInt32
      Binary.sizeString x.consumerGroup + Binary.sizeArray x.topics topicSize

    static member write (x:OffsetFetchRequest) buf =
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps Binary.writeInt32)
      buf
      |> Binary.writeString x.consumerGroup
      |> Binary.writeArray x.topics writeTopic

  type OffsetFetchResponse =
    struct
      val topics : (TopicName * (Partition * Offset * Meta * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end
  with

    static member read buf =
      let readPartition =
        Binary.read4 Binary.readInt32 Binary.readInt64 Binary.readString Binary.readInt16
      let readTopic =
        Binary.read2 Binary.readString (Binary.readArray readPartition)
      let topics, buf = buf |> Binary.readArray readTopic
      (OffsetFetchResponse(topics), buf)

  // Group Membership API

  /// The offsets for a given consumer group are maintained by a specific
  /// broker called the group coordinator. i.e., a consumer needs to
  /// issue its offset commit and fetch requests to this specific broker.
  /// It can discover the current coordinator by issuing a group coordinator request.
  /// Can be routed to any node in the bootstrap list.
  type GroupCoordinatorRequest =
    struct
      val groupId : GroupId
      new (groupId) = { groupId = groupId }
    end
  with

    static member size (x:GroupCoordinatorRequest) =
      Binary.sizeString x.groupId

    static member write (x:GroupCoordinatorRequest) buf =
      Binary.writeString x.groupId buf

  type GroupCoordinatorResponse =
    struct
      val errorCode : ErrorCode
      val coordinatorId : CoordinatorId
      val coordinatorHost : CoordinatorHost
      val coordinatorPort : CoordinatorPort
      new (errorCode, coordinatorId, coordinatorHost, coordinatorPort) =
        { errorCode = errorCode; coordinatorId = coordinatorId; coordinatorHost = coordinatorHost;
          coordinatorPort = coordinatorPort }
    end
  with

    static member read buf =
      let ec, buf = Binary.readInt16 buf
      let cid, buf = Binary.readInt32 buf
      let ch, buf = Binary.readString buf
      let cp, buf = Binary.readInt32 buf
      (GroupCoordinatorResponse(ec, cid, ch, cp), buf)

  type SessionTimeout = int32

  type ProtocolType = string

  type GroupProtocols =
    struct
      val protocols : (ProtocolName * ProtocolMetadata)[]
      new (protocols) = { protocols = protocols }
    end
  with

    static member size (x:GroupProtocols) =
      let protocolSize (name, metadata) =
        Binary.sizeString name + Binary.sizeBytes metadata
      Binary.sizeArray x.protocols protocolSize

    static member write (x:GroupProtocols) buf =
      buf |> Binary.writeArray x.protocols (Binary.write2 Binary.writeString Binary.writeBytes)

  type Members =
    struct
      val members : (MemberId * MemberMetadata)[]
      new (members) = { members = members }
    end
  with

    static member read buf =
      let readMember =
        Binary.read2 Binary.readString Binary.readBytes
      let xs, buf = buf |> Binary.readArray readMember
      (Members(xs), buf)

  module JoinGroup =

    [<Struct>]
    type Request =
      val groupId : GroupId
      val sessionTimeout : SessionTimeout
      val memberId : MemberId
      val protocolType : ProtocolType
      val groupProtocols : GroupProtocols
      new (groupId, sessionTimeout, memberId, protocolType, groupProtocols) =
        { groupId = groupId; sessionTimeout = sessionTimeout; memberId = memberId;
          protocolType = protocolType; groupProtocols = groupProtocols }

    [<Struct>]
    type Response =
      val errorCode : ErrorCode
      val generationId : GenerationId
      val groupProtocol : GroupProtocol
      val leaderId : LeaderId
      val memberId : MemberId
      val members : Members
      new (errorCode, generationId, groupProtocol, leaderId, memberId, members) =
        { errorCode = errorCode; generationId = generationId; groupProtocol = groupProtocol;
          leaderId = leaderId; memberId = memberId; members = members }

    let sizeRequest (req:Request) =
      Binary.sizeString req.groupId +
      Binary.sizeInt32 req.sessionTimeout +
      Binary.sizeString req.memberId +
      Binary.sizeString req.protocolType +
      GroupProtocols.size req.groupProtocols

    let writeRequest (req:Request) =
      Binary.writeString req.groupId
      >> Binary.writeInt32 req.sessionTimeout
      >> Binary.writeString req.memberId
      >> Binary.writeString req.protocolType
      >> GroupProtocols.write req.groupProtocols

    let readResponse buf =
      let (errorCode, gid, gp, lid, mid, ms), buf =
        buf |> Binary.read6
          Binary.readInt16
          Binary.readInt32
          Binary.readString
          Binary.readString
          Binary.readString
          Members.read
      (Response(errorCode, gid, gp, lid, mid, ms), buf)

  type GroupAssignment =
    struct
      val members : (MemberId * MemberAssignment)[]
      new (members) = { members = members }
    end
  with

    static member size (x:GroupAssignment) =
      Binary.sizeArray x.members (fun (memId, memAssign) -> Binary.sizeString memId + Binary.sizeBytes memAssign)

    static member write (x:GroupAssignment) buf =
      buf |> Binary.writeArray x.members (Binary.write2 Binary.writeString Binary.writeBytes)

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
        { groupId = groupId; generationId = generationId; memberId = memberId; groupAssignment = groupAssignment }
    end
  with

    static member size (x:SyncGroupRequest) =
      Binary.sizeString x.groupId +
      Binary.sizeInt32 x.generationId +
      Binary.sizeString x.memberId +
      GroupAssignment.size x.groupAssignment

    static member write (x:SyncGroupRequest) buf =
      let buf =
        buf
        |> Binary.writeString x.groupId
        |> Binary.writeInt32 x.generationId
        |> Binary.writeString x.memberId
      GroupAssignment.write x.groupAssignment buf

  type SyncGroupResponse =
    struct
      val errorCode : ErrorCode
      val memberAssignment : MemberAssignment
      new (errorCode, memberAssignment) = { errorCode = errorCode; memberAssignment = memberAssignment }
    end
  with

    static member read buf =
      let errorCode, buf = Binary.readInt16 buf
      let ma, buf = Binary.readBytes buf
      (SyncGroupResponse(errorCode, ma), buf)

  /// Sent by a consumer to the group coordinator.
  type HeartbeatRequest =
    struct
      val groupId : GroupId
      val generationId : GenerationId
      val memberId : MemberId
      new (groupId, generationId, memberId) =
        { groupId = groupId; generationId = generationId; memberId = memberId }
    end
  with

    static member size (x:HeartbeatRequest) =
      Binary.sizeString x.groupId + Binary.sizeInt32 x.generationId + Binary.sizeString x.memberId

    static member write (x:HeartbeatRequest) buf =
      buf
      |> Binary.writeString x.groupId
      |> Binary.writeInt32 x.generationId
      |> Binary.writeString x.memberId

  /// Heartbeat response from the group coordinator.
  /// - GROUP_COORDINATOR_NOT_AVAILABLE
  /// - ILLEGAL_GENERATION
  /// - UNKNOWN_MEMBER_ID
  /// - REBALANCE_IN_PROGRESS
  /// - GROUP_AUTHORIZATION_FAILED
  type HeartbeatResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end
  with

    static member read buf =
      let errorCode, buf = Binary.readInt16 buf
      (HeartbeatResponse(errorCode), buf)

  /// An explciti request to leave a group. Preferred over session timeout.
  type LeaveGroupRequest =
    struct
      val groupId : GroupId
      val memberId : MemberId
      new (groupId, memberId) = { groupId = groupId; memberId = memberId }
    end
  with

    static member size (x:LeaveGroupRequest) =
      Binary.sizeString x.groupId + Binary.sizeString x.memberId

    static member write (x:LeaveGroupRequest) buf =
      buf |> Binary.writeString x.groupId |> Binary.writeString x.memberId

  type LeaveGroupResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end
  with

    static member read buf =
      let errorCode, buf = Binary.readInt16 buf
      (LeaveGroupResponse(errorCode), buf)

  // Consumer groups
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
  // http://people.apache.org/~nehanarkhede/kafka-0.9-consumer-javadoc/doc/

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module ProtocolType =

    let consumer = "consumer"

  type Version = int16

  /// User data sent as part of protocol metadata.
  type UserData = Binary.Segment

  /// ProtocolMetadata for the consumer group protocol.
  type ConsumerGroupProtocolMetadata =
    struct
      val version : Version
      val subscription : TopicName[]
      val userData : UserData
      new (version, subscription, userData) =
        { version = version; subscription = subscription; userData = userData }
    end
  with

    static member size (x:ConsumerGroupProtocolMetadata) =
      Binary.sizeInt16 x.version +
      Binary.sizeArray x.subscription Binary.sizeString +
      Binary.sizeBytes x.userData

    static member write (x:ConsumerGroupProtocolMetadata) buf =
      buf
      |> Binary.writeInt16 x.version
      |> Binary.writeArray x.subscription Binary.writeString
      |> Binary.writeBytes x.userData

  type AssignmentStrategy = string

  type PartitionAssignment =
    struct
      val assignments : (TopicName * Partition[])[]
      new (assignments) = { assignments = assignments }
    end
  with

    static member size (x:PartitionAssignment) =
      let topicSize (name, parts) =
        Binary.sizeString name + Binary.sizeArray parts Binary.sizeInt32
      Binary.sizeArray x.assignments topicSize

    static member write (x:PartitionAssignment) buf =
      let writePartitions partitions = Binary.writeArray partitions Binary.writeInt32
      buf |> Binary.writeArray x.assignments (Binary.write2 Binary.writeString writePartitions)

    static member read buf =
      let assignments, buf = buf |> Binary.readArray (fun buf ->
        let topicName, buf = Binary.readString buf
        let partitions, buf = buf |> Binary.readArray Binary.readInt32
        ((topicName, partitions), buf))
      (PartitionAssignment(assignments), buf)

  /// MemberAssignment for the consumer group protocol.
  /// Each member in the group will receive the assignment from the leader in the sync group response.
  type ConsumerGroupMemberAssignment =
    struct
      val version : Version
      val partitionAssignment : PartitionAssignment
      new (version, partitionAssignment) = { version = version; partitionAssignment = partitionAssignment }
    end
  with

    static member size (x:ConsumerGroupMemberAssignment) =
      Binary.sizeInt16 x.version + PartitionAssignment.size x.partitionAssignment

    static member write (x:ConsumerGroupMemberAssignment) buf =
      let buf = Binary.writeInt16 x.version buf
      PartitionAssignment.write x.partitionAssignment buf

    static member read buf =
      let version, buf = Binary.readInt16 buf
      let assignments, buf = PartitionAssignment.read buf
      (ConsumerGroupMemberAssignment(version, assignments), buf)

  // Administrative API

  type ListGroupsRequest =
    struct
    end
  with

    static member size (_:ListGroupsRequest) = 0

    static member write (_:ListGroupsRequest) buf = buf

  type ListGroupsResponse =
    struct
      val errorCode : ErrorCode
      val groups : (GroupId * ProtocolType)[]
      new (errorCode, groups) = { errorCode = errorCode; groups = groups }
    end
  with

    static member read buf =
      let readGroup =
        Binary.read2 Binary.readString Binary.readString
      let errorCode, buf = Binary.readInt16 buf
      let gs, buf = buf |> Binary.readArray readGroup
      (ListGroupsResponse(errorCode, gs), buf)

  type State = string

  type Protocol = string

  type ClientHost = string

  type GroupMembers =
    struct
      val members : (MemberId * ClientId * ClientHost * MemberMetadata * MemberAssignment)[]
      new (members) = { members = members }
    end
  with

    static member read buf =
      let readGroupMember =
        Binary.read5 Binary.readString Binary.readString Binary.readString Binary.readBytes Binary.readBytes
      let xs, buf = buf |> Binary.readArray readGroupMember
      (GroupMembers(xs), buf)

  type DescribeGroupsRequest =
    struct
      val groupIds : GroupId[]
      new (groupIds) = { groupIds = groupIds }
    end
  with

    static member size (x:DescribeGroupsRequest) =
      Binary.sizeArray x.groupIds Binary.sizeString

    static member write (x:DescribeGroupsRequest) buf =
      buf |> Binary.writeArray x.groupIds Binary.writeString


  type DescribeGroupsResponse =
    struct
      val groups : (ErrorCode * GroupId * State * ProtocolType * Protocol * GroupMembers)[]
      new (groups) = { groups = groups }
    end
  with

    static member read buf =
      let readGroup =
        Binary.read6
          Binary.readInt16
          Binary.readString
          Binary.readString
          Binary.readString
          Binary.readString
          GroupMembers.read
      let xs, buf = buf |> Binary.readArray readGroup
      (DescribeGroupsResponse(xs), buf)

  /// A Kafka request message.
  type RequestMessage =
    | Metadata of Metadata.Request
    | Fetch of FetchRequest
    | Produce of ProduceRequest
    | Offset of OffsetRequest
    | GroupCoordinator of GroupCoordinatorRequest
    | OffsetCommit of OffsetCommitRequest
    | OffsetFetch of OffsetFetchRequest
    | JoinGroup of JoinGroup.Request
    | SyncGroup of SyncGroupRequest
    | Heartbeat of HeartbeatRequest
    | LeaveGroup of LeaveGroupRequest
    | ListGroups of ListGroupsRequest
    | DescribeGroups of DescribeGroupsRequest
  with

    static member size (x:RequestMessage) =
      match x with
      | Heartbeat x -> HeartbeatRequest.size x
      | Metadata x -> Metadata.sizeRequest x
      | Fetch x -> FetchRequest.size x
      | Produce x -> ProduceRequest.size x
      | Offset x -> OffsetRequest.size x
      | GroupCoordinator x -> GroupCoordinatorRequest.size x
      | OffsetCommit x -> OffsetCommitRequest.size x
      | OffsetFetch x -> OffsetFetchRequest.size x
      | JoinGroup x -> JoinGroup.sizeRequest x
      | SyncGroup x -> SyncGroupRequest.size x
      | LeaveGroup x -> LeaveGroupRequest.size x
      | ListGroups x -> ListGroupsRequest.size x
      | DescribeGroups x -> DescribeGroupsRequest.size x

    static member write (x:RequestMessage) buf =
      match x with
      | Heartbeat x -> HeartbeatRequest.write x buf
      | Metadata x -> Metadata.writeRequest x buf
      | Fetch x -> FetchRequest.write x buf
      | Produce x -> ProduceRequest.write x buf
      | Offset x -> OffsetRequest.write x buf
      | GroupCoordinator x -> GroupCoordinatorRequest.write x buf
      | OffsetCommit x -> OffsetCommitRequest.write x buf
      | OffsetFetch x -> OffsetFetchRequest.write x buf
      | JoinGroup x -> JoinGroup.writeRequest x buf
      | SyncGroup x -> SyncGroupRequest.write x buf
      | LeaveGroup x -> LeaveGroupRequest.write x buf
      | ListGroups x -> ListGroupsRequest.write x buf
      | DescribeGroups x -> DescribeGroupsRequest.write x buf

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

  /// A Kafka request envelope.
  type Request =
    struct
      val apiKey : ApiKey
      val apiVersion : ApiVersion
      val correlationId : CorrelationId
      val clientId : ClientId
      val message : RequestMessage
      new (apiVersion, correlationId, clientId, message:RequestMessage) =
        { apiKey = message.ApiKey; apiVersion = apiVersion; correlationId = correlationId;
          clientId = clientId; message = message }
    end
  with

    static member size (x:Request) =
      Binary.sizeInt16 (int16 x.apiKey) +
      Binary.sizeInt16 x.apiVersion +
      Binary.sizeInt32 x.correlationId +
      Binary.sizeString x.clientId +
      RequestMessage.size x.message

    static member inline write (x:Request) buf =
      let buf =
        buf
        |> Binary.writeInt16 (int16 x.apiKey)
        |> Binary.writeInt16 x.apiVersion
        |> Binary.writeInt32 x.correlationId
        |> Binary.writeString x.clientId
      RequestMessage.write x.message buf

  /// A Kafka response message.
  type ResponseMessage =
    | MetadataResponse of MetadataResponse
    | FetchResponse of FetchResponse
    | ProduceResponse of ProduceResponse
    | OffsetResponse of OffsetResponse
    | GroupCoordinatorResponse of GroupCoordinatorResponse
    | OffsetCommitResponse of OffsetCommitResponse
    | OffsetFetchResponse of OffsetFetchResponse
    | JoinGroupResponse of JoinGroup.Response
    | SyncGroupResponse of SyncGroupResponse
    | HeartbeatResponse of HeartbeatResponse
    | LeaveGroupResponse of LeaveGroupResponse
    | ListGroupsResponse of ListGroupsResponse
    | DescribeGroupsResponse of DescribeGroupsResponse
  with

    /// Decodes the response given the specified ApiKey corresponding to the request.
    static member inline readApiKey apiKey buf : ResponseMessage =
      match apiKey with
      | ApiKey.HeartbeatRequest ->
        let x, _ = HeartbeatResponse.read buf in (ResponseMessage.HeartbeatResponse x)
      | ApiKey.MetadataRequest ->
        let x, _ = MetadataResponse.read buf in (ResponseMessage.MetadataResponse x)
      | ApiKey.FetchRequest ->
        let x, _ = FetchResponse.read buf in (ResponseMessage.FetchResponse x)
      | ApiKey.ProduceRequest ->
        let x, _ = ProduceResponse.read buf in (ResponseMessage.ProduceResponse x)
      | ApiKey.OffsetRequest ->
        let x, _ = OffsetResponse.read buf in (ResponseMessage.OffsetResponse x)
      | ApiKey.GroupCoordinatorRequest ->
        let x, _ = GroupCoordinatorResponse.read buf in (ResponseMessage.GroupCoordinatorResponse x)
      | ApiKey.OffsetCommitRequest ->
        let x, _ = OffsetCommitResponse.read buf in (ResponseMessage.OffsetCommitResponse x)
      | ApiKey.OffsetFetchRequest ->
        let x, _ = OffsetFetchResponse.read buf in (ResponseMessage.OffsetFetchResponse x)
      | ApiKey.JoinGroupRequest ->
        let x, _ = JoinGroup.readResponse buf in (ResponseMessage.JoinGroupResponse x)
      | ApiKey.SyncGroupRequest ->
        let x, _ = SyncGroupResponse.read buf in (ResponseMessage.SyncGroupResponse x)
      | ApiKey.LeaveGroupRequest ->
        let x, _ = LeaveGroupResponse.read buf in (ResponseMessage.LeaveGroupResponse x)
      | ApiKey.ListGroupsRequest ->
        let x, _ = ListGroupsResponse.read buf in (ResponseMessage.ListGroupsResponse x)
      | ApiKey.DescribeGroupsRequest ->
        let x, _ = DescribeGroupsResponse.read buf in (ResponseMessage.DescribeGroupsResponse x)
      | x -> failwith (sprintf "Unsupported ApiKey=%A" x)

  /// A Kafka response envelope.
  type Response =
    struct
      val correlationId : CorrelationId
      val message : ResponseMessage
      new (correlationId, message) = { correlationId = correlationId; message = message }
    end

  // TODO: provide generic version with static constraints
  let inline toArraySeg size write x =
    let size = size x
    let buf = Binary.zeros size
    buf |> write x |> ignore
    buf