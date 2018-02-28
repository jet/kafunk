namespace Kafunk

/// The Kafka RPC protocol.
[<AutoOpen>]
module Protocol =

  type ApiKey =
    | Produce = 0
    | Fetch = 1
    | Offset = 2
    | Metadata = 3
    | OffsetCommit = 8
    | OffsetFetch = 9
    | GroupCoordinator = 10
    | JoinGroup = 11
    | Heartbeat = 12
    | LeaveGroup = 13
    | SyncGroup = 14
    | DescribeGroups = 15
    | ListGroups = 16
    | ApiVersions = 18

  type ApiVersion = int16

  [<Compile(Module)>]
  module internal MessageVersions =
    
    /// Gets the version of Message for a ProduceRequest of the specified API version.
    let internal produceReqMessage (apiVer:ApiVersion) =
      if apiVer >= 2s then 1s
      else 0s

    /// Gets the version of Message for a FetchResponse of the specified API version.
    let internal fetchResMessage (apiVer:ApiVersion) =
      if apiVer >= 2s then 1s
      else 0s

  /// A correlation id of a Kafka request-response transaction.
  type CorrelationId = int32

  /// A client id.
  type ClientId = string

  /// Crc digest of a Kafka message.
  type Crc = int32

  type MagicByte = int8

  /// Kafka message attributes.
  type Attributes = int8

  /// The timestamp of a message.
  type Timestamp = int64

  /// Byte flag indicating compression codec in use.
  type CompressionCodec = byte

  [<Compile(Module)>]
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
  type Key = System.ArraySegment<byte>

  /// A Kafka message value (bytes).
  type Value = System.ArraySegment<byte>

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

  type ErrorMessage = string

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
    let MessageSizeTooLarge = 10s

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

  /// Duration in milliseconds for which the request was throttled due to quota violation.
  type ThrottleTime = int32

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

  /// The earliest available offset for a partition.
  type LogStartOffset = int64

  /// The maximum bytes to include in the message set for this partition. This
  /// helps bound the size of the response.
  type MaxBytes = int32

  /// This controls visibility of transactional records. If set to 0, makes all records
  /// visible. If set to 1, separates aborted transactions so they can be discarded.
  type IsolationLevel = int8

  /// The offset at the end of the log for this partition. This can be used by
  /// the client to determine how many messages behind the end of the log they
  /// are.
  type HighwaterMarkOffset = int64

  /// The last offset such that the state of all transactional records prior to this offset 
  /// have been decided (ABORTED or COMMITTED)
  type LastStableOffset = int64

  /// Producer id associated with an AbortedTransaction
  type ProducerId = int64

  /// First offset in an AbortedTransaction
  type FirstOffset = int64

  type AbortedTransaction = ProducerId * FirstOffset

  type TransactionalId = string

  /// Used to ask for all messages before a certain time (ms).
  type Time = int64

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module Time =
    
    /// End of topic.
    let [<Literal>] LatestOffset = -1L
    
    /// Beginning of topic.
    let [<Literal>] EarliestOffset = -2L

  type MaxNumberOfOffsets = int32

  /// A Kafka group id.
  type GroupId = string

  /// A Kafka group coordinator id.
  type CoordinatorId = int32

  /// A Kafka group coordinator host name.
  type CoordinatorHost = Host

  /// A Kafka group coordinator TCP port.
  type CoordinatorPort = Port

  /// The id of the consumer group (same as GroupId).
  type ConsumerGroup = string

  type ConsumerGroupGenerationId = int32

  type ConsumerId = string

  type RetentionTime = int64

  type Meta = string

  type MemberId = string

  type ProtocolName = string

  type ProtocolMetadata = System.ArraySegment<byte>

  /// An id of a Kafka group protocol generation.
  type GenerationId = int32

  type GroupProtocol = string

  /// The id of a group leader.
  type LeaderId = string

  /// Metadata associated with a Kafka group member.
  type MemberMetadata = System.ArraySegment<byte>

  /// A byte[] representing member assignment of a particular Kafka group protocol.
  type MemberAssignment = System.ArraySegment<byte>

  /// Raised when the received message CRC32 is different from the computed CRC32.
  type CorruptCrc32Exception (msg:string, ex:exn) =
    inherit System.Exception (msg, ex)
    new (msg) = new CorruptCrc32Exception (msg, null)

  /// Raised when the message is bigger than the message set and therefore can't be received.
  type MessageTooBigException (msg:string, ex:exn) =
    inherit System.Exception (msg, ex)
    new (msg) = new MessageTooBigException (msg, null)

  /// A Kafka message type used for producing and fetching.
  [<NoEquality;NoComparison>]
  type Message =
    struct
      val crc : Crc
      val magicByte : MagicByte
      val attributes : Attributes
      val timestamp : Timestamp
      val key : System.ArraySegment<byte>
      val value : System.ArraySegment<byte>
      new (crc, magicByte, attributes, ts, key, value) =
        { crc = crc ; magicByte = magicByte ; attributes = attributes ; timestamp = ts ; key = key ; value = value }
    end
  with

    static member Size (_messageVer:ApiVersion, m:Message) =
      Binary.sizeInt32 m.crc +
      Binary.sizeInt8 m.magicByte +
      Binary.sizeInt8 m.attributes +
      (if m.magicByte >= 1y then Binary.sizeInt64 m.timestamp else 0) +
      Binary.sizeBytes m.key +
      Binary.sizeBytes m.value

    static member Write (_messageVer:ApiVersion, m:Message, buf:BinaryZipper) =
      
      buf.ShiftOffset 4 // CRC
      let offsetAfterCrc = buf.Buffer.Offset
      buf.WriteInt8 m.magicByte
      buf.WriteInt8 m.attributes
      if m.magicByte >= 1y then
        buf.WriteInt64 m.timestamp
      buf.WriteBytes m.key
      buf.WriteBytes m.value
      let crc = Crc.crc32 buf.Buffer.Array offsetAfterCrc (buf.Buffer.Offset - offsetAfterCrc)
      let crcBuf = System.ArraySegment<_>(buf.Buffer.Array, offsetAfterCrc - 4, 4)
      Binary.pokeInt32 (int crc) crcBuf |> ignore

    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let crc = buf.ReadInt32 ()
      let magicByte = buf.ReadInt8 ()
      let attrs = buf.ReadInt8 ()
      let timestamp = 
        if ver >= 1s then 
          buf.ReadInt64 ()
        else 
          0L
      let key = buf.ReadBytes ()
      let value = buf.ReadBytes ()
      Message(crc,magicByte,attrs,timestamp,key,value)

    // NB: assumes that m.key and m.value use the same underlying array
    static member ComputeCrc (ver:ApiVersion, m:Message) =
      let offsetAtKey =
        m.value.Offset
        - 4 // key length
        - 4 // value length
        - (if isNull m.key.Array then 0 else m.key.Count)
      let offsetAfterCrc =
        offsetAtKey
        - 1 // magicByte
        - 1 // attrs
        - (if ver >= 1s then 8 else 0) // timestamp
      let offsetAtEnd = m.value.Offset + m.value.Count
      let readMessageSize = offsetAtEnd - offsetAfterCrc
      let crc32 = Crc.crc32 m.value.Array offsetAfterCrc readMessageSize
      int32 crc32
    
    // NB: assumes that m.key and m.value use the same underlying array
    static member CheckCrc (ver:ApiVersion, m:Message) =
      let crc' = Message.ComputeCrc (ver,m)
      if m.crc <> crc' then
        raise (CorruptCrc32Exception(sprintf "Corrupt message data. Computed CRC32=%i received CRC32=%i|key=%s" crc' m.crc (Binary.toString m.key)))


  [<NoEquality;NoComparison>]
  type MessageSetItem =
    struct
      val offset : Offset
      val messageSize : MessageSize
      val message : Message
      new (o,ms,m) = { offset = o ; messageSize = ms ; message = m }
    end
    
  [<NoEquality;NoComparison>]
  type MessageSet =
    struct
      val messages : MessageSetItem[]
      new (set) = { messages = set }
    end
  with

    static member Size (ver:ApiVersion, x:MessageSet) =
      let mutable size = 0
      for i = 0 to x.messages.Length - 1 do
        let m = x.messages.[i].message
        size <- size + 8 + 4 + (Message.Size (ver,m))
      size

    static member Write (messageVer:ApiVersion, ms:MessageSet, buf:BinaryZipper) =
      //for (o,ms,m) in ms.messages do
      for x in ms.messages do
        buf.WriteInt64 x.offset
        buf.WriteInt32 x.messageSize
        Message.Write (messageVer, x.message, buf)

    // NB: skipTooLarge=true is for scenarios where decompression is involved and a message set is being decoded from an individual message
    // which was itself too small.
    static member Read (messageVer:ApiVersion, partition:Partition, ec:ErrorCode, messageSetSize:int, skipTooLarge:bool, buf:BinaryZipper) =
      let mutable consumed = 0
      let arr = ResizeArray<_>()
      while consumed < messageSetSize && buf.Buffer.Count > 0 do
        let o' = buf.Buffer.Offset
        let messageSetRemainder = messageSetSize - consumed
        if messageSetRemainder >= 12 && buf.Buffer.Count >= 12 then
          let (offset:Offset) = buf.ReadInt64 ()
          let (messageSize:MessageSize) = buf.ReadInt32 ()
          let messageSetRemainder = messageSetRemainder - 12 // (Offset + MessageSize)
          if messageSize > messageSetSize then
            let errMsg = sprintf "partition=%i offset=%i message_set_size=%i message_size=%i consumed_bytes=%i consumed_count=%i" 
                                    partition offset messageSetSize messageSize consumed arr.Count
            if not skipTooLarge then
              raise (MessageTooBigException(errMsg))
            else
//              let payload = Binary.toString buf.Buffer
//              printfn "|WARN|MessageTooBig|%s" errMsg
//              printfn "|WARN|MessageTooBig|payload=%s" payload
//              try
//                let message = Message.Read (messageVer,buf)
//                printfn "|WARN|MessageTooBig|payload=%s" (Binary.toString message.value)
//              with ex ->
//                printfn "ERROR DECODING MESSAGE|%O" ex
              ()
          try
            if messageSetRemainder >= messageSize && buf.Buffer.Count >= messageSize then
              let message = Message.Read (messageVer,buf)
              arr.Add (MessageSetItem(offset,messageSize,message))
            else
              let rem = min messageSetRemainder buf.Buffer.Count
              buf.ShiftOffset rem
          with :? CorruptCrc32Exception as ex ->
            let msg =
              sprintf "partition=%i offset=%i error_code=%i consumed=%i message_set_size=%i message_set_remainder=%i message_size=%i buffer_offset=%i buffer_size=%i"
                partition
                offset
                ec
                consumed 
                messageSetSize
                messageSetRemainder 
                messageSize
                buf.Buffer.Offset
                buf.Buffer.Count
            raise (CorruptCrc32Exception(msg, ex))
        else
          buf.ShiftOffset messageSetRemainder
        consumed <- consumed + (buf.Buffer.Offset - o')
      MessageSet(arr.ToArray())
  
    static member CheckCrc (ver:ApiVersion, ms:MessageSet) =
      //for (_,_,m) in ms.messages do
      for x in ms.messages do
        Message.CheckCrc (ver,x.message)


  /// A Kafka broker consists of a node id, host name and TCP port.
  [<AutoSerializable(false);StructuralEquality;StructuralComparison>]
  type Broker =
    struct
      val nodeId : NodeId
      val host : Host
      val port : Port
      val rack : string
      new (nodeId, host, port) = { nodeId = nodeId; host = host; port = port; rack = null }
      new (nodeId, host, port, rack) = { nodeId = nodeId; host = host; port = port; rack = rack }
    end
  with
    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
      match ver with
      | 0s ->
        let nodeId = buf.ReadInt32()
        let host = buf.ReadString()
        let port = buf.ReadInt32()
        Broker(nodeId, host, port)
      | 1s ->
        let nodeId = buf.ReadInt32()
        let host = buf.ReadString()
        let port = buf.ReadInt32()
        let rack = buf.ReadString()
        Broker(nodeId, host, port, rack)
      | _ ->
        failwithf "Unsupported Broker Format for Metadata Api Response Version: %i" ver


  [<NoEquality;NoComparison;AutoSerializable(false)>]
  type PartitionMetadata =
    struct
      val partitionErrorCode : PartitionErrorCode
      val partitionId : Partition
      val leader : Leader
      val replicas : Replicas
      val isr : Isr
      val offlineReplicas : Replicas
      new (partitionErrorCode, partitionId, leader, replicas, isr, offlineReplicas) =
        { partitionErrorCode = partitionErrorCode; partitionId = partitionId;
          leader = leader; replicas = replicas; isr = isr; offlineReplicas = offlineReplicas }
    end
  with
    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
      let errorCode = buf.ReadInt16()
      let partition = buf.ReadInt32()
      let leader = buf.ReadInt32()
      let replicas = buf.ReadArray (fun b -> b.ReadInt32())
      let inSyncReplicas = buf.ReadArray (fun b -> b.ReadInt32())
      let offlineReplicas = 
        match ver with 
        | 5s -> buf.ReadArray (fun b -> b.ReadInt32()) 
        | _ -> [||]
      PartitionMetadata(errorCode, partition, leader, replicas, inSyncReplicas, offlineReplicas)

  /// Metadata for a specific topic consisting of a set of partition-to-broker assignments.
  [<NoEquality;NoComparison>]
  type TopicMetadata =
    struct
      val topicErrorCode : TopicErrorCode
      val topicName : TopicName
      val partitionMetadata : PartitionMetadata[]
      val isInternal : bool
      new (topicErrorCode, topicName, partitionMetadata, isInternal) =
        { topicErrorCode = topicErrorCode; topicName = topicName; partitionMetadata = partitionMetadata; isInternal = isInternal }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let errorCode = buf.ReadInt16()
      let topic = buf.ReadString()
      let isInternal = match ver with | 0s -> false | _ -> buf.ReadBool()
      let partitionMetadata = buf.ReadArray (fun b -> PartitionMetadata.Read(ver, b))
      TopicMetadata(errorCode, topic, partitionMetadata, isInternal)
    
  /// Request metadata on all or a specific set of topics.
  /// Can be routed to any node in the bootstrap list.
  [<NoEquality;NoComparison>]
  type MetadataRequest =
    struct
      val topics : TopicName[]
      val autoTopicCreation : bool
      new (topics) =  { topics = topics ; autoTopicCreation = false }
      new (topics, autoTopicCreation) =  { topics = topics ; autoTopicCreation = autoTopicCreation }
    end
  with
    static member Size (ver:ApiVersion, req:MetadataRequest) =
      let topicSize = Binary.sizeArray req.topics Binary.sizeString
      match ver with
      | 0s | 1s | 2s | 3s ->
        topicSize
      | 4s | 5s ->
        topicSize + Binary.sizeBool req.autoTopicCreation
      | _ ->
        failwithf "Unsupported MetadataRequest API Version %i" ver

    static member Write (ver:ApiVersion, req:MetadataRequest, buf:BinaryZipper) =
      let writeTopics (buf:BinaryZipper, t:TopicName) =
        buf.WriteString t
        
      match ver with
      | 0s | 1s | 2s | 3s ->
        buf.WriteArray(req.topics, writeTopics)
      | 4s | 5s ->
        buf.WriteArray(req.topics, writeTopics)
        buf.WriteBool req.autoTopicCreation
      | _ ->
        failwithf "Unsupported MetadataRequest API Version %i" ver

  /// Contains a list of all brokers (node id, host, post) and assignment of topic/partitions to brokers.
  /// The assignment consists of a leader, a set of replicas and a set of in-sync replicas.
  [<NoEquality;NoComparison>]
  type MetadataResponse =
    struct
      val brokers : Broker[]
      val topicMetadata : TopicMetadata[]
      val controllerId : int
      val clusterId: string
      val throttleTimeMs: ThrottleTime
      new (brokers, topicMetadata, controllerId, clusterId, throttleTimeMs) =  
        { brokers = brokers; topicMetadata = topicMetadata; controllerId = controllerId; clusterId = clusterId;
          throttleTimeMs = throttleTimeMs }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let throttleTimeMs =
        match ver with
        | 0s | 1s | 2s -> 0
        | _ -> buf.ReadInt32()
      let brokers = buf.ReadArray (fun b -> Broker.Read(ver, b))
      let clusterId =
        match ver with
        | 0s | 1s -> null
        | _ -> buf.ReadString()
      let controllerId = 
        match ver with 
        | 0s -> -1 
        | _ -> buf.ReadInt32()
      let topicMetadata = buf.ReadArray (fun b -> TopicMetadata.Read(ver,b))
      MetadataResponse(brokers, topicMetadata, controllerId, clusterId, throttleTimeMs)

  // Produce API

  [<NoEquality;NoComparison>]
  type ProduceRequestPartitionMessageSet =
    struct
      val partition : Partition
      val messageSetSize : MessageSetSize
      val messageSet : MessageSet
      new (p,mss,ms) = { partition = p ; messageSetSize = mss ; messageSet = ms }
    end

  [<NoEquality;NoComparison>]
  type ProduceRequestTopicMessageSet =
    struct
      val topic : TopicName
      val partitions : ProduceRequestPartitionMessageSet[]
      new (t,ps) = { topic = t ; partitions = ps }
    end

  [<NoEquality;NoComparison>]
  type ProduceRequest =
    struct
      val transactionalId : TransactionalId
      val requiredAcks : RequiredAcks
      val timeout : Timeout
      val topics : ProduceRequestTopicMessageSet[]
      new (requiredAcks, timeout, topics) =
        { requiredAcks = requiredAcks; timeout = timeout; topics = topics; transactionalId = null }
      new (requiredAcks, timeout, topics, transactionalId) =
        { requiredAcks = requiredAcks; timeout = timeout; topics = topics; transactionalId = transactionalId }
    end
  with

    static member Size (ver:ApiVersion, x:ProduceRequest) =
      let mutable size = 0
      if ver >= 3s then 
        size <- size + Binary.sizeString x.transactionalId
      size <- size + 10 // requiredAcks (2), timeout (4), topics array size (4)
      for i = 0 to x.topics.Length - 1 do
        let y = x.topics.[i]
        size <- size + (Binary.sizeString y.topic)
        size <- size + 4 // partition array size
        for z in y.partitions do
          let mss = z.messageSetSize
          size <- size + 8 + mss // partitionId (4), message set size (4), message set  
      size

    static member Write (ver:ApiVersion, x:ProduceRequest, buf:BinaryZipper) =
      let messageVer = MessageVersions.produceReqMessage ver
      if ver >= 3s then 
        buf.WriteString x.transactionalId
      buf.WriteInt16 x.requiredAcks
      buf.WriteInt32 x.timeout
      buf.WriteInt32 x.topics.Length
      for i = 0 to x.topics.Length - 1 do
        let y = x.topics.[i]
        buf.WriteString y.topic
        buf.WriteInt32 y.partitions.Length
        for z in y.partitions do
          buf.WriteInt32 z.partition
          buf.WriteInt32 z.messageSetSize
          MessageSet.Write (messageVer, z.messageSet, buf)

  and [<NoEquality;NoComparison>] ProduceResponsePartitionItem =
    struct
      val partition : Partition
      val errorCode : ErrorCode
      val offset : Offset
      val timestamp : Timestamp
      val logStartOffset : Offset
      new (p,ec,o,ts,lso) = { partition = p ; errorCode = ec ; offset = o ; timestamp = ts ; logStartOffset = lso }
    end

  and [<NoEquality;NoComparison>] ProduceResponseTopicItem =
    struct
      val topic : TopicName
      val partitions : ProduceResponsePartitionItem[]
      new (t,ps) = { topic = t ; partitions = ps }
    end

  /// A reponse to a produce request.
  and [<NoEquality;NoComparison>] ProduceResponse =
    struct
      val topics : ProduceResponseTopicItem[]
      val throttleTime : ThrottleTime
      new (topics,throttleTime) = { topics = topics ; throttleTime = throttleTime }
    end
  with

    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let numTopics = buf.ReadInt32 ()
      let topics = Array.zeroCreate numTopics
      for i = 0 to topics.Length - 1 do
        let topicName = buf.ReadString ()
        let numPartitions = buf.ReadInt32 ()
        let partitions = Array.zeroCreate numPartitions
        for j = 0 to partitions.Length - 1 do
          let partition = buf.ReadInt32 ()
          let errorCode = buf.ReadInt16 ()
          let baseoffset = buf.ReadInt64 ()
          let logAppendTime = 
            if ver >= 2s then buf.ReadInt64 ()
            else 0L
          let logStartOffset = 
            if ver >= 5s then buf.ReadInt64 ()
            else 0L
          partitions.[j] <- ProduceResponsePartitionItem(partition,errorCode,baseoffset,logAppendTime, logStartOffset)
        topics.[i] <- ProduceResponseTopicItem(topicName,partitions)
      let throttleTime = 
        if ver >= 1s then buf.ReadInt32 ()
        else 0
      ProduceResponse(topics,throttleTime)

  // Fetch API

  [<NoEquality;NoComparison>]
  type FetchRequest =
    struct
      val replicaId : ReplicaId
      val maxWaitTime : MaxWaitTime
      val minBytes : MinBytes
      val maxBytes : MaxBytes
      val isolationLevel : IsolationLevel
      val topics : (TopicName * (Partition * FetchOffset * LogStartOffset * MaxBytes)[])[]
      new (replicaId, maxWaitTime, minBytes, topics, maxBytes, isolationLevel) =
        { replicaId = replicaId; maxWaitTime = maxWaitTime; minBytes = minBytes; topics = topics; 
          maxBytes = maxBytes; isolationLevel = isolationLevel }
    end
  with
    // leverages mutability to reduce performance overhead
    static member Size (ver:ApiVersion, req:FetchRequest) =
      let mutable size = 16 // replicaId + maxWaitTime + minBytes + topic array size (4 bytes each)
      if ver >= 3s then size <- size + 4 // maxBytes
      if ver >= 4s then size <- size + 1 // isolation level
      for i = 0 to req.topics.Length - 1 do 
        let (t,ps) = req.topics.[i]
        size <- size + (Binary.sizeString t) // topic name size
        size <- size + 4 // partition array size
        for (_,_,_,_) in ps do
          size <- size + 16 // partition (4) + fetch offset (8) + maxBytes (4)
          if ver >= 5s then size <- size + 8 // log start offset
      size

    static member Write (ver:ApiVersion, req:FetchRequest, buf:BinaryZipper) =
      buf.WriteInt32 req.replicaId
      buf.WriteInt32 req.maxWaitTime
      buf.WriteInt32 req.minBytes
      if ver >= 3s then buf.WriteInt32 req.maxBytes
      if ver >= 4s then buf.WriteInt8 req.isolationLevel
      buf.WriteInt32 req.topics.Length
      for i = 0 to req.topics.Length - 1 do
        let (topic, partitions) = req.topics.[i]
        buf.WriteString topic
        buf.WriteInt32 partitions.Length
        for j = 0 to partitions.Length - 1 do
          let (p,offset,logStartOffset,maxBytes) = partitions.[j]
          buf.WriteInt32 p
          buf.WriteInt64 offset
          if ver >= 5s then buf.WriteInt64 logStartOffset
          buf.WriteInt32 maxBytes

  [<NoEquality;NoComparison>]
  type FetchResponse =
    struct
      val throttleTime : ThrottleTime
      val topics : (TopicName * (Partition * ErrorCode * HighwaterMarkOffset * LastStableOffset * LogStartOffset * AbortedTransaction[] * MessageSetSize * MessageSet)[])[]
      new (tt,topics) = { throttleTime = tt ; topics = topics }
    end
  with
    // Leverages mutability and lack of modular functions for efficiency, as this is a high throughput API. 
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let throttleTime =
        match ver with
        | 0s -> 0 
        | _ -> buf.ReadInt32 ()
      let numTopics = buf.ReadInt32()
      let topics = Array.zeroCreate numTopics
      for i = 0 to topics.Length - 1 do
        let topic = buf.ReadString ()
        let numPartitions = buf.ReadInt32 ()
        let partitions = Array.zeroCreate numPartitions
        for j = 0 to partitions.Length - 1 do
          let partition = buf.ReadInt32 ()
          let errorCode = buf.ReadInt16 ()
          let highWatermark = buf.ReadInt64 ()
          let lastStableOffset = if ver >= 4s then buf.ReadInt64() else -1L
          let logStartOffset = if ver >= 5s then buf.ReadInt64() else -1L
          let numAbortedTxns = if ver >= 4s then buf.ReadInt32() else -1
          let abortedTxns =
            if numAbortedTxns >= 0 then
              let abortedTxns = Array.zeroCreate numAbortedTxns
              for k = 0 to abortedTxns.Length - 1 do
                let producerId = buf.ReadInt64 ()
                let firstOffset = buf.ReadInt64 ()
                abortedTxns.[k] <- (producerId, firstOffset)
              abortedTxns
            else
              null
          let messageSetSize = buf.ReadInt32 ()
          let messageSet = MessageSet.Read (MessageVersions.fetchResMessage ver,partition,errorCode,messageSetSize,false,buf)
          partitions.[j] <-  partition, errorCode, highWatermark, lastStableOffset, logStartOffset, abortedTxns, messageSetSize, messageSet
        topics.[i] <- (topic,partitions)
      let res = FetchResponse(throttleTime, topics)
      res

  // Offset API

  [<NoEquality;NoComparison>]
  type PartitionOffsets =
    struct
      val partition : Partition
      val errorCode : ErrorCode
      val timestamp : Timestamp
      val offsets : Offset[]
      new (partition, errorCode, ts, offsets) =
        { partition = partition; errorCode = errorCode; timestamp = ts ; offsets = offsets }
    end

  /// A request to return offset information for a set of topics on a specific replica.
  [<NoEquality;NoComparison>]
  type OffsetRequest =
    struct
      val replicaId : ReplicaId
      val topics : (TopicName * (Partition * Time * MaxNumberOfOffsets)[])[]
      new (replicaId, topics) = { replicaId = replicaId; topics = topics }
    end
  with

    static member Size (ver:ApiVersion, x:OffsetRequest) =
      let partitionSize (part, time, maxNumOffsets) =
        Binary.sizeInt32 part + 
        Binary.sizeInt64 time + 
        (if ver = 0s then Binary.sizeInt32 maxNumOffsets else 0)
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeInt32 x.replicaId + Binary.sizeArray x.topics topicSize

    static member Write (apiVer:ApiVersion, x:OffsetRequest, buf:BinaryZipper) =
      let writePartition (buf:BinaryZipper,(p,t,mo)) =
        buf.WriteInt32 p
        buf.WriteInt64 t
        if apiVer = 0s then buf.WriteInt32 mo
        else ()
      let writeTopic (buf:BinaryZipper, (t,ps)) =
        buf.WriteString t
        buf.WriteArray (ps, writePartition)
      buf.WriteInt32 x.replicaId
      buf.WriteArray (x.topics, writeTopic)

  [<NoEquality;NoComparison>]
  type OffsetResponse =
    struct
      val topics : (TopicName * PartitionOffsets[])[]
      new (topics) = { topics = topics }
    end
  with

    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let readPartition (buf:BinaryZipper) =
        let p = buf.ReadInt32 ()
        let ec = buf.ReadInt16 ()
        let ts = 
          if ver >= 1s then buf.ReadInt64 ()
          else 0L
        let os = buf.ReadArray (fun buf -> buf.ReadInt64 ())
        PartitionOffsets(p, ec, ts, os)
      let readTopic (buf:BinaryZipper) =
        let t = buf.ReadString ()
        let ps = buf.ReadArray readPartition
        t,ps
      let topics = buf.ReadArray readTopic
      OffsetResponse(topics)

  // Offset Commit/Fetch API

  [<NoEquality;NoComparison>]
  type OffsetCommitRequest =
    struct
      val consumerGroup : ConsumerGroup
      val consumerGroupGenerationId : ConsumerGroupGenerationId
      val consumerId : ConsumerId
      val retentionTime : RetentionTime
      val topics : (TopicName * (Partition * Offset * Timestamp * Meta)[])[]
      new (consumerGroup, consumerGroupGenerationId, consumerId, retentionTime, topics) =
        { consumerGroup = consumerGroup; consumerGroupGenerationId = consumerGroupGenerationId;
          consumerId = consumerId; retentionTime = retentionTime; topics = topics }
    end
  with
    static member Size (ver:ApiVersion, x:OffsetCommitRequest) =
      let partitionSize (part, offset, ts, metadata) =
        Binary.sizeInt32 part + 
        Binary.sizeInt64 offset + 
        (if ver = 1s then Binary.sizeInt64 ts else 0) +
        Binary.sizeString metadata
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeString x.consumerGroup +
      Binary.sizeInt32 x.consumerGroupGenerationId +
      Binary.sizeString x.consumerId +
      Binary.sizeInt64 x.retentionTime +
      Binary.sizeArray x.topics topicSize

    static member Write (ver:ApiVersion, x:OffsetCommitRequest, buf:BinaryZipper) =
      let writePartition (buf:BinaryZipper, (p,o,ts,m)) =
        buf.WriteInt32 p
        buf.WriteInt64 o
        if ver = 1s then buf.WriteInt64 ts
        buf.WriteString m
      let writeTopic (buf:BinaryZipper, (t,ps)) =
        buf.WriteString t
        buf.WriteArray (ps, writePartition)
      buf.WriteString x.consumerGroup
      buf.WriteInt32 x.consumerGroupGenerationId
      buf.WriteString x.consumerId
      buf.WriteInt64 x.retentionTime
      buf.WriteArray (x.topics, writeTopic)

  [<NoEquality;NoComparison>]
  type OffsetCommitResponse =
    struct
      val throttleTimeMs : ThrottleTime
      val topics : (TopicName * (Partition * ErrorCode)[])[]
      new (topics) = { throttleTimeMs = 0; topics = topics }
      new (throttleTime, topics) = { throttleTimeMs = throttleTime; topics = topics }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let readPartitions (buf:BinaryZipper) =
        let partition = buf.ReadInt32()
        let errorCode = buf.ReadInt16()
        partition, errorCode

      let readTopic (buf:BinaryZipper) =
        let topicName = buf.ReadString()
        let partitions = buf.ReadArray readPartitions
        topicName, partitions
           
      match ver with
      | 0s | 1s | 2s ->
        let topics = buf.ReadArray readTopic
        OffsetCommitResponse(topics)
      | 3s ->
        let throttleTimeMs = buf.ReadInt32()
        let topics = buf.ReadArray readTopic
        OffsetCommitResponse(throttleTimeMs, topics)
      | _ ->
        failwithf "Unsupported OffsetCommit Response API Version: %i" ver


  type OffsetFetchRequest =
    struct
      val consumerGroup : ConsumerGroup
      val topics : (TopicName * Partition[])[]
      new (consumerGroup, topics) = { consumerGroup = consumerGroup; topics = topics }
    end
  with

    static member Size (_: ApiVersion, req: OffsetFetchRequest) =
        let partitionsSize = Binary.sizeInt32 

        let topicSize (topicName, partitions) = 
            Binary.sizeString topicName +
            Binary.sizeArray partitions partitionsSize

        Binary.sizeString req.consumerGroup +
        Binary.sizeArray req.topics topicSize
    
    static member Write (_: ApiVersion, req:OffsetFetchRequest, buf:BinaryZipper) =
        let writePartitions (buf: BinaryZipper, partition) =
            buf.WriteInt32 partition

        let writeTopics (buf: BinaryZipper, (topicName, partitions)) =
            buf.WriteString topicName
            buf.WriteArray (partitions, writePartitions)

        buf.WriteString req.consumerGroup
        buf.WriteArray (req.topics, writeTopics)

  [<NoEquality;NoComparison>]
  type OffsetFetchResponse =
    struct
      val throttleTime : ThrottleTime
      val topics : (TopicName * (Partition * Offset * Meta * ErrorCode)[])[]
      val errorCode : ErrorCode
      new (topics, errorCode, throttleTime) = { topics = topics; errorCode = errorCode; throttleTime = throttleTime }
    end
  with

    static member Read (version:ApiVersion, buf:BinaryZipper) =
        let readTopics (buf: BinaryZipper) =
            let readPartition (buf: BinaryZipper) =
                let partition = buf.ReadInt32()
                let offset = buf.ReadInt64()
                let metadata = buf.ReadString()
                let errorCode = buf.ReadInt16()
                partition, offset, metadata, errorCode

            let readTopic (buf: BinaryZipper) =
                let topicName = buf.ReadString()
                let partitions = buf.ReadArray readPartition
                topicName, partitions
            
            buf.ReadArray readTopic

        match version with
        | 0s 
        | 1s -> 
            let topics = readTopics buf
            OffsetFetchResponse(topics, ErrorCode.NoError, 0)    
        | 2s -> 
            let topics = readTopics buf
            let errorCode = buf.ReadInt16()
            OffsetFetchResponse(topics, errorCode, 0) 
        | 3s -> 
            let throttleTimeMs = buf.ReadInt32()
            let topics = readTopics buf
            let errorCode = buf.ReadInt16()
            OffsetFetchResponse(topics, errorCode, throttleTimeMs)
        | _ -> 
            failwithf "Unsupported API Version: %i" version

  // Group Membership API

  /// The offsets for a given consumer group are maintained by a specific
  /// broker called the group coordinator. i.e., a consumer needs to
  /// issue its offset commit and fetch requests to this specific broker.
  /// It can discover the current coordinator by issuing a group coordinator request.
  /// Can be routed to any node in the bootstrap list.
  [<NoEquality;NoComparison>]
  type GroupCoordinatorRequest =
    struct
      val groupId : GroupId
      val coordinatorType: int8
      new (groupId) = {groupId = groupId; coordinatorType = 0y }
      new (groupId, coordinatorType) = { groupId = groupId ; coordinatorType = coordinatorType }
    end
  with
    static member Size (ver:ApiVersion, req:GroupCoordinatorRequest) =
      match ver with
      | 0s -> 
        Binary.sizeString req.groupId
      | 1s ->
        Binary.sizeString req.groupId + Binary.sizeInt8 req.coordinatorType
      | _ -> 
        failwithf "Unsupported FindCoordinator API Request Version: %i" ver
     
    static member Write (ver:ApiVersion, req:GroupCoordinatorRequest, buf:BinaryZipper) =
      match ver with
      | 0s ->
        buf.WriteString req.groupId
      | 1s ->
        buf.WriteString req.groupId
        buf.WriteInt8 req.coordinatorType
      | _ -> 
        failwithf "Unsupported FindCoordinator API Request Version: %i" ver

  type GroupCoordinatorResponse =
    struct
      val errorCode : ErrorCode
      val errorMessage : ErrorMessage
      val coordinatorId : CoordinatorId
      val coordinatorHost : CoordinatorHost
      val coordinatorPort : CoordinatorPort
      val throttleTimeMs : ThrottleTime
      new (errorCode, errorMessage, coordinatorId, coordinatorHost, coordinatorPort, throttleTimeMs) =
        { errorCode = errorCode; errorMessage = errorMessage; coordinatorId = coordinatorId; coordinatorHost = coordinatorHost;
          coordinatorPort = coordinatorPort; throttleTimeMs = throttleTimeMs }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      if ver > 1s then failwithf "Unsupported FindCoordinator Response Version: %i" ver

      let throttleTimeMs = 
        match ver with 
        | 1s -> buf.ReadInt32() 
        | _ -> 0
      let errorCode = buf.ReadInt16()
      let errorMessage = 
        match ver with 
        | 1s -> buf.ReadString() 
        | _ -> null
      let coordinatorId = buf.ReadInt32()
      let host = buf.ReadString()
      let port = buf.ReadInt32()
      GroupCoordinatorResponse(errorCode, errorMessage, coordinatorId, host, port, throttleTimeMs)

  type SessionTimeout = int32

  type RebalanceTimeout = int32

  type ProtocolType = string

  [<NoEquality;NoComparison>]
  type GroupProtocols =
    struct
      val protocols : (ProtocolName * ProtocolMetadata)[]
      new (protocols) = { protocols = protocols }
    end
  with

    static member Size (_:ApiVersion, x:GroupProtocols) =
      let protocolSize (name, metadata) =
        Binary.sizeString name + Binary.sizeBytes metadata
      Binary.sizeArray x.protocols protocolSize

    static member Write (_:ApiVersion, x:GroupProtocols, buf:BinaryZipper) =
      let writeProtocol (buf:BinaryZipper, (protocolName, protocolMetadata)) =
        buf.WriteString protocolName
        buf.WriteBytes protocolMetadata

      buf.WriteArray(x.protocols, writeProtocol)

  [<NoEquality;NoComparison>]
  type Members =
    struct
      val members : (MemberId * MemberMetadata)[]
      new (members) = { members = members }
    end
  with

    static member Read (buf:BinaryZipper) =      
      let ms = 
        buf.ReadArray (fun buf ->
          let mid = buf.ReadString ()
          let md = buf.ReadBytes()
          mid,md)
      Members(ms)

  [<NoEquality;NoComparison>]
  type JoinGroupRequest =
    struct
      val groupId : GroupId
      val sessionTimeout : SessionTimeout
      val rebalanceTimeout : SessionTimeout
      val memberId : MemberId
      val protocolType : ProtocolType
      val groupProtocols : GroupProtocols
      new (groupId, sessionTimeout, rebalanceTimeout, memberId, protocolType, groupProtocols) =
        { groupId = groupId; sessionTimeout = sessionTimeout; rebalanceTimeout = rebalanceTimeout ; memberId = memberId;
          protocolType = protocolType; groupProtocols = groupProtocols }
    end
  with
    static member Size (ver:ApiVersion, req:JoinGroupRequest) =
      Binary.sizeString req.groupId +
      Binary.sizeInt32 req.sessionTimeout +
      (if ver >= 1s then 4 else 0) +
      Binary.sizeString req.memberId +
      Binary.sizeString req.protocolType +
      GroupProtocols.Size(ver,req.groupProtocols)

    static member Write (ver:ApiVersion, req:JoinGroupRequest, buf:BinaryZipper) =
      buf.WriteString req.groupId
      buf.WriteInt32 req.sessionTimeout
      (if ver >= 1s then buf.WriteInt32 req.rebalanceTimeout)
      buf.WriteString req.memberId
      buf.WriteString req.protocolType
      GroupProtocols.Write(ver, req.groupProtocols, buf)

  [<NoEquality;NoComparison>]
  type JoinGroupResponse =
    struct
      val throttleTime : ThrottleTime
      val errorCode : ErrorCode
      val generationId : GenerationId
      val groupProtocol : GroupProtocol
      val leaderId : LeaderId
      val memberId : MemberId
      val members : Members
      new (throttleTimeMs,errorCode, generationId, groupProtocol, leaderId, memberId, members) =
        { throttleTime = throttleTimeMs ; errorCode = errorCode; generationId = generationId; 
          groupProtocol = groupProtocol; leaderId = leaderId; memberId = memberId; members = members }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let throttleTimeMs = 
        if ver >= 2s then buf.ReadInt32 ()
        else 0
      let errorCode = buf.ReadInt16 ()
      let groupId = buf.ReadInt32 ()
      let groupProtocol = buf.ReadString ()
      let leaderId = buf.ReadString ()
      let memberId = buf.ReadString ()
      let members = Members.Read buf
      JoinGroupResponse(throttleTimeMs, errorCode, groupId, groupProtocol, leaderId, memberId, members)
        

  [<NoEquality;NoComparison>]
  type GroupAssignment =
    struct
      val members : (MemberId * MemberAssignment)[]
      new (members) = { members = members }
    end
  with

    static member Size (_:ApiVersion, req:GroupAssignment) =
      Binary.sizeArray req.members (fun (memId, memAssign) -> Binary.sizeString memId + Binary.sizeBytes memAssign)

    static member Write (_:ApiVersion, req:GroupAssignment, buf:BinaryZipper) =
      let writeMember (buf: BinaryZipper, (memberId, memberAssignment)) =
        buf.WriteString memberId
        buf.WriteBytes memberAssignment

      buf.WriteArray(req.members, writeMember)

  /// The sync group request is used by the group leader to assign state (e.g.
  /// partition assignments) to all members of the current generation. All
  /// members send SyncGroup immediately after joining the group, but only the
  /// leader provides the group's assignment.
  [<NoEquality;NoComparison>]
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
    static member Size (ver:ApiVersion, req: SyncGroupRequest) =
      Binary.sizeString req.groupId +
      Binary.sizeInt32 req.generationId +
      Binary.sizeString req.memberId +
      GroupAssignment.Size(ver, req.groupAssignment)

    static member Write (ver:ApiVersion, req:SyncGroupRequest, buf:BinaryZipper) =
      buf.WriteString req.groupId
      buf.WriteInt32 req.generationId
      buf.WriteString req.memberId
      GroupAssignment.Write(ver, req.groupAssignment, buf)

  [<NoEquality;NoComparison>]
  type SyncGroupResponse =
    struct
      val throttleTime : ThrottleTime
      val errorCode : ErrorCode
      val memberAssignment : MemberAssignment
      new (throttleTime,errorCode, memberAssignment) = 
        { throttleTime = throttleTime ; errorCode = errorCode; memberAssignment = memberAssignment }
    end
  with
    static member Read (ver:ApiVersion,buf:BinaryZipper) =
      let tt = 
        if ver >= 1s then buf.ReadInt32 ()
        else 0
      let errorCode = buf.ReadInt16 ()
      let ma = buf.ReadBytes ()
      SyncGroupResponse(tt, errorCode, ma)


  /// Sent by a consumer to the group coordinator.
  [<NoEquality;NoComparison>]
  type HeartbeatRequest =
    struct
      val groupId : GroupId
      val generationId : GenerationId
      val memberId : MemberId
      new (groupId, generationId, memberId) =
        { groupId = groupId; generationId = generationId; memberId = memberId }
    end
  with
    static member Size (_:ApiVersion, req:HeartbeatRequest) =
      Binary.sizeString req.groupId + Binary.sizeInt32 req.generationId + Binary.sizeString req.memberId

    static member Write (_:ApiVersion, req:HeartbeatRequest, buf: BinaryZipper) =
      buf.WriteString req.groupId
      buf.WriteInt32 req.generationId
      buf.WriteString req.memberId

  /// Heartbeat response from the group coordinator.
  [<NoEquality;NoComparison>]
  type HeartbeatResponse =
    struct
      val errorCode : ErrorCode
      val throttleTimeMs : ThrottleTime
      new (errorCode, throttleTimeMs) = { errorCode = errorCode ; throttleTimeMs = throttleTimeMs }
    end
  with
    static member Read (ver: ApiVersion, buf: BinaryZipper) =
      match ver with
      | 0s ->
        let errorCode = buf.ReadInt16()
        HeartbeatResponse(errorCode, 0)
      | 1s ->
        let throttleTimeMs = buf.ReadInt32()
        let errorCode = buf.ReadInt16()
        HeartbeatResponse(errorCode, throttleTimeMs)
      | _ ->
        failwithf "Unsupported Heartbeat Response API Version: %i" ver
        

  /// An explicit request to leave a group. Preferred over session timeout.
  [<NoEquality;NoComparison>]
  type LeaveGroupRequest =
    struct
      val groupId : GroupId
      val memberId : MemberId
      new (groupId, memberId) = { groupId = groupId; memberId = memberId }
    end
  with
    static member Size (_:ApiVersion, req:LeaveGroupRequest) =
      Binary.sizeString req.groupId + Binary.sizeString req.memberId

    static member Write (_:ApiVersion, req:LeaveGroupRequest, buf: BinaryZipper) =
      buf.WriteString req.groupId
      buf.WriteString req.memberId

  [<NoEquality;NoComparison>]
  type LeaveGroupResponse =
    struct
      val errorCode : ErrorCode
      val throttleTimeMs : ThrottleTime
      new (errorCode, throttleTimeMs) = { errorCode = errorCode ; throttleTimeMs = throttleTimeMs }
    end
  with
    static member Read (ver: ApiVersion, buf:BinaryZipper) =
      match ver with
      | 0s ->
        let errorCode = buf.ReadInt16()
        LeaveGroupResponse(errorCode, 0)
      | 1s ->
        let throttleMs = buf.ReadInt32()
        let errorCode = buf.ReadInt16()
        LeaveGroupResponse(errorCode, throttleMs)
      | _ ->
        failwithf "Unsupported LeaveGroup Response API Version: %i" ver


  // Consumer groups
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal
  // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+0.9+Consumer+Rewrite+Design
  // http://people.apache.org/~nehanarkhede/kafka-0.9-consumer-javadoc/doc/

  [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
  module ProtocolType =

    let consumer = "consumer"

  type ConsumerGroupProtocolMetadataVersion = int16

  /// ProtocolMetadata for the consumer group protocol.
  [<NoEquality;NoComparison>]
  type ConsumerGroupProtocolMetadata =
    struct
      val version : ConsumerGroupProtocolMetadataVersion
      val subscription : TopicName[]
      val userData : System.ArraySegment<byte>
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

    static member read buf =
      let version,buf = Binary.readInt16 buf
      let subs,buf = Binary.readArray (Binary.readString) buf
      let userData,buf = Binary.readBytes buf
      ConsumerGroupProtocolMetadata(version,subs,userData),buf

  type AssignmentStrategyName = string

  [<NoEquality;NoComparison>]
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
  [<NoEquality;NoComparison>]
  type ConsumerGroupMemberAssignment =
    struct
      val version : ConsumerGroupProtocolMetadataVersion
      val partitionAssignment : PartitionAssignment
      val userData : System.ArraySegment<byte>
      new (version, partitionAssignment, userData) = 
        { version = version; partitionAssignment = partitionAssignment ; userData = userData }
    end
  with

    static member size (x:ConsumerGroupMemberAssignment) =
      Binary.sizeInt16 x.version + PartitionAssignment.size x.partitionAssignment + Binary.sizeBytes x.userData

    static member write (x:ConsumerGroupMemberAssignment) buf =
      let buf = Binary.writeInt16 x.version buf
      let buf = PartitionAssignment.write x.partitionAssignment buf
      let buf = Binary.writeBytes x.userData buf
      buf

    static member read buf =
      let version, buf = Binary.readInt16 buf
      let assignments, buf = PartitionAssignment.read buf
      let userData, buf = Binary.readBytes buf
      (ConsumerGroupMemberAssignment(version, assignments, userData), buf)

  // Administrative API

  [<NoEquality;NoComparison>]
  type ListGroupsRequest =
    struct
    end
  with
    static member Size (_:ApiVersion, _:ListGroupsRequest) = 0
    static member Write (_:ApiVersion, _:ListGroupsRequest, _:BinaryZipper) = ()

  type ListGroupsResponse =
    struct
      val errorCode : ErrorCode
      val groups : (GroupId * ProtocolType)[]
      val throttleTimeMs : ThrottleTime
      new (errorCode, groups, throttleTimeMs) = { errorCode = errorCode; groups = groups ; throttleTimeMs = throttleTimeMs }
    end
  with
    static member Read (ver: ApiVersion, buf: BinaryZipper) =
      let readGroup (buf: BinaryZipper) =
        let groupId = buf.ReadString()
        let protocolType = buf.ReadString()
        groupId, protocolType

      match ver with
      | 0s ->
        let errorCode = buf.ReadInt16()
        let groups = buf.ReadArray readGroup
        ListGroupsResponse(errorCode, groups, 0)
      | 1s ->
        let throttleTime = buf.ReadInt32()
        let errorCode = buf.ReadInt16()
        let groups = buf.ReadArray readGroup
        ListGroupsResponse(errorCode, groups, throttleTime)
      | _ ->
        failwithf "Unsupported ListGroups Response API Version: %i" ver


  type State = string

  type Protocol = string

  type ClientHost = string

  [<NoEquality;NoComparison>]
  type GroupMembers =
    struct
      val members : (MemberId * ClientId * ClientHost * MemberMetadata * MemberAssignment)[]
      new (members) = { members = members }
    end
  with
    static member Read (_:ApiVersion, buf: BinaryZipper) =
      let readGroupMember (buf: BinaryZipper) =
        let memberId = buf.ReadString()
        let clientId = buf.ReadString()
        let clientHost = buf.ReadString()
        let memberMetadata = buf.ReadBytes()
        let memberAssignment = buf.ReadBytes()
        memberId, clientId, clientHost, memberMetadata, memberAssignment
      
      let members = buf.ReadArray readGroupMember
      GroupMembers(members)

  [<NoEquality;NoComparison>]
  type DescribeGroupsRequest =
    struct
      val groupIds : GroupId[]
      new (groupIds) = { groupIds = groupIds }
    end
  with
    static member Size (_: ApiVersion, req: DescribeGroupsRequest) =
      Binary.sizeArray req.groupIds Binary.sizeString
    
    static member Write (_: ApiVersion, req:DescribeGroupsRequest, buf:BinaryZipper) =
      let writeGroup (buf: BinaryZipper, groupId) =
        buf.WriteString groupId

      buf.WriteArray (req.groupIds, writeGroup)

  [<NoEquality;NoComparison>]
  type DescribeGroupsResponse =
    struct
      val throttleTime : ThrottleTime
      val groups : (ErrorCode * GroupId * State * ProtocolType * Protocol * GroupMembers)[]
      new (groups, throttleTime) = { groups = groups; throttleTime = throttleTime }
    end
  with
    static member Read (ver:ApiVersion, buf:BinaryZipper) =
      let readGroup (buf: BinaryZipper) =
        let errorCode = buf.ReadInt16()
        let groupId = buf.ReadString()
        let state = buf.ReadString()
        let protocolType = buf.ReadString()
        let protocol = buf.ReadString()
        let members = GroupMembers.Read(ver, buf)
        errorCode, groupId, state, protocolType, protocol, members

      match ver with
      | 0s ->
        let groups = buf.ReadArray readGroup
        DescribeGroupsResponse(groups, 0)
      | 1s ->
        let throttleTimeMs = buf.ReadInt32()
        let groups = buf.ReadArray readGroup
        DescribeGroupsResponse(groups, throttleTimeMs)
      | _ -> 
        failwithf "Unsupported DescribeGroups API Response Version: %i" ver
            

  [<NoEquality;NoComparison>]
  type ApiVersionsRequest =
    struct end
    with
      static member Size (_:ApiVersionsRequest) = 0
      static member Write (_:ApiVersionsRequest, _:BinaryZipper) = ()

  type MinVersion = int16
  type MaxVersion = int16

  [<NoEquality;NoComparison>]
  type ApiVersionsResponse =
    struct
      val errorCode : ErrorCode
      val throttleTimeMs : ThrottleTime
      val apiVersions : (ApiKey * MinVersion * MaxVersion)[]
      new (ec,apiVersions, throttleTimeMs) = { errorCode = ec ; apiVersions = apiVersions ; throttleTimeMs = throttleTimeMs }
    end
    with
      static member Read (ver:ApiVersion, buf:BinaryZipper) =
        let errorCode = buf.ReadInt16 ()
        let apiVersions = buf.ReadArray (fun buf ->
          let apiKey : ApiKey = enum<ApiKey> (int (buf.ReadInt16 ()))
          let min = buf.ReadInt16 ()
          let max = buf.ReadInt16 ()
          apiKey,min,max)
        let throttleTimeMs =
          if ver >= 1s then buf.ReadInt32 () else 0
        ApiVersionsResponse(errorCode,apiVersions, throttleTimeMs)
        

  /// A Kafka request message.
  type RequestMessage =
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
    | ApiVersions of ApiVersionsRequest
  with

    static member internal size (ver:ApiVersion, x:RequestMessage) =
      match x with
      | Heartbeat x -> HeartbeatRequest.Size (ver,x)
      | Metadata x -> MetadataRequest.Size (ver,x)
      | Fetch x -> FetchRequest.Size (ver,x)
      | Produce x -> ProduceRequest.Size (ver,x)
      | Offset x -> OffsetRequest.Size (ver,x)
      | GroupCoordinator x -> GroupCoordinatorRequest.Size (ver,x)
      | OffsetCommit x -> OffsetCommitRequest.Size (ver,x)
      | OffsetFetch x -> OffsetFetchRequest.Size (ver,x)
      | JoinGroup x -> JoinGroupRequest.Size (ver,x)
      | SyncGroup x -> SyncGroupRequest.Size (ver,x)
      | LeaveGroup x -> LeaveGroupRequest.Size (ver,x)
      | ListGroups x -> ListGroupsRequest.Size (ver,x)
      | DescribeGroups x -> DescribeGroupsRequest.Size (ver,x)
      | ApiVersions x -> ApiVersionsRequest.Size x

    static member internal Write (ver:ApiVersion, x:RequestMessage, buf:BinaryZipper) =
      match x with
      | Heartbeat x -> HeartbeatRequest.Write (ver,x,buf)
      | Metadata x -> MetadataRequest.Write (ver,x,buf)
      | Fetch x -> FetchRequest.Write (ver,x,buf)
      | Produce x -> ProduceRequest.Write (ver,x,buf)
      | Offset x -> OffsetRequest.Write (ver,x,buf)
      | GroupCoordinator x -> GroupCoordinatorRequest.Write (ver,x,buf)
      | OffsetCommit x -> OffsetCommitRequest.Write (ver,x,buf)
      | OffsetFetch x -> OffsetFetchRequest.Write (ver, x, buf)
      | JoinGroup x -> JoinGroupRequest.Write (ver,x,buf) 
      | SyncGroup x -> SyncGroupRequest.Write (ver,x,buf)
      | LeaveGroup x -> LeaveGroupRequest.Write (ver,x,buf)
      | ListGroups x -> ListGroupsRequest.Write (ver,x,buf)
      | DescribeGroups x -> DescribeGroupsRequest.Write (ver,x,buf) 
      | ApiVersions x -> ApiVersionsRequest.Write (x,buf)

    member x.ApiKey =
      match x with
      | Metadata _ -> ApiKey.Metadata
      | Fetch _ -> ApiKey.Fetch
      | Produce _ -> ApiKey.Produce
      | Offset _ -> ApiKey.Offset
      | GroupCoordinator _ -> ApiKey.GroupCoordinator
      | OffsetCommit _ -> ApiKey.OffsetCommit
      | OffsetFetch _ -> ApiKey.OffsetFetch
      | JoinGroup _ -> ApiKey.JoinGroup
      | SyncGroup _ -> ApiKey.SyncGroup
      | Heartbeat _ -> ApiKey.Heartbeat
      | LeaveGroup _ -> ApiKey.LeaveGroup
      | ListGroups _ -> ApiKey.ListGroups
      | DescribeGroups _ -> ApiKey.DescribeGroups
      | ApiVersions _ -> ApiKey.ApiVersions

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

    static member internal size (ver:ApiVersion, x:Request) =
      Binary.sizeInt16 (int16 x.apiKey) +
      Binary.sizeInt16 x.apiVersion +
      Binary.sizeInt32 x.correlationId +
      Binary.sizeString x.clientId +
      RequestMessage.size (ver, x.message)

    static member internal Write (ver:ApiVersion, x:Request, buf:BinaryZipper) =
      buf.WriteInt16 (int16 x.apiKey)
      buf.WriteInt16 (x.apiVersion)
      buf.WriteInt32 (x.correlationId)
      buf.WriteString (x.clientId)
      RequestMessage.Write (ver, x.message, buf)

  /// A Kafka response message.
  type ResponseMessage =
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
    | ApiVersionsResponse of ApiVersionsResponse
  with

    /// Decodes the response given the specified ApiKey corresponding to the request.
    static member internal Read (apiKey:ApiKey, apiVer:ApiVersion, buf:BinaryZipper) : ResponseMessage =
      match apiKey with
      | ApiKey.Heartbeat -> HeartbeatResponse.Read (apiVer,buf) |> ResponseMessage.HeartbeatResponse 
      | ApiKey.Metadata -> MetadataResponse.Read (apiVer,buf) |> ResponseMessage.MetadataResponse 
      | ApiKey.Fetch -> FetchResponse.Read (apiVer,buf) |> ResponseMessage.FetchResponse
      | ApiKey.Produce -> ProduceResponse.Read (apiVer,buf) |> ResponseMessage.ProduceResponse
      | ApiKey.Offset -> OffsetResponse.Read (apiVer,buf) |> ResponseMessage.OffsetResponse
      | ApiKey.GroupCoordinator -> GroupCoordinatorResponse.Read (apiVer, buf) |> ResponseMessage.GroupCoordinatorResponse 
      | ApiKey.OffsetCommit -> OffsetCommitResponse.Read (apiVer,buf) |> ResponseMessage.OffsetCommitResponse 
      | ApiKey.OffsetFetch -> OffsetFetchResponse.Read(apiVer,buf) |> ResponseMessage.OffsetFetchResponse
      | ApiKey.JoinGroup -> JoinGroupResponse.Read (apiVer,buf) |> ResponseMessage.JoinGroupResponse
      | ApiKey.SyncGroup -> SyncGroupResponse.Read (apiVer,buf) |> ResponseMessage.SyncGroupResponse
      | ApiKey.LeaveGroup -> LeaveGroupResponse.Read (apiVer,buf) |> ResponseMessage.LeaveGroupResponse
      | ApiKey.ListGroups -> ListGroupsResponse.Read (apiVer,buf) |> ResponseMessage.ListGroupsResponse
      | ApiKey.DescribeGroups -> DescribeGroupsResponse.Read (apiVer,buf) |> ResponseMessage.DescribeGroupsResponse 
      | ApiKey.ApiVersions -> ApiVersionsResponse.Read (apiVer,buf) |> ResponseMessage.ApiVersionsResponse
      | x -> 
        failwith (sprintf "Unsupported ApiKey=%A" x)

  /// A Kafka response envelope.
  type Response =
    struct
      val correlationId : CorrelationId
      val message : ResponseMessage
      new (correlationId, message) = { correlationId = correlationId; message = message }
    end

