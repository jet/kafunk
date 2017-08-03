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

  /// The maximum bytes to include in the message set for this partition. This
  /// helps bound the size of the response.
  type MaxBytes = int32

  /// The offset at the end of the log for this partition. This can be used by
  /// the client to determine how many messages behind the end of the log they
  /// are.
  type HighwaterMarkOffset = int64

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

    static member internal Size (ver:ApiVersion, m:Message) =
      Binary.sizeInt32 m.crc +
      Binary.sizeInt8 m.magicByte +
      Binary.sizeInt8 m.attributes +
      (if ver >= 1s then Binary.sizeInt64 m.timestamp else 0) +
      Binary.sizeBytes m.key +
      Binary.sizeBytes m.value

    static member internal Write (ver:ApiVersion, m:Message, buf:BinaryZipper) =
      buf.ShiftOffset 4 // CRC
      let offsetAfterCrc = buf.Buffer.Offset
      buf.WriteInt8 m.magicByte
      buf.WriteInt8 m.attributes
      if ver >= 1s then
        buf.WriteInt64 m.timestamp
      buf.WriteBytes m.key
      buf.WriteBytes m.value
      let crc = Crc.crc32 buf.Buffer.Array offsetAfterCrc (buf.Buffer.Offset - offsetAfterCrc)
      let crcBuf = System.ArraySegment<_>(buf.Buffer.Array, offsetAfterCrc - 4, 4)
      Binary.pokeInt32 (int crc) crcBuf |> ignore

    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
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
    static member internal ComputeCrc (ver:ApiVersion, m:Message) =
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
    static member internal CheckCrc (ver:ApiVersion, m:Message) =
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

    static member internal Size (ver:ApiVersion, x:MessageSet) =
      let mutable size = 0
      for i = 0 to x.messages.Length - 1 do
        let m = x.messages.[i].message
        size <- size + 8 + 4 + (Message.Size (ver,m))
      size

    static member internal Write (messageVer:ApiVersion, ms:MessageSet, buf:BinaryZipper) =
      //for (o,ms,m) in ms.messages do
      for x in ms.messages do
        buf.WriteInt64 x.offset
        buf.WriteInt32 x.messageSize
        Message.Write (messageVer, x.message, buf)

    // NB: skipTooLarge=true is for scenarios where decompression is involved and a message set is being decoded from an individual message
    // which was itself too small.
    static member internal Read (messageVer:ApiVersion, partition:Partition, ec:ErrorCode, messageSetSize:int, skipTooLarge:bool, buf:BinaryZipper) =
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
  [<AutoSerializable(false);StructuralEquality;StructuralComparison>]
  type Broker =
    struct
      val nodeId : NodeId
      val host : Host
      val port : Port
      new (nodeId, host, port) = { nodeId = nodeId; host = host; port = port }
    end
  with

    static member internal read buf =
      let (nodeId, host, port), buf = Binary.read3 Binary.readInt32 Binary.readString Binary.readInt32 buf
      (Broker(nodeId, host, port), buf)

  [<NoEquality;NoComparison;AutoSerializable(false)>]
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

    static member internal read buf =
      let partitionErrorCode, buf = Binary.readInt16 buf
      let partitionId, buf = Binary.readInt32 buf
      let leader, buf = Binary.readInt32 buf
      let replicas, buf = Binary.readArray Binary.readInt32 buf
      let isr, buf = Binary.readArray Binary.readInt32 buf
      (PartitionMetadata(partitionErrorCode, partitionId, leader, replicas, isr), buf)

  /// Metadata for a specific topic consisting of a set of partition-to-broker assignments.
  [<NoEquality;NoComparison>]
  type TopicMetadata =
    struct
      val topicErrorCode : TopicErrorCode
      val topicName : TopicName
      val partitionMetadata : PartitionMetadata[]
      new (topicErrorCode, topicName, partitionMetadata) =
        { topicErrorCode = topicErrorCode; topicName = topicName; partitionMetadata = partitionMetadata }
    end
  with

    static member internal read buf =
      let errorCode, buf = Binary.readInt16 buf
      let topicName, buf = Binary.readString buf
      let partitionMetadata, buf = Binary.readArray PartitionMetadata.read buf
      (TopicMetadata(errorCode, topicName, partitionMetadata), buf)

  /// Contains a list of all brokers (node id, host, post) and assignment of topic/partitions to brokers.
  /// The assignment consists of a leader, a set of replicas and a set of in-sync replicas.
  [<NoEquality;NoComparison>]
  type MetadataResponse =
    struct
      val brokers : Broker[]
      val topicMetadata : TopicMetadata[]
      new (brokers, topicMetadata) =  { brokers = brokers; topicMetadata = topicMetadata }
    end
  with

    static member internal read buf =
      let brokers, buf = Binary.readArray Broker.read buf
      let topicMetadata, buf = Binary.readArray TopicMetadata.read buf
      (MetadataResponse(brokers, topicMetadata), buf)

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
      val requiredAcks : RequiredAcks
      val timeout : Timeout
      val topics : ProduceRequestTopicMessageSet[]
      new (requiredAcks, timeout, topics) =
        { requiredAcks = requiredAcks; timeout = timeout; topics = topics }
    end
  with

    static member internal Size (x:ProduceRequest) =
      let mutable size = 0
      size <- size + 2 // requiredAcks
      size <- size + 4 // timeout
      size <- size + 4 // topics array size
      for i = 0 to x.topics.Length - 1 do
        let y = x.topics.[i]
        size <- size + (Binary.sizeString y.topic)
        size <- size + 4 // partition array size
        for z in y.partitions do
          let mss = z.messageSetSize
          size <- size + 4 + 4 + mss
      size

    static member internal Write (ver:ApiVersion, x:ProduceRequest, buf:BinaryZipper) =
      let messageVer = MessageVersions.produceReqMessage ver
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
      new (p,ec,o,ts) = { partition = p ; errorCode = ec ; offset = o ; timestamp = ts }
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

    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
      let tn = buf.ReadInt32 ()
      let topics = Array.zeroCreate tn
      for i = 0 to topics.Length - 1 do
        let t = buf.ReadString ()
        let psn = buf.ReadInt32 ()
        let ps = Array.zeroCreate psn
        for j = 0 to ps.Length - 1 do
          let p = buf.ReadInt32 ()
          let ec = buf.ReadInt16 ()
          let o = buf.ReadInt64 ()
          let ts = 
            if ver >= 2s then buf.ReadInt64 ()
            else 0L
          ps.[j] <- ProduceResponsePartitionItem(p,ec,o,ts)
        topics.[i] <- ProduceResponseTopicItem(t,ps)
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
      val topics : (TopicName * (Partition * FetchOffset * MaxBytes)[])[]
      new (replicaId, maxWaitTime, minBytes, topics) =
        { replicaId = replicaId; maxWaitTime = maxWaitTime; minBytes = minBytes; topics = topics }
    end
  with

    static member internal Size (x:FetchRequest) =
      let mutable size = 0
      size <- size + 4 // replicaId
      size <- size + 4 // maxWaitTime
      size <- size + 4 // minBytes
      size <- size + 4 // topic array size
      for i = 0 to x.topics.Length - 1 do
        let (t,ps) = x.topics.[i]
        size <- size + (Binary.sizeString t)
        size <- size + 4 // partition array size
        for (_,_,_) in ps do
          size <- size + 4 // partition
          size <- size + 8 // offset
          size <- size + 4 // maxBytes
      size

    static member internal Write (x:FetchRequest, buf:BinaryZipper) =
      buf.WriteInt32 x.replicaId
      buf.WriteInt32 x.maxWaitTime
      buf.WriteInt32 x.minBytes
      buf.WriteInt32 x.topics.Length
      for i = 0 to x.topics.Length - 1 do
        let (t,ps) = x.topics.[i]
        buf.WriteString t
        buf.WriteInt32 ps.Length
        for j = 0 to ps.Length - 1 do
          let (p,o,mb) = ps.[j]
          buf.WriteInt32 p
          buf.WriteInt64 o
          buf.WriteInt32 mb

  [<NoEquality;NoComparison>]
  type FetchResponse =
    struct
      val throttleTime : ThrottleTime
      val topics : (TopicName * (Partition * ErrorCode * HighwaterMarkOffset * MessageSetSize * MessageSet)[])[]
      new (tt,topics) = { throttleTime = tt ; topics = topics }
    end
  with

    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
      let throttleTime =
        match ver with
        | v when v >= 1s -> buf.ReadInt32 ()
        | _ -> 0
      let nt = buf.ReadInt32()
      let topics = Array.zeroCreate nt
      for i = 0 to topics.Length - 1 do
        let t = buf.ReadString ()
        let nps = buf.ReadInt32 ()
        let ps = Array.zeroCreate nps
        for j = 0 to ps.Length - 1 do
          let partition = buf.ReadInt32 ()
          let errorCode = buf.ReadInt16 ()
          let hwo = buf.ReadInt64 ()
          let mss = buf.ReadInt32 ()
          let ms = MessageSet.Read (MessageVersions.fetchResMessage ver,partition,errorCode,mss,false,buf)
          ps.[j] <-  partition, errorCode, hwo, mss, ms
        topics.[i] <- (t,ps)
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

    static member internal Size (ver:ApiVersion, x:OffsetRequest) =
      let partitionSize (part, time, maxNumOffsets) =
        Binary.sizeInt32 part + 
        Binary.sizeInt64 time + 
        (if ver = 0s then Binary.sizeInt32 maxNumOffsets else 0)
      let topicSize (name, partitions) =
        Binary.sizeString name + Binary.sizeArray partitions partitionSize
      Binary.sizeInt32 x.replicaId + Binary.sizeArray x.topics topicSize

    static member internal Write (apiVer:ApiVersion, x:OffsetRequest, buf:BinaryZipper) =
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

    static member internal Read (ver:ApiVersion, buf:BinaryZipper) =
      let readPartition (buf:BinaryZipper) =
        let p = buf.ReadInt32 ()
        let ec = buf.ReadInt16 ()
        let ts = 
          if ver >= 1s then buf.ReadInt64 ()
          else 0L
        let os = 
          if ver >= 1s then [|buf.ReadInt64 ()|]
          else buf.ReadArray (fun buf -> buf.ReadInt64 ())
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

//    static member internal size (x:OffsetCommitRequest) =
//      let partitionSize (part, offset, ts, metadata) =
//        Binary.sizeInt32 part + Binary.sizeInt64 offset + Binary.sizeString metadata
//      let topicSize (name, partitions) =
//        Binary.sizeString name + Binary.sizeArray partitions partitionSize
//      Binary.sizeString x.consumerGroup +
//      Binary.sizeInt32 x.consumerGroupGenerationId +
//      Binary.sizeString x.consumerId +
//      Binary.sizeInt64 x.retentionTime +
//      Binary.sizeArray x.topics topicSize

//    static member internal write (x:OffsetCommitRequest) buf =
//      let writePartition =
//        Binary.write3 Binary.writeInt32 Binary.writeInt64 Binary.writeString
//      let writeTopic =
//        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps writePartition)
//      buf
//      |> Binary.writeString x.consumerGroup
//      |> Binary.writeInt32 x.consumerGroupGenerationId
//      |> Binary.writeString x.consumerId
//      |> Binary.writeInt64 x.retentionTime
//      |> Binary.writeArray x.topics writeTopic

    static member internal Size (ver:ApiVersion, x:OffsetCommitRequest) =
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

    static member internal Write (ver:ApiVersion, x:OffsetCommitRequest, buf:BinaryZipper) =
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
      val topics : (TopicName * (Partition * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end
  with

    static member internal read buf =
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

    static member internal size (x:OffsetFetchRequest) =
      let topicSize (name, parts) =
        Binary.sizeString name + Binary.sizeArray parts Binary.sizeInt32
      Binary.sizeString x.consumerGroup + Binary.sizeArray x.topics topicSize

    static member internal write (x:OffsetFetchRequest) buf =
      let writeTopic =
        Binary.write2 Binary.writeString (fun ps -> Binary.writeArray ps Binary.writeInt32)
      buf
      |> Binary.writeString x.consumerGroup
      |> Binary.writeArray x.topics writeTopic

  [<NoEquality;NoComparison>]
  type OffsetFetchResponse =
    struct
      val topics : (TopicName * (Partition * Offset * Meta * ErrorCode)[])[]
      new (topics) = { topics = topics }
    end
  with

    static member internal read buf =
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
  [<NoEquality;NoComparison>]
  type GroupCoordinatorRequest =
    struct
      val groupId : GroupId
      new (groupId) = { groupId = groupId }
    end
  with

    static member internal size (x:GroupCoordinatorRequest) =
      Binary.sizeString x.groupId

    static member internal write (x:GroupCoordinatorRequest) buf =
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

    static member internal  read buf =
      let ec, buf = Binary.readInt16 buf
      let cid, buf = Binary.readInt32 buf
      let ch, buf = Binary.readString buf
      let cp, buf = Binary.readInt32 buf
      (GroupCoordinatorResponse(ec, cid, ch, cp), buf)

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

    static member internal size (x:GroupProtocols) =
      let protocolSize (name, metadata) =
        Binary.sizeString name + Binary.sizeBytes metadata
      Binary.sizeArray x.protocols protocolSize

    static member internal write (x:GroupProtocols) buf =
      buf |> Binary.writeArray x.protocols (Binary.write2 Binary.writeString Binary.writeBytes)

  [<NoEquality;NoComparison>]
  type Members =
    struct
      val members : (MemberId * MemberMetadata)[]
      new (members) = { members = members }
    end
  with

    static member internal read buf =
      let readMember =
        Binary.read2 Binary.readString Binary.readBytes
      let xs, buf = buf |> Binary.readArray readMember
      (Members(xs), buf)

  module JoinGroup =

    [<Struct>]
    [<NoEquality;NoComparison>]
    type Request =
      val groupId : GroupId
      val sessionTimeout : SessionTimeout
      val rebalanceTimeout : SessionTimeout
      val memberId : MemberId
      val protocolType : ProtocolType
      val groupProtocols : GroupProtocols
      new (groupId, sessionTimeout, rebalanceTimeout, memberId, protocolType, groupProtocols) =
        { groupId = groupId; sessionTimeout = sessionTimeout; rebalanceTimeout = rebalanceTimeout ; memberId = memberId;
          protocolType = protocolType; groupProtocols = groupProtocols }

    [<Struct>]
    [<NoEquality;NoComparison>]
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

    let internal sizeRequest (ver:ApiVersion, req:Request) =
      Binary.sizeString req.groupId +
      Binary.sizeInt32 req.sessionTimeout +
      (if ver >= 1s then 4 else 0) +
      Binary.sizeString req.memberId +
      Binary.sizeString req.protocolType +
      GroupProtocols.size req.groupProtocols

    let internal writeRequest (ver:ApiVersion, req:Request) buf =
      let buf = Binary.writeString req.groupId buf
      let buf = Binary.writeInt32 req.sessionTimeout buf
      let buf = 
        if ver >= 1s then Binary.writeInt32 req.rebalanceTimeout buf
        else buf
      let buf = Binary.writeString req.memberId buf
      let buf = Binary.writeString req.protocolType buf
      let buf = GroupProtocols.write req.groupProtocols buf
      buf

    let internal readResponse buf =
      let (errorCode, gid, gp, lid, mid, ms), buf =
        buf |> Binary.read6
          Binary.readInt16
          Binary.readInt32
          Binary.readString
          Binary.readString
          Binary.readString
          Members.read
      (Response(errorCode, gid, gp, lid, mid, ms), buf)

  [<NoEquality;NoComparison>]
  type GroupAssignment =
    struct
      val members : (MemberId * MemberAssignment)[]
      new (members) = { members = members }
    end
  with

    static member internal size (x:GroupAssignment) =
      Binary.sizeArray x.members (fun (memId, memAssign) -> Binary.sizeString memId + Binary.sizeBytes memAssign)

    static member internal write (x:GroupAssignment) buf =
      buf |> Binary.writeArray x.members (Binary.write2 Binary.writeString Binary.writeBytes)

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

    static member internal size (x:SyncGroupRequest) =
      Binary.sizeString x.groupId +
      Binary.sizeInt32 x.generationId +
      Binary.sizeString x.memberId +
      GroupAssignment.size x.groupAssignment

    static member internal write (x:SyncGroupRequest) buf =
      let buf =
        buf
        |> Binary.writeString x.groupId
        |> Binary.writeInt32 x.generationId
        |> Binary.writeString x.memberId
      GroupAssignment.write x.groupAssignment buf

  [<NoEquality;NoComparison>]
  type SyncGroupResponse =
    struct
      val errorCode : ErrorCode
      val memberAssignment : MemberAssignment
      new (errorCode, memberAssignment) = { errorCode = errorCode; memberAssignment = memberAssignment }
    end
  with

    static member internal read buf =
      let errorCode, buf = Binary.readInt16 buf
      let ma, buf = Binary.readBytes buf
      (SyncGroupResponse(errorCode, ma), buf)

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

    static member internal size (x:HeartbeatRequest) =
      Binary.sizeString x.groupId + Binary.sizeInt32 x.generationId + Binary.sizeString x.memberId

    static member internal write (x:HeartbeatRequest) buf =
      buf
      |> Binary.writeString x.groupId
      |> Binary.writeInt32 x.generationId
      |> Binary.writeString x.memberId

  /// Heartbeat response from the group coordinator.
  [<NoEquality;NoComparison>]
  type HeartbeatResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end
  with

    static member internal read buf =
      let errorCode, buf = Binary.readInt16 buf
      (HeartbeatResponse(errorCode), buf)

  /// An explciti request to leave a group. Preferred over session timeout.
  [<NoEquality;NoComparison>]
  type LeaveGroupRequest =
    struct
      val groupId : GroupId
      val memberId : MemberId
      new (groupId, memberId) = { groupId = groupId; memberId = memberId }
    end
  with

    static member internal size (x:LeaveGroupRequest) =
      Binary.sizeString x.groupId + Binary.sizeString x.memberId

    static member internal write (x:LeaveGroupRequest) buf =
      buf |> Binary.writeString x.groupId |> Binary.writeString x.memberId

  [<NoEquality;NoComparison>]
  type LeaveGroupResponse =
    struct
      val errorCode : ErrorCode
      new (errorCode) = { errorCode = errorCode }
    end
  with

    static member internal read buf =
      let errorCode, buf = Binary.readInt16 buf
      (LeaveGroupResponse(errorCode), buf)

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

    static member internal size (x:ConsumerGroupProtocolMetadata) =
      Binary.sizeInt16 x.version +
      Binary.sizeArray x.subscription Binary.sizeString +
      Binary.sizeBytes x.userData

    static member internal write (x:ConsumerGroupProtocolMetadata) buf =
      buf
      |> Binary.writeInt16 x.version
      |> Binary.writeArray x.subscription Binary.writeString
      |> Binary.writeBytes x.userData

    static member internal read buf =
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

    static member internal size (x:PartitionAssignment) =
      let topicSize (name, parts) =
        Binary.sizeString name + Binary.sizeArray parts Binary.sizeInt32
      Binary.sizeArray x.assignments topicSize

    static member internal write (x:PartitionAssignment) buf =
      let writePartitions partitions = Binary.writeArray partitions Binary.writeInt32
      buf |> Binary.writeArray x.assignments (Binary.write2 Binary.writeString writePartitions)

    static member internal read buf =
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

    static member internal size (x:ConsumerGroupMemberAssignment) =
      Binary.sizeInt16 x.version + PartitionAssignment.size x.partitionAssignment + Binary.sizeBytes x.userData

    static member internal write (x:ConsumerGroupMemberAssignment) buf =
      let buf = Binary.writeInt16 x.version buf
      let buf = PartitionAssignment.write x.partitionAssignment buf
      let buf = Binary.writeBytes x.userData buf
      buf

    static member internal read buf =
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

    static member size (_:ListGroupsRequest) = 0

    static member write (_:ListGroupsRequest) buf = buf

  type ListGroupsResponse =
    struct
      val errorCode : ErrorCode
      val groups : (GroupId * ProtocolType)[]
      new (errorCode, groups) = { errorCode = errorCode; groups = groups }
    end
  with

    static member internal read buf =
      let readGroup =
        Binary.read2 Binary.readString Binary.readString
      let errorCode, buf = Binary.readInt16 buf
      let gs, buf = buf |> Binary.readArray readGroup
      (ListGroupsResponse(errorCode, gs), buf)

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

    static member internal read buf =
      let readGroupMember =
        Binary.read5 Binary.readString Binary.readString Binary.readString Binary.readBytes Binary.readBytes
      let xs, buf = buf |> Binary.readArray readGroupMember
      (GroupMembers(xs), buf)

  [<NoEquality;NoComparison>]
  type DescribeGroupsRequest =
    struct
      val groupIds : GroupId[]
      new (groupIds) = { groupIds = groupIds }
    end
  with

    static member internal size (x:DescribeGroupsRequest) =
      Binary.sizeArray x.groupIds Binary.sizeString

    static member internal write (x:DescribeGroupsRequest) buf =
      buf |> Binary.writeArray x.groupIds Binary.writeString


  [<NoEquality;NoComparison>]
  type DescribeGroupsResponse =
    struct
      val groups : (ErrorCode * GroupId * State * ProtocolType * Protocol * GroupMembers)[]
      new (groups) = { groups = groups }
    end
  with

    static member internal read buf =
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

  [<NoEquality;NoComparison>]
  type ApiVersionsRequest =
    struct end
    with
      static member Size (_:ApiVersionsRequest) = 0
      static member internal Write (_:ApiVersionsRequest, _:BinaryZipper) = ()

  type MinVersion = int16
  type MaxVersion = int16

  [<NoEquality;NoComparison>]
  type ApiVersionsResponse =
    struct
      val errorCode : ErrorCode
      val apiVersions : (ApiKey * MinVersion * MaxVersion)[]
      new (ec,apiVersions) = { errorCode = ec ; apiVersions = apiVersions }
    end
    with
      static member internal Read (_:ApiVersion, buf:BinaryZipper) =
        let ec = buf.ReadInt16 ()
        let apiVersions = buf.ReadArray (fun buf ->
          let apiKey : ApiKey = enum<ApiKey> (int (buf.ReadInt16 ()))
          let min = buf.ReadInt16 ()
          let max = buf.ReadInt16 ()
          apiKey,min,max)
        ApiVersionsResponse(ec,apiVersions)
        

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
    | ApiVersions of ApiVersionsRequest
  with

    static member internal size (ver:ApiVersion, x:RequestMessage) =
      match x with
      | Heartbeat x -> HeartbeatRequest.size x
      | Metadata x -> Metadata.sizeRequest x
      | Fetch x -> FetchRequest.Size x
      | Produce x -> ProduceRequest.Size x
      | Offset x -> OffsetRequest.Size (ver,x)
      | GroupCoordinator x -> GroupCoordinatorRequest.size x
      | OffsetCommit x -> OffsetCommitRequest.Size (ver,x)
      | OffsetFetch x -> OffsetFetchRequest.size x
      | JoinGroup x -> JoinGroup.sizeRequest (ver,x)
      | SyncGroup x -> SyncGroupRequest.size x
      | LeaveGroup x -> LeaveGroupRequest.size x
      | ListGroups x -> ListGroupsRequest.size x
      | DescribeGroups x -> DescribeGroupsRequest.size x
      | ApiVersions x -> ApiVersionsRequest.Size x

    static member internal Write (ver:ApiVersion, x:RequestMessage, buf:BinaryZipper) =
      match x with
      | Heartbeat x -> HeartbeatRequest.write x buf.Buffer |> ignore
      | Metadata x -> Metadata.writeRequest x buf.Buffer |> ignore
      | Fetch x -> FetchRequest.Write (x,buf)
      | Produce x -> ProduceRequest.Write (ver,x,buf)
      | Offset x -> OffsetRequest.Write (ver,x,buf)
      | GroupCoordinator x -> GroupCoordinatorRequest.write x buf.Buffer |> ignore
      | OffsetCommit x -> OffsetCommitRequest.Write (ver,x,buf)
      | OffsetFetch x -> OffsetFetchRequest.write x buf.Buffer |> ignore
      | JoinGroup x -> JoinGroup.writeRequest (ver,x) buf.Buffer |> ignore
      | SyncGroup x -> SyncGroupRequest.write x buf.Buffer |> ignore
      | LeaveGroup x -> LeaveGroupRequest.write x buf.Buffer |> ignore
      | ListGroups x -> ListGroupsRequest.write x buf.Buffer |> ignore
      | DescribeGroups x -> DescribeGroupsRequest.write x buf.Buffer |> ignore
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
    | JoinGroupResponse of JoinGroup.Response
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
      | ApiKey.Heartbeat ->
        let x, _ = HeartbeatResponse.read buf.Buffer in (ResponseMessage.HeartbeatResponse x)
      | ApiKey.Metadata ->
        let x, _ = MetadataResponse.read buf.Buffer in (ResponseMessage.MetadataResponse x)
      | ApiKey.Fetch -> FetchResponse.Read (apiVer,buf) |> ResponseMessage.FetchResponse
      | ApiKey.Produce -> ProduceResponse.Read (apiVer,buf) |> ResponseMessage.ProduceResponse
      | ApiKey.Offset -> OffsetResponse.Read (apiVer, buf) |> ResponseMessage.OffsetResponse
      | ApiKey.GroupCoordinator ->
        let x, _ = GroupCoordinatorResponse.read buf.Buffer in (ResponseMessage.GroupCoordinatorResponse x)
      | ApiKey.OffsetCommit ->
        let x, _ = OffsetCommitResponse.read buf.Buffer in (ResponseMessage.OffsetCommitResponse x)
      | ApiKey.OffsetFetch ->
        let x, _ = OffsetFetchResponse.read buf.Buffer in (ResponseMessage.OffsetFetchResponse x)
      | ApiKey.JoinGroup ->
        let x, _ = JoinGroup.readResponse buf.Buffer in (ResponseMessage.JoinGroupResponse x)
      | ApiKey.SyncGroup ->
        let x, _ = SyncGroupResponse.read buf.Buffer in (ResponseMessage.SyncGroupResponse x)
      | ApiKey.LeaveGroup ->
        let x, _ = LeaveGroupResponse.read buf.Buffer in (ResponseMessage.LeaveGroupResponse x)
      | ApiKey.ListGroups ->
        let x, _ = ListGroupsResponse.read buf.Buffer in (ResponseMessage.ListGroupsResponse x)
      | ApiKey.DescribeGroups ->
        let x, _ = DescribeGroupsResponse.read buf.Buffer in (ResponseMessage.DescribeGroupsResponse x)
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

