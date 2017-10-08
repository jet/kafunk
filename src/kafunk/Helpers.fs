[<AutoOpen>]
module internal Kafunk.Helpers

open Kafunk
open System
open System.Text

module Message =

  let create value key attrs =
    // NB: the CRC is computed by the Protocol module during encoding
    //Message(0, 0y, (defaultArg attrs 0y), DateTime.UtcNowUnixMilliseconds, key, value)
    Message(0, 0y, (defaultArg attrs 0y), 0L, key, value)

module MessageSet =

  let ofMessage (messageVer:ApiVersion) (m:Message) =
    MessageSet([| MessageSetItem(0L, Message.Size (messageVer,m), m) |])

  let ofMessages (messageVer:ApiVersion) ms =
    MessageSet(ms |> Seq.map (fun m -> MessageSetItem (0L, Message.Size (messageVer,m), m)) |> Seq.toArray)

  /// Returns the frist offset in the message set.
  let firstOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      //let (o,_,_) = ms.messages.[0] in o
       ms.messages.[0].offset
    else
      0L

  /// Returns the last offset in the message set.
  let lastOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      //let (o,_,_) = ms.messages.[ms.messages.Length - 1] in o
      ms.messages.[ms.messages.Length - 1].offset
    else
      0L

  /// Returns the next offset to fetch, by taking the max offset in the
  /// message set and adding 1.
  /// Ensures the next offset is bellow high watermark offset.
  let nextOffset (ms:MessageSet) (hwm:HighwaterMarkOffset) : Offset =
    let lastOffset = lastOffset ms
    let nextOffset = lastOffset + 1L
    if nextOffset <= hwm then
      nextOffset
    else 
      failwithf "invalid offset computation last_offset=%i hwm=%i" lastOffset hwm
      
  /// Returns the timestamp of the first message, if messages are available
  /// and the message has a timestamp.
  let firstTimestamp (ms:MessageSet) =
    if ms.messages.Length = 0 then None
    else
      let m = ms.messages.[0]
      if m.message.timestamp >= 0L then 
        Some (DateTime.FromUnixMilliseconds m.message.timestamp)
      else
        None

// -------------------------------------------------------------------------------------------------------------------------------------





// -------------------------------------------------------------------------------------------------------------------------------------

[<AutoOpen>]
module internal ResponseEx =

  let private wrongResponse () =
    failwith (sprintf "Wrong response!")

  type RequestMessage with
    /// If a request does not expect a response, returns the default response.
    /// Used to generate responses for requests which don't expect a response from the server.
    static member awaitResponse (x:RequestMessage) =
      match x with
      | RequestMessage.Produce req when req.requiredAcks = RequiredAcks.None ->
        Some(ResponseMessage.ProduceResponse(new ProduceResponse([||], 0)))
      | _ -> None

  type ResponseMessage with
    static member internal toFetch res = match res with FetchResponse x -> x | _ -> wrongResponse ()
    static member internal toProduce res = match res with ProduceResponse x -> x | _ -> wrongResponse ()
    static member internal toOffset res = match res with OffsetResponse x -> x | _ -> wrongResponse ()
    static member internal toGroupCoordinator res = match res with GroupCoordinatorResponse x -> x | _ -> wrongResponse ()
    static member internal toOffsetCommit res = match res with OffsetCommitResponse x -> x | _ -> wrongResponse ()
    static member internal toOffsetFetch res = match res with OffsetFetchResponse x -> x | _ -> wrongResponse ()
    static member internal toJoinGroup res = match res with JoinGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toSyncGroup res = match res with SyncGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toHeartbeat res = match res with HeartbeatResponse x -> x | _ -> wrongResponse ()
    static member internal toLeaveGroup res = match res with LeaveGroupResponse x -> x | _ -> wrongResponse ()
    static member internal toListGroups res = match res with ListGroupsResponse x -> x | _ -> wrongResponse ()
    static member internal toDescribeGroups res = match res with DescribeGroupsResponse x -> x | _ -> wrongResponse ()
    static member internal toMetadata res = match res with MetadataResponse x -> x | _ -> wrongResponse ()
    static member internal toApiVersions res = match res with ApiVersionsResponse x -> x | _ -> wrongResponse ()

  type Broker with
    static member endpoint (b:Broker) =
      String.Concat(b.host, ":", b.port)

// -------------------------------------------------------------------------------------------------------------------------------------



// ------------------------------------------------------------------------------------------------------------------------------
// printers

[<AutoOpen>]
module internal Printers =
  
  open System.Text

  let concatMapSbDo (sb:StringBuilder) (s:seq<'a>) (f:StringBuilder -> 'a -> _) (sep:string) =
    use en = s.GetEnumerator()
    if en.MoveNext () then
      f sb en.Current |> ignore
      while en.MoveNext () do
        sb.Append sep |> ignore
        f sb en.Current |> ignore

  let concatMapSb (s:seq<'a>) (f:StringBuilder -> 'a -> _) (sep:string) =
    let sb = StringBuilder()
    concatMapSbDo sb s f sep
    sb.ToString()

  let partitions (os:seq<Partition>) =
    concatMapSb os (fun sb (p) -> sb.AppendFormat("{0}", p)) ","

  let partitionCount (pc:int) =
    partitions (Seq.init pc id)

  let topicPartitions (os:seq<TopicName * Partition>) =
    concatMapSb os (fun sb (t,p) -> sb.AppendFormat("[t={0} p={1}]", t, p)) ","
    
  let partitionOffsetPairs (os:seq<Partition * Offset>) =
    concatMapSb os (fun sb (p,o) -> sb.AppendFormat("[p={0} o={1}]", p, o)) " ; "

  let partitionErrorCodePairs (os:seq<Partition * ErrorCode>) =
    concatMapSb os (fun sb (p,o) -> sb.AppendFormat("[p={0} ec={1}]", p, o)) " ; "
    
  let stringsCsv (ss:seq<#obj>) =
    concatMapSb ss (fun sb s -> sb.AppendFormat("{0}", s)) ","

  let partitionOffsetErrorCodes (xs:seq<Partition * Offset * ErrorCode>) =
    concatMapSb xs (fun sb (p,o,ec) -> sb.AppendFormat("[p={0} o={1} error_code={2}]", p, o, ec)) ","
    

  type MetadataResponse with
    static member Print (x:MetadataResponse) =
      let topics =
        x.topicMetadata
        |> Seq.map (fun tmd -> 
          let partitions = 
            tmd.partitionMetadata
            |> Seq.map (fun pmd -> sprintf "[p=%i leader=%i isr=%s]" pmd.partitionId pmd.leader (pmd.isr |> Seq.map string |> String.concat ","))
            |> String.concat " ; "
          sprintf "[topic=%s partitions=[%s]]" tmd.topicName partitions)
        |> String.concat " ; "
      let brokers =
        x.brokers
        |> Seq.map (fun b -> sprintf "[node_id=%i host=%s port=%i]" b.nodeId b.host b.port)
        |> String.concat " ; "
      sprintf "MetadataResponse|brokers=%s|topics=%s" brokers topics

  type ProduceRequest with
    static member Print (x:ProduceRequest) =
      let ts =
        x.topics
        |> Seq.map (fun x ->
          let ps =
            x.partitions
            |> Seq.map (fun y -> sprintf "p=%i mss=%i mc=%i" y.partition y.messageSetSize y.messageSet.messages.Length)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" x.topic ps)
        |> String.concat " ; "
      sprintf "ProduceRequest|required_acks=%i timeout=%i topics=[%s]" x.requiredAcks x.timeout ts

  type ProduceResponse with
    static member Print (x:ProduceResponse) =
      let ts =
        x.topics
        |> Seq.map (fun x ->
          let ps =
            x.partitions
            |> Seq.map (fun y -> sprintf "p=%i o=%i error_code=%i" y.partition y.offset y.errorCode)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" x.topic ps)
        |> String.concat " ; "
      sprintf "ProduceResponse|topics=[%s]" ts

  type FetchRequest with
    static member Print (x:FetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps = 
            ps
            |> Seq.map (fun (p,o,_mb) -> sprintf "(p=%i o=%i)" p o)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "FetchRequest|%s" ts

  type FetchResponse with
    static member Print (x:FetchResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec,hwmo,mss,ms) ->
              //let offsetInfo = ms.messages |> Seq.tryItem 0 |> Option.map (fun (o,_,_) -> sprintf " o=%i lag=%i" o (hwmo - o)) |> Option.getOr ""
              let offsetInfo = ms.messages |> Seq.tryItem 0 |> Option.map (fun x -> sprintf " o=%i lag=%i" x.offset (hwmo - x.offset)) |> Option.getOr ""
              sprintf "(p=%i error_code=%i hwo=%i mss=%i%s)" p ec hwmo mss offsetInfo)
            |> String.concat ";"
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "FetchResponse|%s" ts

  type OffsetRequest with
    static member Print (x:OffsetRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,t,_) -> sprintf "p=%i time=%i" p t)
            |> String.concat " ; "
          sprintf "topic=%s partitions=%s" tn ps)
        |> String.concat " ; "
      sprintf "OffsetRequest|topics=%s" ts

  type OffsetResponse with
    static member Print (x:OffsetResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun po -> 
              sprintf "p=%i error_code=%i offsets=[%s]" 
                po.partition 
                po.errorCode 
                (po.offsets |> Seq.map (sprintf "o=%i") |> String.concat " ; "))
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetResponse|topics=%s" ts

  type OffsetFetchRequest with
    static member Print (x:OffsetFetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun p -> sprintf "p=%i" p)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetFetchRequest|group_id=%s topics=%s" x.consumerGroup ts

  type OffsetFetchResponse with
    static member Print (x:OffsetFetchResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,o,_md,ec) -> sprintf "p=%i o=%i error_code=%i" p o ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetFetchResponse|%s" ts

  type OffsetCommitRequest with
    static member Print (x:OffsetCommitRequest) =
      let topics =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,o,_,_) -> sprintf "(p=%i o=%i)" p o)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetCommitRequest|group_id=%s member_id=%s generation_id=%i topics=%s" x.consumerGroup x.consumerId x.consumerGroupGenerationId topics

  type OffsetCommitResponse with
    static member Print (x:OffsetCommitResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec) -> sprintf "(p=%i ec=%i)" p ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "OffsetCommitResponse|%s" ts

  type HeartbeatRequest with
    static member Print (x:HeartbeatRequest) =
      sprintf "HeartbeatRequest|group_id=%s member_id=%s generation_id=%i" x.groupId x.memberId x.generationId

  type HeartbeatResponse with
    static member Print (x:HeartbeatResponse) =
      sprintf "HeartbeatResponse|error_code=%i" x.errorCode

  type GroupCoordinatorResponse with
    static member Print (x:GroupCoordinatorResponse) =
      sprintf "GroupCoordinatorResponse|coordinator_id=%i host=%s port=%i error_code=%i" 
        x.coordinatorId x.coordinatorHost x.coordinatorPort x.errorCode

  type JoinGroup.Request with
    static member Print (x:JoinGroup.Request) =
      sprintf "JoinGroupRequest|group_id=%s member_id=%s" x.groupId x.memberId

  type JoinGroup.Response with
    static member Print (x:JoinGroup.Response) =
      sprintf "JoinGroupResponse|generation_id=%i error_code=%i member_id=%s leader_id=%s" x.generationId x.errorCode x.memberId x.leaderId

  type SyncGroupRequest with
    static member Print (x:SyncGroupRequest) =
      sprintf "SyncGroupRequest|group_id=%s member_id=%s generation_id=%i" x.groupId x.memberId x.generationId

  type SyncGroupResponse with
    static member Print (x:SyncGroupResponse) =
      sprintf "SyncGroupResponse|error_code=%i" x.errorCode

  type ApiVersionsResponse with
    static member Print (x:ApiVersionsResponse) =
      sprintf "ApiVersionsResponse|ec=%i api_versions=[%s]" 
                x.errorCode
                (x.apiVersions |> Seq.map (fun (apiKey,_,v) -> sprintf "[api=%i ver=%i]" (int apiKey) v) |> String.concat " ; ")

  type RequestMessage with
    static member Print (x:RequestMessage) =
      match x with
      | RequestMessage.Fetch x -> FetchRequest.Print x
      | RequestMessage.Produce x -> ProduceRequest.Print x
      | RequestMessage.OffsetCommit x -> OffsetCommitRequest.Print x
      | RequestMessage.OffsetFetch x -> OffsetFetchRequest.Print x
      | RequestMessage.Offset x -> OffsetRequest.Print x
      | RequestMessage.Heartbeat x -> HeartbeatRequest.Print x
      | RequestMessage.JoinGroup x -> JoinGroup.Request.Print x
      | RequestMessage.SyncGroup x -> SyncGroupRequest.Print x
      | _ ->  sprintf "%A" x

  type ResponseMessage with
    static member Print (x:ResponseMessage) =
      match x with
      | ResponseMessage.MetadataResponse x -> MetadataResponse.Print x
      | ResponseMessage.FetchResponse x -> FetchResponse.Print x
      | ResponseMessage.ProduceResponse x -> ProduceResponse.Print x
      | ResponseMessage.OffsetCommitResponse x -> OffsetCommitResponse.Print x
      | ResponseMessage.OffsetFetchResponse x -> OffsetFetchResponse.Print x
      | ResponseMessage.OffsetResponse x -> OffsetResponse.Print x
      | ResponseMessage.HeartbeatResponse x -> HeartbeatResponse.Print x
      | ResponseMessage.GroupCoordinatorResponse x -> GroupCoordinatorResponse.Print x
      | ResponseMessage.JoinGroupResponse x -> JoinGroup.Response.Print x
      | ResponseMessage.SyncGroupResponse x -> SyncGroupResponse.Print x
      | x -> sprintf "%A" x

// ------------------------------------------------------------------------------------------------------------------------------