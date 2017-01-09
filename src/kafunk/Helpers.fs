[<AutoOpen>]
module internal Kafunk.Helpers

open Kafunk
open System
open System.Text

module Message =

  let create value key attrs =
    Message(0, 0y, (defaultArg attrs 0y), 0L, key, value)

  let ofBuffer data key =
    Message(0, 0y, 0y, 0L, (defaultArg  key (Binary.empty)), data)

  let ofBytes value key =
    let key =
      match key with
      | Some key -> Binary.ofArray key
      | None -> Binary.empty
    Message(0, 0y, 0y, 0L, key, Binary.ofArray value)

  let ofString (value:string) (key:string) =
    let value = Encoding.UTF8.GetBytes value |> Binary.ofArray
    let key = Encoding.UTF8.GetBytes key |> Binary.ofArray
    Message(0, 0y, 0y, 0L, key, value)

module MessageSet =

  let ofMessage (m:Message) =
    MessageSet([| 0L, Message.size m, m |])

  let ofMessages ms =
    MessageSet(ms |> Seq.map (fun m -> 0L, Message.size m, m) |> Seq.toArray)

  /// Returns the frist offset in the message set.
  let firstOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      let (o,_,_) = ms.messages.[0] in o
    else
      0L

  /// Returns the last offset in the message set.
  let lastOffset (ms:MessageSet) =
    if ms.messages.Length > 0 then
      let (o,_,_) = ms.messages.[ms.messages.Length - 1] in o
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
      
module ProduceRequest =

  let ofMessageSetTopics ms requiredAcks timeout =
    ProduceRequest(requiredAcks, timeout,
      ms |> Array.map (fun (t, ms) -> (t, ms |> Array.map (fun (p, ms) -> (p, MessageSet.size ms, ms)))))


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
    
  let partitionOffsetPairs (os:seq<Partition * Offset>) =
    concatMapSb os (fun sb (p,o) -> sb.AppendFormat("[partition={0} offset={1}]", p, o)) " ; "
    
  type MetadataResponse with
    static member Print (x:MetadataResponse) =
      let topics =
        x.topicMetadata
        |> Seq.map (fun tmd -> 
          let partitions = 
            tmd.partitionMetadata
            |> Seq.map (fun pmd -> sprintf "[partition_id=%i leader=%i]" pmd.partitionId pmd.leader)
            |> String.concat " ; "
          sprintf "[topic=%s partitions=%s]" tmd.topicName partitions)
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
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,mss,ms) -> sprintf "partition=%i message_set_size=%i message_count=%i" p mss ms.messages.Length)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "ProduceRequest|required_acks=%i timeout=%i topics=[%s]" x.requiredAcks x.timeout ts

  type ProduceResponse with
    static member Print (x:ProduceResponse) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps =
            ps
            |> Seq.map (fun (p,ec,o) -> sprintf "partition=%i offset=%i error_code=%i" p o ec)
            |> String.concat " ; "
          sprintf "topic=%s partitions=[%s]" tn ps)
        |> String.concat " ; "
      sprintf "ProduceResponse|topics=[%s]" ts

  type FetchRequest with
    static member Print (x:FetchRequest) =
      let ts =
        x.topics
        |> Seq.map (fun (tn,ps) ->
          let ps = 
            ps
            |> Seq.map (fun (p,o,_mb) -> sprintf "(partition=%i offset=%i)" p o)
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
              let offsetInfo = ms.messages |> Seq.tryItem 0 |> Option.map (fun (o,_,_) -> sprintf " offset=%i lag=%i" o (hwmo - o)) |> Option.getOr ""
              sprintf "(partition=%i error_code=%i high_watermark_offset=%i message_set_size=%i%s)" p ec hwmo mss offsetInfo)
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
            |> Seq.map (fun (p,t,_) -> sprintf "partition=%i time=%i" p t)
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
              sprintf "partition=%i error_code=%i offsets=[%s]" 
                po.partition 
                po.errorCode 
                (po.offsets |> Seq.map (sprintf "offset=%i") |> String.concat " ; "))
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
            |> Seq.map (fun p -> sprintf "partition=%i" p)
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
            |> Seq.map (fun (p,o,_md,ec) -> sprintf "partition=%i offset=%i error_code=%i" p o ec)
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
            |> Seq.map (fun (p,o,_) -> sprintf "(partition=%i offset=%i)" p o)
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
            |> Seq.map (fun (p,ec) -> sprintf "(partition=%i error_code=%i)" p ec)
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

  type RequestMessage with
    static member Print (x:RequestMessage) =
      match x with
      | RequestMessage.Fetch x -> FetchRequest.Print x
      | RequestMessage.Produce x -> ProduceRequest.Print x
      | RequestMessage.OffsetCommit x -> OffsetCommitRequest.Print x
      | RequestMessage.OffsetFetch x -> OffsetFetchRequest.Print x
      | RequestMessage.Offset x -> OffsetRequest.Print x
      | RequestMessage.Heartbeat x -> HeartbeatRequest.Print x
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
      | x -> sprintf "%A" x

// ------------------------------------------------------------------------------------------------------------------------------