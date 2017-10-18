#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let Log = Log.create __SOURCE_FILE__

let host = argiDefault 1 "localhost"


let go = async {

  let connCfg = KafkaConfig.create ([KafkaUri.parse host])
  let conn = Kafka.conn connCfg

  let! groups = ConsumerInfo.consumerGroups conn

  for g in groups do
    if not <| g.group_id.StartsWith("_") then
      let topics = 
        g.members 
        |> Seq.collect (fun m -> m.assignments |> Seq.map (fun (t,_) -> t))
        |> Seq.distinct
        |> Seq.toArray
      Log.info "group_id=%s topics=%A" g.group_id topics

//  let topic = "nova-skus"
//  let! groups = consumerGroupByTopics conn [topic]
//
//  for (topic,groups) in groups |> Map.toSeq do
//    for g in groups do
//      Log.info "topic=%s group_id=%s" topic g.group_id

//  let! res = Kafka.listGroups conn (ListGroupsRequest())
//  let groupIds = res.groups |> Seq.map (fun (g,_) -> g) |> Seq.toArray
//  Log.info "group_ids|%A" groupIds
//
//  let! res = Kafka.describeGroups conn (DescribeGroupsRequest(groupIds))
//  
//  for (ec,gid,s,pt,p,gm) in res.groups do
//    for (mid,cid,ch,md,ma) in gm.members do
//      let t,ps = ConsumerGroup.decodeMemberAssignment ma
//      Log.info "assignments|group_id=%s mid=%s topic=%s ps=%A" gid mid t ps
//      ()
//    ()
//
//  return ()

}

Async.RunSynchronously go

Log.info "DONE"



