#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open Refs

let Log = Log.create __SOURCE_FILE__

let host = "localhost"

let go = async {

  let connCfg = KafkaConfig.create ([KafkaUri.parse host])
  let conn = Kafka.conn connCfg

  let! res = Kafka.listGroups conn (ListGroupsRequest())
  let groupIds = res.groups |> Seq.map (fun (g,_) -> g) |> Seq.toArray

  let! res = Kafka.describeGroups conn (DescribeGroupsRequest(groupIds))
  
  for (ec,gid,s,pt,p,gm) in res.groups do
    for (mid,cid,ch,md,ma) in gm.members do
      let t,ps = ConsumerGroup.decodeMemberAssignment ma
      Log.info "assignments|group_id=%s mid=%s topic=%s ps=%A" gid mid t ps
      ()
    ()

  return ()

}

Async.RunSynchronously go

Log.info "DONE"



