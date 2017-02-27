#load "Refs.fsx"
#time "on"

open FSharp.Control
open Kafunk
open System

let Log = Log.create __SOURCE_FILE__

let argiDefault i def = fsi.CommandLineArgs |> Seq.tryItem i |> Option.getOr def

let host = argiDefault 1 "localhost"
let sleep = argiDefault 2 "10000" |> Int32.Parse

let topicGroups : (TopicName * GroupId) list = 
  [ for i = 3 to fsi.CommandLineArgs.Length - 1 do
      let arg = fsi.CommandLineArgs.[i]
      if arg.Contains("|") then
        let pts = arg.Split('|')
        let topic = pts.[0]
        let group = pts.[1]
        yield topic,group ]

let go (topicGroups:(TopicName * GroupId) seq) = async {
  let! conn = Kafka.connHostAsync host
  let showProgress (t,g) = async {
    while true do
      let! info = ConsumerInfo.progress conn g t [||]
      Log.info "topic=%s group_id=%s" t g
      do! Async.Sleep sleep }
  return!
    topicGroups
    |> Seq.map showProgress
    |> Async.Parallel
    |> Async.Ignore }

Async.RunSynchronously (go topicGroups)

