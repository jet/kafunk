#r "../../../packages/FAKE/tools/FakeLib.dll"

open Fake
open System

/// Instances are a measure
[<Measure>]
type instance

/// The number of instances we expect to run of a given service
type Scale = int<instance>

/// The type of services we support running
type Service =
    | Docker of Scale

let env key =
    let value = Environment.GetEnvironmentVariable(key)
    if isNull value then None else Some value

let konst v _ = v

let supportPath = __SOURCE_DIRECTORY__

let dockerCompose command =
    let formatArgs args (key, format, defaultVal) =
        match env key with
        | Some value -> args + " " + format value
        | None -> args + " " + defaultVal
    let dockerArgs =
      [
        ("DOCKER_HOST", sprintf "-H %s", "")
        ("PROJECT_NAME", sprintf "-p %s", "-p kafunk")
        ("VERBOSE", konst "--verbose", "")
      ]
        |> List.fold formatArgs ""
    let args = dockerArgs + " " + command
    let exitCode = Shell.Exec("docker-compose", args, supportPath + "\docker")
    if exitCode <> 0 then failwithf "Unable to run docker-compose with '%s'" args

let serviceScale =
    Map.ofList [
        ("zookeeper", Docker 1<instance>)
        ("kafka", Docker 3<instance>)
        ("zk-web", Docker 1<instance>) ]

let startService service =
    match serviceScale |> Map.find service with
    | Docker scale ->
        dockerCompose <| sprintf "scale %s=%i" service scale

let stopService service =
    match serviceScale |> Map.find service with
    | Docker _ ->
        dockerCompose <| sprintf "scale %s=0" service

Target "Start:ZooKeeper" <| fun () ->
    startService "zookeeper"

Target "Stop:ZooKeeper" <| fun () ->
    stopService "zookeeper"

Target "Start:Kafka" <| fun () ->
    startService "kafka"

Target "Stop:Kafka" <| fun () ->
    stopService "kafka"

Target "Start:ZkWeb" <| fun () ->
    startService "zk-web"

Target "Stop:ZkWeb" <| fun () ->
    stopService "zk-web"

Target "Start:All" <| ignore

Target "Stop:All" <| fun () ->
    dockerCompose "down"

/// Some of our targets require these asynchronous starts to be
/// farther along, so we'll artificially delay. HACK HACK ;-)
Target "Delay" <| fun () ->
    System.Threading.Thread.Sleep(1500)

"Start:All" <== ["Start:ZooKeeper"; "Start:ZkWeb"; "Start:Kafka"]
"Start:Kafka" <== ["Start:ZooKeeper"; "Delay"]
"Start:ZkWeb" <== ["Start:ZooKeeper"]

"Stop:ZooKeeper" <== ["Stop:Kafka"]

RunTargetOrDefault "Start:All"