/// A list of supported versions.
[<Compile(Module)>]
module Kafunk.Versions

open Kafunk

let V_0_8_2 = System.Version (0, 8, 2)
let V_0_9_0 = System.Version (0, 9, 0)
let V_0_10_0 = System.Version (0, 10, 0)
let V_0_10_1 = System.Version (0, 10, 1)

/// Returns an ApiVersion given a system version and an ApiKey.
let internal byVersion (version:System.Version) : ApiKey -> ApiVersion = 
  fun apiKey ->
    match apiKey with
    | ApiKey.OffsetFetch -> 
      if version >= V_0_9_0 then 1s
      elif version >= V_0_8_2 then 0s
      else failwith "not supported"
    | ApiKey.OffsetCommit -> 
      if version >= V_0_9_0 then 2s
      elif version >= V_0_8_2 then 1s
      else 0s
    | ApiKey.Produce -> 
      if version >= V_0_10_0 then 2s
      elif version >= V_0_9_0 then 1s
      else 0s
    | ApiKey.Fetch ->
      if version >= V_0_10_0 then 2s
      elif version >= V_0_9_0 then 1s
      else 0s
    | ApiKey.JoinGroup -> 
      if version >= V_0_10_1 then 1s
      else 0s
    | ApiKey.Offset ->
      if version >= V_0_10_1 then 1s
      else 0s
    | _ -> 
      0s

/// Returns the maximum ApiVersion supported for the specified ApiKey.
let internal byApiVersionResponse (x:ApiVersionsResponse) : ApiKey -> ApiVersion =
  fun (key:ApiKey) -> 
    let (_,_,v) = x.apiVersions.[int key]
    match key with
    | ApiKey.Produce -> min 2s v // TODO: any higher version for produce is currently crashing
    | ApiKey.Fetch -> min 3s v // TODO: any higher version for offset is currently crashing
    | ApiKey.Offset -> min 1s v // TODO: any higher version for offset is currently crashing
    | ApiKey.Metadata -> min 5s v
    | ApiKey.OffsetCommit -> min 2s v
    | ApiKey.OffsetFetch -> min 2s v
    | ApiKey.GroupCoordinator -> min 1s v
    | ApiKey.JoinGroup -> min 2s v 
    | ApiKey.Heartbeat -> min 0s v
    | ApiKey.LeaveGroup -> min 0s v
    | ApiKey.SyncGroup -> min 0s v
    | ApiKey.DescribeGroups -> min 0s v
    | ApiKey.ListGroups -> min 0s v
    | ApiKey.ApiVersions -> min 0s v
    | _ -> failwithf "unsupported key=%O" key