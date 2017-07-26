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
    | ApiKey.Fetch -> min 2s v
    | ApiKey.Produce -> min 2s v
    | ApiKey.Metadata -> min 0s v
    | ApiKey.Offset -> min 1s v
    | _ -> v