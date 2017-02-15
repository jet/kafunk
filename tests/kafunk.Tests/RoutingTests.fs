module RoutingTests

open Kafunk
open System
open System.Net
open NUnit.Framework

[<Test>]
let ``Routing.route should group offset requests by broker`` () =
  
  ()

//  let routes = 
//    Routes.ofBootstrap (EndPoint.ofIPAddressAndPort (IPAddress.Loopback, 9092))
//    |> Routes.addBrokersAndTopicNodes 
//        [ (1, EndPoint.parse ("127.0.0.1",9092)) ] 
//        [ ("A",0,1) ; ("A",1,1) ]
//
//  let offsetReq = OffsetRequest(-1, [| ("A", [| 0, Time.EarliestOffset, 1 |]) ; ("A", [| 1, Time.EarliestOffset, 1 |]) |])
//  
//  let actual = 
//    Routing.route routes (RequestMessage.Offset offsetReq)
//    |> Result.map (fun r -> r.Length)
//
//  let expected = Success 1
//
//  shouldEqual expected actual None