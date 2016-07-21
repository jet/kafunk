module ProducerTests

open NUnit.Framework
open Kafunk
open Kafunk.Protocol

let host = "localhost"

let port = 9092

let Log = Log.create __SOURCE_FILE__

[<Test>]
let ``simple connection test`` () = 
    let conn = Kafka.connHostAndPort host port
    let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously

    metadata.brokers
    |> Array.iter(fun b -> printfn "%A" b.nodeId)

    metadata.brokers |> Assert.IsNotEmpty
