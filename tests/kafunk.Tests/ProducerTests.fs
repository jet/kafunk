module ProducerTests

open NUnit.Framework
open Kafunk
open Kafunk.Protocol

let host = "localhost"

let port = 9092

let Log = Log.create __SOURCE_FILE__

//[<Test>]
//let ``simple connection test`` () = 
//    let conn = Kafka.connHostAndPort host port
//    let metadata = Kafka.metadata conn (Metadata.Request([||])) |> Async.RunSynchronously
//    metadata.brokers |> Assert.IsNotEmpty

[<Test>]
let ``error when writing to unknown topic test`` () = 
    let topicName = "TopicA"
    let conn = Kafka.connHostAndPort host port
    let req = ProduceRequest.ofMessageSet topicName 0 (MessageSet.ofMessage (Message.ofBytes "hello world"B (Some "key"B))) None None
    let res = Kafka.produce conn req |> Async.RunSynchronously
    if res.topics |> Array.isEmpty then
        Assert.Fail("No response returned")
    else
        let topic,data = 
            res.topics.[0]
        if topic = topicName then
            let x = data |> Array.tryFind(fun (partition, errorCode, offset) -> 
                printfn "p:%A" partition
                printfn "ec:%A" errorCode
                printfn "o:%A" offset
                errorCode = 0s)
            Assert.Pass()
            //|> Array.tryFind(fun (topic,(partition,errorCode,offset)) -> topic = "~~")
        else
            printfn ""
            Assert.Fail("")
