module CompressionGzipTests

open KafkaFs
open KafkaFs.Protocol
open NUnit.Framework
open System
open System.Text

[<Test>]
[<Category("Compression")>]
let ``Message Compression with Gzip`` () =
    let messageBytes = [| 1uy; 2uy; 3uy; 4uy|]
    let message = Message.create(value=Buffer.ofArray messageBytes, compression=Protocol.Compression.GZIP)
    let message2 =
        Compression.gzip [message]

    let messageSet =
        Compression.decompress message2 1

    let messages = messageSet.messages
    Assert.IsTrue (messages.Length = 1)
    let (offset, size, msg) = messages.[0]
    Assert.IsTrue (msg.value |> Buffer.toArray = messageBytes)

