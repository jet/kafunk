module CompressionGzipTests

open Kafunk
open NUnit.Framework
open System
open System.Text

[<Test>]
[<Category("Compression")>]
let ``Message Compression with Gzip`` () =
    let messageBytes = [| 1uy; 2uy; 3uy; 4uy; 2uy; 6uy; 8uy |]
    let message2Bytes = [| 1uy; 2uy; 3uy; 2uy |]

    let message = Message.create (Binary.ofArray messageBytes) None None
    let message2 = Message.create (Binary.ofArray message2Bytes) None None
    
    let inputMessage =
        Compression.compressMessages [message; message2] Protocol.CompressionCodec.GZIP

    let outputMessageSet =
        Compression.decompressMessage inputMessage Protocol.CompressionCodec.GZIP

    let messages = outputMessageSet.messages
    Assert.IsTrue (messages.Length = 2)
    let (offset, size, msg) = messages.[0]
    let (offset2, size2, msg2) = messages.[1]
    Assert.IsTrue (msg.value |> Binary.toArray = messageBytes)
    Assert.IsTrue (msg2.value |> Binary.toArray = message2Bytes)

[<Test>]
[<Category("Compression")>]
let ``Message Compression with Snappy`` () =
    let messageBytes = [| 1uy; 2uy; 3uy; 4uy; 2uy; 6uy; 8uy |]
    let message2Bytes = [| 1uy; 2uy; 3uy; 2uy |]

    let message = Message.create (Binary.ofArray messageBytes) None None
    let message2 = Message.create (Binary.ofArray message2Bytes) None None
    
    let inputMessage =
        Compression.compressMessages [message; message2] Protocol.CompressionCodec.Snappy

    let outputMessageSet =
        Compression.decompressMessage inputMessage Protocol.CompressionCodec.Snappy

    let messages = outputMessageSet.messages
    Assert.IsTrue (messages.Length = 2)
    let (offset, size, msg) = messages.[0]
    let (offset2, size2, msg2) = messages.[1]
    Assert.IsTrue (msg.value |> Binary.toArray = messageBytes)
    Assert.IsTrue (msg2.value |> Binary.toArray = message2Bytes)

