module CompressionGzipTests

open KafkaFs
open KafkaFs.Compression
open NUnit.Framework
open System
open System.Text


type CompressionCodecs = NoCompressionCodec = 0 | DefaultCompressoinCodec = 1 | GZIPCompressionCodec = 2 | SnappyCompressionCodec = 3


[<Test>]
[<Category("Compression")>]
let ``Message Compression with Gzip`` () =
    let messageBytes = [| 1uy; 2uy; 3uy; 4uy|]
    let message = Message.create <| ArraySeg.ofArray messageBytes




    let attrs = 3 ||| int CompressionCodecs.GZIPCompressionCodec
    let message2 = Message.create (value = ArraySeg.ofArray messageBytes, attrs = CompressionCodecs.GZIPCompressionCodec )

//    let message = Message.ofBytes messageBytes
//    message.attributes 
//    message.compress
    Assert.IsTrue(true)







let compressionCodecValue x = 
    match x with 
    | CompressionCodecs.SnappyCompressionCodec -> 2uy
    | CompressionCodecs.GZIPCompressionCodec -> 1uy
    | CompressionCodecs.NoCompressionCodec -> 0uy
    | __ -> 1uy
    