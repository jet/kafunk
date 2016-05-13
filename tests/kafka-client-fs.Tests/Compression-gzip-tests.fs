module CompressionGzipTests

open Kafunk
open NUnit.Framework
open System
open System.Text
//
//[<Test>]
//[<Category("Compression")>]
//let ``Message Compression with Gzip`` () =
//    let messageBytes = [| 1uy; 2uy; 3uy; 4uy|]
//    let message = Message.create(value=ArraySeg.ofArray messageBytes, compression=Protocol.CompressionCodecs.GZIPCompressionCodec)
//    let message2 =
//        Compression.gzip [message]
//
//    let messageSet =
//        Compression.decompress message2 1
//
//    let messages = messageSet.messages
//    Assert.IsTrue (messages.Length = 1)
//    let (offset, size, msg) = messages.[0]
//    Assert.IsTrue (msg.value |> ArraySeg.toArray = messageBytes)

