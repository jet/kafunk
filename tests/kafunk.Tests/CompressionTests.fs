module CompressionTests

open Kafunk
open NUnit.Framework
open System
open System.Text

module Message =

  let create value key attrs =
    Message(0, 0y, (defaultArg attrs 0y), 0L, key, value)

let ofMessages ms =
  MessageSet(ms |> Seq.map (fun m -> MessageSetItem (0L, Message.Size m, m)) |> Seq.toArray)


//[<Test>]
//[<Category("Compression")>]
//let ``Compression.GZip should work`` () =

//    let messageBytes = [| 1uy; 2uy; 3uy; 4uy; 2uy; 6uy; 8uy |]
//    let message2Bytes = [| 1uy; 2uy; 3uy; 2uy |]

//    let message = Message.create (Binary.ofArray messageBytes) (Binary.empty) None
//    let message2 = Message.create (Binary.ofArray message2Bytes) (Binary.empty) None
    
//    let inputMessage =
//        Compression.GZip.compress (ofMessages [message; message2])

//    let outputMessageSet =
//        Compression.GZip.decompress 0y inputMessage

//    let messages = outputMessageSet.messages
//    Assert.IsTrue (messages.Length = 2)
//    let (offset, size, msg) = let x = messages.[0] in x.offset, x.messageSize, x.message
//    let (offset2, size2, msg2) = let x = messages.[1] in x.offset, x.messageSize, x.message
//    Assert.IsTrue (msg.value |> Binary.toArray = messageBytes)
//    Assert.IsTrue (msg2.value |> Binary.toArray = message2Bytes)


//[<Test>]
//[<Category("Compression")>]
//let ``Compression.Snappy should work`` () =

//    let messageBytes = [| 1uy; 2uy; 3uy; 4uy; 2uy; 6uy; 8uy |]
//    let message2Bytes = [| 1uy; 2uy; 3uy; 2uy |]

//    let message = Message.create (Binary.ofArray messageBytes) (Binary.empty) None
//    let message2 = Message.create (Binary.ofArray message2Bytes) (Binary.empty) None
    
//    let inputMessage =
//        Compression.Snappy.compress 0s (MessageSet.ofMessages 0s [message; message2])

//    let outputMessageSet =
//        Compression.Snappy.decompress 0s inputMessage

//    let messages = outputMessageSet.messages
//    Assert.IsTrue (messages.Length = 2)
//    let (offset, size, msg) = let x = messages.[0] in x.offset, x.messageSize, x.message
//    let (offset2, size2, msg2) = let x = messages.[1] in x.offset, x.messageSize, x.message
//    Assert.IsTrue (msg.value |> Binary.toArray = messageBytes)
//    Assert.IsTrue (msg2.value |> Binary.toArray = message2Bytes)

//module Samples = 
//  /// Payload of the message sent using kafka-console-producer.bat with snappy compression on. 
//  let consoleProducerMsg = 
//    [|
//      // header
//      130uy; 83uy; 78uy; 65uy; 80uy; 80uy; 89uy; 0uy; 
//      0uy; 0uy; 0uy; 1uy; 0uy; 0uy; 0uy; 1uy; 
//      // length
//      0uy; 0uy; 0uy; 25uy; 
//      // content
//      30uy; 0uy; 0uy; 25uy; 1uy; 72uy; 18uy; 89uy; 42uy; 
//      71uy; 135uy; 0uy; 0uy; 255uy; 255uy; 255uy; 255uy; 
//      0uy; 0uy; 0uy; 4uy; 
//      // "test" 
//      116uy; 101uy; 115uy; 116uy 
//    |]

//[<Test>]
//[<Category("Compression")>]
//let ``Compression.Snappy writes messages that are compatible with reference implementation``() = 
//  let payload = "test" 

//  let msg = Message.create (Binary.ofArray (Encoding.ASCII.GetBytes(payload))) (Binary.empty) None
//  let msgSet = MessageSet.ofMessage 0s msg

//  let outputMsg = Compression.Snappy.compress 0s msgSet

//  Assert.AreEqual (Samples.consoleProducerMsg, Binary.toArray outputMsg.value)

//[<Test>]
//[<Category("Compression")>] 
//let ``Compression.Snappy reads messages that are compatible with reference implementation``() = 
//  let msg = Message.create (Binary.ofArray Samples.consoleProducerMsg) (Binary.empty) None

//  let outputMsgSet = 
//    Compression.Snappy.decompress 0s msg

//  Assert.AreEqual (1, outputMsgSet.messages.Length) 
  
//  let payload =
//    let outputMsg = outputMsgSet.messages.[0] 
//    Encoding.ASCII.GetString(outputMsg.message.value |> Binary.toArray)

//  Assert.AreEqual ("test", payload)
