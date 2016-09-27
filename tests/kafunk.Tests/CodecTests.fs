module CodecTests

open NUnit.Framework

open System.Text

open Kafunk
open Kafunk.Protocol

let arraySegToFSharp (arr:Binary.Segment) =
  let sb = StringBuilder()
  sb.Append("[| ") |> ignore
  for byte in arr do
    sb.AppendFormat("{0}uy;", byte) |> ignore
  sb.Append(" |]") |> ignore
  sb.ToString()

[<Test>]
let ``should encode int32 negative``() =
  let x = -1
  let buf = Binary.zeros 4
  Binary.writeInt32 x buf |> ignore
  let x2 = Binary.readInt32 buf |> fst
  Assert.AreEqual(x, x2)

[<Test>]
let ``crc32 message``() =
  let m = Message.ofBytes "hello world"B None
  let bytes = toArraySeg Message.size Message.write m
  let crc32 = Crc.crc32 bytes.Array (bytes.Offset + 4) (bytes.Count - 4)
  let expected = 1940715388u
  Assert.AreEqual(expected, crc32)

[<Test>]
let ``crc32 string``() =
  let bytes = "hello world"B
  let crc32 = Crc.crc32 bytes 0 bytes.Length
  let expected = 0x0D4A1185u
  Assert.AreEqual(expected, crc32)

[<Test>]
let ``should encode MessageSet``() =
  let expected =
    [
        0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 16uy; 45uy;
        70uy; 24uy; 62uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy; 0uy;
        1uy; 48uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 16uy;
        90uy; 65uy; 40uy; 168uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy;
        0uy; 1uy; 49uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy;
        16uy; 195uy; 72uy; 121uy; 18uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy;
        0uy; 0uy; 1uy; 50uy
    ]
  let ms =
    [
      Message.ofString "0" "1"
      Message.ofString "1" "1"
      Message.ofString "2" "1"
    ]
    |> MessageSet.ofMessages
  let data = toArraySeg MessageSet.size MessageSet.write ms
  let encoded = data |> Binary.toArray |> Array.toList
  Assert.True ((expected = encoded))

[<Test>]
let ``should decode FetchResponse``() =
  let data =
    Binary.ofArray [|
      0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;
      0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;8uy;0uy;0uy;0uy;37uy;0uy;0uy;
      0uy;0uy;0uy;0uy;0uy;7uy;0uy;0uy;0uy;25uy;115uy;172uy;247uy;124uy;0uy;0uy;
      255uy;255uy;255uy;255uy;0uy;0uy;0uy;11uy;104uy;101uy;108uy;108uy;111uy;
      32uy;119uy;111uy;114uy;108uy;100uy; |]
  let (res:FetchResponse), _ = FetchResponse.read data
  let topicName, ps = res.topics.[0]
  let p, ec, _hwo, mss, ms = ps.[0]
  let o, _ms, m = ms.messages.[0]
  Assert.AreEqual("test", topicName)
  Assert.AreEqual(p, 0)
  Assert.AreEqual(ec, 0s)
  Assert.AreEqual(mss, 37)
  Assert.AreEqual(o, 7L)
  Assert.AreEqual(1940715388, int m.crc)
  Assert.AreEqual("hello world", (m.value |> Binary.toString))

[<Test>]
let ``should decode partial FetchResponse`` () =
  let data =
    Binary.ofArray [|
      0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;
      0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;8uy;0uy;0uy;0uy;37uy;0uy;0uy;
      0uy;0uy;0uy;0uy;0uy;7uy;0uy;0uy;0uy;25uy;115uy;172uy;247uy;124uy;0uy;0uy;
      255uy;255uy;255uy;255uy;0uy;0uy;0uy;11uy;104uy;101uy;108uy;108uy;111uy;
      32uy;119uy;111uy;114uy;108uy |]
  let (res:FetchResponse), _ = FetchResponse.read data
  let tn,ps = res.topics.[0]
  let _p, _ec, _hwo, _mss, ms = ps.[0]  
  Assert.AreEqual(1, res.topics.Length)
  Assert.AreEqual("test", tn)
  Assert.AreEqual(0, ms.messages.Length)
  
  
  


[<Test>]
let ``should decode ProduceResponse``() =
  let data =
    Binary.ofArray [|
      0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;
      0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;8uy; |]
  let (res:ProduceResponse), _ = ProduceResponse.read data
  let topicName, ps = res.topics.[0]
  let p, ec, off = ps.[0]
  Assert.AreEqual("test", topicName)
  Assert.AreEqual(0, p)
  Assert.AreEqual(0s, ec)
  Assert.AreEqual(8L, off)

[<Test>]
let ``should encode ProduceRequest``() =
  let req = ProduceRequest.ofMessageSet "test" 0 (MessageSet.ofMessage (Message.ofBytes "hello world"B None)) None None
  let data = toArraySeg ProduceRequest.size ProduceRequest.write req |> Binary.toArray |> Array.toList
  let expected = [
    0uy;1uy;0uy;0uy;3uy;232uy;0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;
    0uy;0uy;0uy;1uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;37uy;0uy;0uy;0uy;0uy;0uy;0uy;
    0uy;0uy;0uy;0uy;0uy;25uy;115uy;172uy;247uy;124uy;0uy;0uy;255uy;255uy;255uy;
    255uy;0uy;0uy;0uy;11uy;104uy;101uy;108uy;108uy;111uy;32uy;119uy;111uy;
    114uy;108uy;100uy; ]
  Assert.True((expected = data))