module CodecTests

open KafkaFs
open NUnit.Framework
open System
open System.Text

let arraySegToFSharp (arr:ArraySeg<byte>) =
  let sb = StringBuilder()
  sb.Append("[| ") |> ignore
  for byte in arr do
    sb.AppendFormat("{0}uy;", byte) |> ignore
  sb.Append(" |]") |> ignore
  sb.ToString()

[<Test>]
let ``should encode int32 negative``() =
  let x = -1
  let bytes = BitConverter.GetBytesBigEndian x
  let x2 = BitConverter.ToInt32BigEndian(bytes, 0)
  Assert.AreEqual(x, x2)

[<Test>]
let ``crc32 message``() =
  let m = Message.ofBytes "hello world"B
  let bytes = toArraySeg m
  let crc32 = Crc.crc32 (bytes.Array, bytes.Offset + 4, bytes.Count - 4)
  let expected = 1940715388u
  Assert.AreEqual(expected, crc32)

[<Test>]
let ``crc32 string``() =
  let bytes = "hello world"B
  let crc32 = Crc.crc32 (bytes, 0, bytes.Length)
  let expected = 0x0D4A1185u
  Assert.AreEqual(expected, crc32)

[<Test>]
let ``should encode MessageSet``() =    
  let expected = 
    [
        0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 16uy; 45uy; 70uy; 24uy; 62uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy; 0uy; 1uy; 48uy; 0uy; 0uy; 0uy;
        0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy; 16uy; 90uy; 65uy; 40uy; 168uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy; 0uy; 0uy; 0uy; 0uy;
        0uy; 0uy; 0uy; 0uy; 0uy; 16uy; 195uy; 72uy; 121uy; 18uy; 0uy; 0uy; 0uy; 0uy; 0uy; 1uy; 49uy; 0uy; 0uy; 0uy; 1uy; 50uy
    ]
  let ms = 
    [
      Message.ofString("0", "1")
      Message.ofString("1", "1")
      Message.ofString("2", "1")
    ]
    |> MessageSet.ofMessages
  let data = toArraySeg ms
  let encoded = data |> ArraySeg.toArray |> Array.toList    
  Assert.True ((expected = encoded))

[<Test>]
let ``should decode FetchResponse``() =
  let data = ArraySeg.ofArray [| 0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;8uy;0uy;0uy;0uy;37uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;7uy;0uy;0uy;0uy;25uy;115uy;172uy;247uy;124uy;0uy;0uy;255uy;255uy;255uy;255uy;0uy;0uy;0uy;11uy;104uy;101uy;108uy;108uy;111uy;32uy;119uy;111uy;114uy;108uy;100uy; |]
  let (res:FetchResponse,_) = read data
  let topicName,ps = res.topics.[0]
  let (p,ec,hwo,mss,ms) = ps.[0]
  let (o,ms,m) = ms.messages.[0]
  Assert.AreEqual("test", topicName)
  Assert.AreEqual(p, 0)
  Assert.AreEqual(ec, 0s)
  Assert.AreEqual(mss, 37)
  Assert.AreEqual(o, 7L)
  Assert.AreEqual(1940715388, int m.crc)
  Assert.AreEqual("hello world", (m.value |> Encoding.UTF8.GetString))

[<Test>]
let ``should decode ProduceResponse``() =
  let data = ArraySeg.ofArray [| 0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;8uy; |]
  let (res:ProduceResponse,_) = read data
  let topicName,ps = res.topics.[0]
  let p,ec,off = ps.[0]
  Assert.AreEqual("test", topicName)
  Assert.AreEqual(0, p)
  Assert.AreEqual(0s, ec)
  Assert.AreEqual(8L, off)

[<Test>]
let ``should encode ProduceRequest``() =
  let req = ProduceRequest.ofMessageSet ("test", 0, MessageSet.ofMessage (Message.ofBytes ("hello world"B)))
  let data = toArraySeg req |> ArraySeg.toArray |> Array.toList
  let expected = [0uy;1uy;0uy;0uy;3uy;232uy;0uy;0uy;0uy;1uy;0uy;4uy;116uy;101uy;115uy;116uy;0uy;0uy;0uy;1uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;37uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;0uy;25uy;115uy;172uy;247uy;124uy;0uy;0uy;255uy;255uy;255uy;255uy;0uy;0uy;0uy;11uy;104uy;101uy;108uy;108uy;111uy;32uy;119uy;111uy;114uy;108uy;100uy;]
  Assert.True((expected = data))
    
