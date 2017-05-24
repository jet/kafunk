[<Compile(Module)>]
module internal Kafunk.Compression

open System.IO
open System.IO.Compression

open Kafunk

let private createMessage (value:Value) (compression:byte) =
  let attrs = compression |> int8
  Message.create value Binary.empty (Some attrs)

[<RequireQualifiedAccess>]
module internal Stream = 
  // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
  let compress (codec: CompressionCodec) (makeStream: MemoryStream -> Stream) (messageVer:ApiVersion) (ms:MessageSet) =
    // TODO: pool MemoryStreams
    use outputStream = new MemoryStream()
    do
      let inputBytes = MessageSet.Size (messageVer,ms) |> Array.zeroCreate
      let buf = Binary.ofArray inputBytes
      MessageSet.Write (messageVer,ms,BinaryZipper(buf))
      use compStream = makeStream outputStream
      try
        compStream.Write(inputBytes, 0, inputBytes.Length)
      with :? IOException as _ex ->
        // TODO: log this
        //printfn "Couldn't write to gzip stream: %A" ex
        reraise()
    createMessage (outputStream.ToArray() |> Binary.ofArray) codec

  let decompress (makeStream: MemoryStream -> Stream) (messageVer:ApiVersion) (m:Message) =
    let inputBytes = m.value |> Binary.toArray
    // TODO: pool MemoryStreams
    use outputStream = new MemoryStream()
    do
      use inputStream = new MemoryStream(inputBytes)
      use compStream = makeStream inputStream
      try
        compStream.CopyTo(outputStream)
      with :? IOException as _ex ->
        // TODO: log this
        //printfn "Couldn't read from gzip stream: %A" ex
        reraise()
    outputStream.Position <- 0L
    let output = outputStream.ToArray()
    // size is output array size divided by message set element size
    let bz = BinaryZipper(output |> Binary.ofArray)
    MessageSet.Read (messageVer, 0, 0s, output.Length, bz)

[<Compile(Module)>]
module GZip =

  open System.IO
  open System.IO.Compression

  let compress =
    Stream.compress CompressionCodec.GZIP <| fun memStream -> 
      upcast new GZipStream(memStream, CompressionMode.Compress)

  let decompress =
    Stream.decompress <| fun memStream -> 
      upcast new GZipStream(memStream, CompressionMode.Decompress)
    
[<Compile(Module)>]
module Snappy = 
  
  open System
  open System.IO
  open System.Text
  open System.IO.Compression
  open Snappy

  module CompressedMessage = 
    
    type BinaryZipper with
      
      member this.WriteBlock (bytes: ArraySegment<byte>) = 
        System.Buffer.BlockCopy(bytes.Array, bytes.Offset, this.Buffer.Array, this.Buffer.Offset, bytes.Count)
        this.ShiftOffset bytes.Count

      member this.ReadBlock (length: int) =
        let arr = ArraySegment<byte>(this.Buffer.Array, this.Buffer.Offset, length)
        this.ShiftOffset length
        arr

    module private Header =     
      // Combined size of header in bytes.  
      let size = 16
      // Magic string used by snappy-java.
      let magic = [| byte -126; byte 'S'; byte 'N'; byte 'A'; byte 'P'; byte 'P'; byte 'Y'; byte 0 |]
      // Current version number taken from snappy-java repo as of 22/05/2017.
      let currentVer = 1
      // Minimum compatible version number taken from snappy-java repo as of 22/05/2017.
      let minimumVer = 1

    let pack (bytes: byte []) =
      let buf = Array.zeroCreate (Header.size + 4 + bytes.Length)
      let bz = BinaryZipper(Binary.ofArray buf)
      
      // write header compatible with snappy-java
      bz.WriteBlock(Binary.ofArray Header.magic)
      bz.WriteInt32(Header.currentVer)
      bz.WriteInt32(Header.minimumVer)

      // write content
      // NOTE: this will also write content length in the first 4 bytes, this is expected.
      bz.WriteBytes(Binary.ofArray bytes)

      buf

    let unpack (bytes: byte []) =
      let bz = BinaryZipper(Binary.ofArray bytes)
      
      // TODO: do we want to validate these?
      let magic      = bz.ReadBlock(Header.magic.Length)
      let currentVer = bz.ReadInt32()
      let minimumVer = bz.ReadInt32()

      let content = bz.ReadBytes()

      let buf = Array.zeroCreate<byte> content.Count
      Buffer.BlockCopy(content.Array, content.Offset, buf, 0, content.Count)
      buf

  let compress (messageVer:ApiVersion) (ms:MessageSet) =
    let inputBytes = MessageSet.Size (messageVer,ms) |> Array.zeroCreate
    let buf = Binary.ofArray inputBytes
    MessageSet.Write (messageVer,ms,BinaryZipper(buf))
    try
      let compressed = SnappyCodec.Compress(inputBytes)
      let output = CompressedMessage.pack compressed
      createMessage (Binary.ofArray output) CompressionCodec.Snappy
    with :? IOException as _ex ->
      // TODO: log this
      reraise()

  let decompress (messageVer:ApiVersion) (m:Message) =
    let inputBytes = m.value |> Binary.toArray
    try        
      let compressed = CompressedMessage.unpack inputBytes      
      let output = SnappyCodec.Uncompress(compressed)
      let bz = BinaryZipper(Binary.ofArray output)
      MessageSet.Read (messageVer, 0, 0s, output.Length, bz)
    with :? IOException as _ex ->
      // TODO: log this
      reraise()
  
let compress (messageVer:int16) (compression:byte) (ms:MessageSet) =
  match compression with
  | CompressionCodec.None -> ms
  | CompressionCodec.GZIP -> MessageSet.ofMessage messageVer (GZip.compress messageVer ms)
  | CompressionCodec.Snappy -> MessageSet.ofMessage messageVer (Snappy.compress messageVer ms)
  | _ -> failwithf "Incorrect compression codec %A" compression
  
let decompress (messageVer:int16) (ms:MessageSet) =
  if ms.messages.Length = 0 then ms
  else
    ms.messages
    |> Array.Parallel.collect (fun msi -> 
      match (msi.message.attributes &&& (sbyte CompressionCodec.Mask)) |> byte with
      | CompressionCodec.None -> [|msi|]
      | CompressionCodec.GZIP ->
        let decompressed = GZip.decompress messageVer msi.message
        decompressed.messages
      | CompressionCodec.Snappy ->
        let decompressed = Snappy.decompress messageVer msi.message
        decompressed.messages
      | c -> failwithf "compression_code=%i not supported" c)
    |> MessageSet
    