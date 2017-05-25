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
      compStream.Write(inputBytes, 0, inputBytes.Length)
    createMessage (outputStream.ToArray() |> Binary.ofArray) codec

  let decompress (makeStream: MemoryStream -> Stream) (messageVer:ApiVersion) (m:Message) =
    let inputBytes = m.value |> Binary.toArray
    // TODO: pool MemoryStreams
    use outputStream = new MemoryStream()
    do
      use inputStream = new MemoryStream(inputBytes)
      use compStream = makeStream inputStream
      compStream.CopyTo(outputStream)
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
  open Snappy
    
  module internal Binary = 

    let writeBlock (bytes: Binary.Segment) (buf : Binary.Segment) = 
      Buffer.BlockCopy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      Binary.shiftOffset bytes.Count buf

    let readBlock (length: int) (buf : Binary.Segment) = 
      let arr = ArraySegment<byte>(buf.Array, buf.Offset, length)
      arr, Binary.shiftOffset length buf

    let truncateIfSmaller actualLength maxLength (array: byte []) = 
      if actualLength < maxLength 
        then Binary.Segment(array, 0, actualLength)
        else Binary.ofArray array
        
  type internal SnappyBinaryZipper (buf: Binary.Segment) = 
      
    let mutable buffer = buf

    member this.Read<'a> (reader: Binary.Reader<'a>) = 
      let res, updatedBuffer = reader buffer
      buffer <- updatedBuffer
      res  

    member this.Write<'a> (writer: Binary.Segment -> Binary.Segment) = 
      buffer <- writer buffer
    
    member this.Buffer = buffer

    member this.ShiftOffset(by: int) = this.Write(Binary.shiftOffset by)

    member this.Seek(offset: int) = 
      buffer <- Binary.Segment(buffer.Array, offset, buffer.Count)

    member this.WriteInt32(x)     = this.Write(Binary.writeInt32 x)
    member this.WriteBlock(block) = this.Write(Binary.writeBlock block)

    member this.ReadInt32()       = this.Read(Binary.readInt32)
    member this.ReadBlock(length) = this.Read(Binary.readBlock length)

  module internal CompressedMessage = 

    module private Header =     
      // Magic string used by snappy-java.
      let magic = [| byte -126; byte 'S'; byte 'N'; byte 'A'; byte 'P'; byte 'P'; byte 'Y'; byte 0 |]
      // Current version number taken from snappy-java repo as of 22/05/2017.
      let currentVer = 1
      // Minimum compatible version number taken from snappy-java repo as of 22/05/2017.
      let minimumVer = 1
      // Total size of the header (magic string + two version ints + content length int)
      let size = magic.Length + Binary.sizeInt32 currentVer + Binary.sizeInt32 minimumVer + Binary.sizeInt32 0

    let compress (bytes: Binary.Segment) : Binary.Segment =
      let maxLength = SnappyCodec.GetMaxCompressedLength(bytes.Count)

      let buf = Array.zeroCreate (Header.size + maxLength)
      let bz = SnappyBinaryZipper(Binary.ofArray buf)
      
      // write header compatible with snappy-java.
      bz.WriteBlock (Binary.ofArray Header.magic)
      bz.WriteInt32 (Header.currentVer)
      bz.WriteInt32 (Header.minimumVer)
      
      // move forward to write compressed content, then go back to write the actual compressed content length.
      bz.ShiftOffset (Binary.sizeInt32 0)
      
      let length = SnappyCodec.Compress(bytes.Array, bytes.Offset, bytes.Count, bz.Buffer.Array, bz.Buffer.Offset)      
      
      bz.Seek (Header.size - Binary.sizeInt32 length)
      bz.WriteInt32 (length)
      
      Binary.truncateIfSmaller (Header.size + length) (Header.size + maxLength) buf    

    let decompress (bytes: Binary.Segment) : Binary.Segment =
      let bz = SnappyBinaryZipper(bytes)
      
      // TODO: do we want to validate these?
      let magic      = bz.ReadBlock(Header.magic.Length)
      let currentVer = bz.ReadInt32()
      let minimumVer = bz.ReadInt32()

      let contentLength = bz.ReadInt32()
      let content = bz.ReadBlock(contentLength)

      let uncompressedLength = SnappyCodec.GetUncompressedLength(content.Array, content.Offset, content.Count) 

      let buf = Array.zeroCreate uncompressedLength
      let actualLength = SnappyCodec.Uncompress(content.Array, content.Offset, content.Count, buf, 0)

      Binary.truncateIfSmaller actualLength uncompressedLength buf

  let compress (messageVer:ApiVersion) (ms:MessageSet) =
    let inputBytes = MessageSet.Size (messageVer,ms) |> Array.zeroCreate
    let buf = Binary.ofArray inputBytes
    MessageSet.Write (messageVer,ms,BinaryZipper(buf))

    let output = CompressedMessage.compress buf 

    createMessage output CompressionCodec.Snappy
    
  let decompress (messageVer:ApiVersion) (m:Message) =
    let output = CompressedMessage.decompress m.value 
    let bz = BinaryZipper(output)
    MessageSet.Read (messageVer, 0, 0s, output.Count, bz)
  
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
    