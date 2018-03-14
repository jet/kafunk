[<Compile(Module)>]
module Kafunk.Compression

open System.IO
open System.IO.Compression

open Kafunk

let private createMessage (value:Value) (compression:byte) =
  let attrs = compression |> int8
  //Message.create value Binary.empty (Some attrs)
  Message(0, 0y, attrs, 0L, Binary.empty, value)

let private ofMessage (m:Message) =
  MessageSet([| MessageSetItem(0L, Message.Size m, m) |])

[<RequireQualifiedAccess>]
module internal Stream = 

  // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
  let compress (codec:CompressionCodec) (makeStream:MemoryStream -> Stream) (ms:MessageSet) =
    use outputStream = new MemoryStream()
    do
      let buf = MessageSet.Size ms |> Binary.zeros
      MessageSet.Write (ms,BinaryZipper(buf))
      use compStream = makeStream outputStream
      compStream.Write(buf.Array, buf.Offset, buf.Count)
    let value = Binary.Segment(outputStream.GetBuffer(), 0, int outputStream.Length)
    createMessage value codec

  let decompress (makeStream:MemoryStream -> Stream) (magicByte:int8) (m:Message) =
    use outputStream = new MemoryStream()
    do
      use inputStream = new MemoryStream(m.value.Array, m.value.Offset, m.value.Count)
      use compStream = makeStream inputStream
      compStream.CopyTo(outputStream)
    let buf = Binary.Segment(outputStream.GetBuffer(), 0, int outputStream.Length)
    MessageSet.Read (magicByte, 0, 0s, buf.Count, true, BinaryZipper(buf))

[<Compile(Module)>]
module GZip =

  open System.IO
  open System.IO.Compression

  let compress ms =
    Stream.compress 
      CompressionCodec.GZIP 
      (fun memStream -> upcast new GZipStream(memStream, CompressionMode.Compress, true))
      ms

  let decompress ver m =
    Stream.decompress 
      (fun memStream -> upcast new GZipStream(memStream, CompressionMode.Decompress, true)) 
      ver 
      m
    
#if !NETSTANDARD2_0

[<Compile(Module)>]
module Snappy = 
  
  open System
  open Snappy
    
  module internal Binary = 

    let truncateIfSmaller actualLength maxLength (array: byte []) = 
      if actualLength < maxLength 
        then Binary.Segment(array, 0, actualLength)
        else Binary.ofArray array
        
  type internal SnappyBinaryZipper (buf:Binary.Segment) = 
      
    let mutable buf = buf
    
    member this.Buffer = buf

    member __.ShiftOffset (n) =
      buf <- Binary.shiftOffset n buf

    member this.Seek(offset: int) = 
      buf <- Binary.Segment(buf.Array, offset, buf.Count)

    member __.WriteInt32 (x:int32) =
      buf <- Binary.writeInt32 x buf

    member __.WriteBytes (bytes:ArraySegment<byte>) =
      System.Buffer.BlockCopy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      __.ShiftOffset bytes.Count

    member __.ReadInt32 () : int32 =
      let r = Binary.peekInt32 buf
      __.ShiftOffset 4
      r

    member __.ReadBytes (length:int) : ArraySegment<byte> =
      let arr = ArraySegment<byte>(buf.Array, buf.Offset, length)
      __.ShiftOffset length
      arr

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
      bz.WriteBytes (Binary.ofArray Header.magic)
      bz.WriteInt32 (Header.currentVer)
      bz.WriteInt32 (Header.minimumVer)
      
      // move forward to write compressed content, then go back to write the actual compressed content length.
      bz.ShiftOffset 4
      
      let length = SnappyCodec.Compress(bytes.Array, bytes.Offset, bytes.Count, bz.Buffer.Array, bz.Buffer.Offset)      
      
      bz.Seek (Header.size - Binary.sizeInt32 length)
      bz.WriteInt32 (length)
      
      Binary.truncateIfSmaller (Header.size + length) (Header.size + maxLength) buf    

    let decompress (bytes: Binary.Segment) : Binary.Segment =
      let bz = SnappyBinaryZipper(bytes)
      
      // TODO: do we want to validate these?
      let _magic      = bz.ReadBytes(Header.magic.Length)
      let _currentVer = bz.ReadInt32()
      let _minimumVer = bz.ReadInt32()

      let contentLength = bz.ReadInt32()
      let content = bz.ReadBytes(contentLength)

      let uncompressedLength = SnappyCodec.GetUncompressedLength(content.Array, content.Offset, content.Count) 
      let buf = Array.zeroCreate uncompressedLength
      let actualLength = SnappyCodec.Uncompress(content.Array, content.Offset, content.Count, buf, 0)
      Binary.truncateIfSmaller actualLength uncompressedLength buf

  let compress (ms:MessageSet) =
    let buf = MessageSet.Size ms |> Binary.zeros
    MessageSet.Write (ms,BinaryZipper(buf))
    let output = CompressedMessage.compress buf 
    createMessage output CompressionCodec.Snappy
    
  let decompress (magicByte:int8) (m:Message) =
    let buf = CompressedMessage.decompress m.value 
    MessageSet.Read (magicByte, 0, 0s, buf.Count, true, BinaryZipper(buf))

#endif
  
let compress (magicByte:int8) (compression:byte) (ms:MessageSet) =
  match compression with
  | CompressionCodec.None -> ms
  | _ ->
    if magicByte >= 2y then 
      failwithf "compression=%i not supported on message_format=%i" compression magicByte else
    // assign offsets
    let ms = 
      ms.messages 
      |> Array.mapi (fun i msi -> MessageSetItem(int64 i, msi.messageSize, msi.message))
      |> MessageSet
    match compression with
    | CompressionCodec.None -> ms
    | CompressionCodec.GZIP -> ofMessage (GZip.compress ms)

  #if !NETSTANDARD2_0
    | CompressionCodec.Snappy -> ofMessage (Snappy.compress ms)
  #endif

    | _ -> failwithf "Incorrect compression codec %A" compression
  
let decompress (magicByte:int8) (ms:MessageSet) =
  if ms.messages.Length = 0 then ms
  else
    let msis =
      ms.messages
      |> Array.Parallel.collect (fun msi -> 
        match (msi.message.attributes &&& (sbyte CompressionCodec.Mask)) |> byte with
        | CompressionCodec.None -> [|msi|]
        | CompressionCodec.GZIP ->
          let decompressed = GZip.decompress magicByte msi.message
          decompressed.messages

  #if !NETSTANDARD2_0    
        | CompressionCodec.Snappy ->
          let decompressed = Snappy.decompress magicByte msi.message
          decompressed.messages
  #endif

        | c -> failwithf "compression_code=%i not supported" c)
    MessageSet (msis, ms.lastOffset) // NB: lastOffset mast be propagated for magicByte>=2y support
    