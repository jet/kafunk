[<Compile(Module)>]
module Kafunk.Compression

open System
open System.IO
open System.IO.Compression
open Kafunk

[<RequireQualifiedAccess>]
module internal Stream = 

  // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
  let compress (makeStream:MemoryStream -> Stream) (value:ArraySegment<byte>) =
    use outputStream = new MemoryStream()
    do
      use compStream = makeStream outputStream
      compStream.Write(value.Array, value.Offset, value.Count)
    ArraySegment(outputStream.GetBuffer(), 0, int outputStream.Length)

  let decompress (makeStream:MemoryStream -> Stream) (value:ArraySegment<byte>) =
    use outputStream = new MemoryStream()
    do
      use inputStream = new MemoryStream(value.Array, value.Offset, value.Count)
      use compStream = makeStream inputStream
      compStream.CopyTo(outputStream)
    ArraySegment(outputStream.GetBuffer(), 0, int outputStream.Length)

[<Compile(Module)>]
module GZip =

  open System.IO
  open System.IO.Compression

  let compress value =
    Stream.compress 
      (fun memStream -> upcast new GZipStream(memStream, CompressionMode.Compress, true))
      value

  let decompress value =
    Stream.decompress 
      (fun memStream -> upcast new GZipStream(memStream, CompressionMode.Decompress, true)) 
      value
    
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

#endif
  
[<Compile(Module)>]
module LZ4 =
    open Kafunk.Native

    let compress (value: ArraySegment<byte>) : ArraySegment<byte> =
        // TODO: consider preallocated buffer for compression
        let compressedBound = Lz4Framing.compressFrameBound value.Count
        let compressedBuffer = Array.zeroCreate compressedBound
        Lz4Framing.compressFrame value compressedBuffer

    let decompress (value: ArraySegment<byte>) : ArraySegment<byte> =
        Lz4Framing.decompress value |> ArraySegment