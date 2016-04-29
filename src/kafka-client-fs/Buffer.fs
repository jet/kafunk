namespace KafkaFs

open System
open System.Text

open KafkaFs.Prelude

/// Delimits an array of bytes with operators for operatoring on different
/// word sizes in big endian format. I wish .NET didn't collide with this
/// name in System but outside of copying data quickly, it's not all that
/// helpful so we're taking it back and will use System.Buffer to refer to
/// the other.
type Buffer = ArraySegment<byte>

/// Operations on array segments.
[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Buffer =

  type Reader<'a> = Buffer -> 'a * Buffer

  type Writer<'a> = 'a -> Buffer -> Buffer

  /// Create an empty buffer.
  let empty : Buffer =
    Buffer()

  /// Create a zero'ed buffer with the specified number of bytes.
  let inline zeros (count : int) : Buffer =
    Buffer(Array.zeroCreate count)

  /// Create a buffer that is backed by the same array.
  /// TODO: The offset and count are relative to the array, not the buffer,
  /// should we allow this? It'd make more sense to make the offset relative
  /// to the buffer itself.
  let inline segment (offset : int) (count : int) (a : Buffer) =
    Buffer(a.Array, offset, count)

  /// Set a new count for the given buffer by constructing a new buffer.
  let inline resize (count : int) (a : Buffer) =
    Buffer(a.Array, a.Offset, count)

  /// Set the offset of the segment.
  let inline offset (offset : int) (a : Buffer) =
    segment offset (a.Count - (offset - a.Offset)) a

  /// Shifts the offset to the right by the specified amount.
  let inline shiftOffset (d : int) (a : Buffer) : Buffer =
    offset (a.Offset + d) a

  let inline ofArray (bytes : byte[]) : Buffer =
    Buffer(bytes, 0, bytes.Length)

  /// Create a copy of the byte array backing this buffer.
  let inline toArray (s : Buffer) : byte[] =
    let arr = Array.zeroCreate s.Count
    System.Buffer.BlockCopy(s.Array, s.Offset, arr, 0, s.Count)
    arr

  let inline toString (buf : Buffer) : string =
    Encoding.UTF8.GetString(buf.Array, buf.Offset, buf.Count)

  let append (a : Buffer) (b : Buffer) : Buffer =
    if a.Count = 0 then b
    elif b.Count = 0 then a
    else
      let arr = Array.zeroCreate (a.Count + b.Count)
      System.Buffer.BlockCopy(a.Array, a.Offset, arr, 0, a.Count)
      System.Buffer.BlockCopy(b.Array, b.Offset, arr, a.Count, b.Count)
      ofArray arr

  /// Partitions an array segment at the specified index.
  let partitionAt i (a : Buffer) : Buffer * Buffer =
    let first = Buffer(a.Array, a.Offset, a.Offset + i)
    let rest = Buffer(a.Array, (a.Offset + i), (a.Count - i))
    first, rest

  let inline copy (src : Buffer) (dest : Buffer) (count : int) =
    System.Buffer.BlockCopy(src.Array, src.Offset, dest.Array, dest.Offset, count)

  let inline peek (read : Buffer -> 'a * Buffer) (buf : Buffer) =
    let v, _ = read buf
    v

  let inline poke (write : 'a -> Buffer -> Buffer) (x : 'a) (buf : Buffer) =
    write x buf |> ignore
    buf

  let inline sizeByte (_:byte) = 1

  let inline peekByte (buf : Buffer) : byte =
    buf.Array.[buf.Offset]

  let inline readByte (buf : Buffer) : byte * Buffer =
    (peekByte buf, (buf |> shiftOffset 1))

  let inline pokeByte (x : byte) (buf : Buffer) =
    buf.Array.[buf.Offset] <- x
    buf

  let inline writeByte (x : byte) (buf : Buffer) =
    pokeByte x buf |> shiftOffset 1

  let inline sizeInt16 (_:int16) = 2

  let inline peekInt16 (buf : Buffer) : int16 =
    let offset = buf.Offset
    let array = buf.Array
    (int16 array.[offset + 0] <<< 8) ||| (int16 array.[offset + 1])

  let inline readInt16 (buf : Buffer) : int16 * Buffer =
    (peekInt16 buf, buf |> shiftOffset 2)

  let inline pokeInt16 (x : int16) (buf : Buffer) =
    let offset = buf.Offset
    let array = buf.Array
    array.[offset] <- byte (x >>> 8)
    array.[offset + 1] <- byte x
    buf

  let inline writeInt16 (x : int16) (buf : Buffer) =
    pokeInt16 x buf |> shiftOffset 2

  let inline sizeInt32 (_:int32) = 4

  let inline peekInt32 (buf : Buffer) : int32 =
    let offset = buf.Offset
    let array = buf.Array
    (int32 array.[offset + 0] <<< 24) |||
    (int32 array.[offset + 1] <<< 16) |||
    (int32 array.[offset + 2] <<< 8) |||
    (int32 array.[offset + 3] <<< 0)

  let inline readInt32 (buf : Buffer) : int32 * Buffer =
    (peekInt32 buf, (buf |> shiftOffset 4))

  let inline pokeInt32 (x : int32) (buf : Buffer) =
    let offset = buf.Offset
    let array = buf.Array
    array.[offset + 0] <- byte (x >>> 24)
    array.[offset + 1] <- byte (x >>> 16)
    array.[offset + 2] <- byte (x >>> 8)
    array.[offset + 3] <- byte x
    buf

  let inline writeInt32 (x : int32) (buf : Buffer) =
    pokeInt32 x buf |> shiftOffset 4

  let inline sizeInt64 (_:int64) = 8

  let inline peekInt64 (buf : Buffer) : int64 =
    let offset = buf.Offset
    let array = buf.Array
    (int64 array.[offset + 0] <<< 56) |||
    (int64 array.[offset + 1] <<< 48) |||
    (int64 array.[offset + 2] <<< 40) |||
    (int64 array.[offset + 3] <<< 32) |||
    (int64 array.[offset + 4] <<< 24) |||
    (int64 array.[offset + 5] <<< 16) |||
    (int64 array.[offset + 6] <<< 8) |||
    (int64 array.[offset + 7])

  let inline readInt64 (buf : Buffer) : int64 * Buffer =
    (peekInt64 buf, (buf |> shiftOffset 8))

  let inline pokeInt64 (x : int64) (buf : Buffer) =
    let offset = buf.Offset
    let array = buf.Array
    array.[offset + 0] <- byte (x >>> 56)
    array.[offset + 1] <- byte (x >>> 48)
    array.[offset + 2] <- byte (x >>> 40)
    array.[offset + 3] <- byte (x >>> 32)
    array.[offset + 4] <- byte (x >>> 24)
    array.[offset + 5] <- byte (x >>> 16)
    array.[offset + 6] <- byte (x >>> 8)
    array.[offset + 7] <- byte x
    buf

  let inline writeInt64 (x : int64) (buf : Buffer) =
    pokeInt64 x buf |> shiftOffset 8

  let inline read2
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (buf : Buffer)
    : ('a * 'b) * Buffer =
    let a, buf = readA buf
    let b, buf = readB buf
    ((a, b), buf)

  let inline read3
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (buf : Buffer)
    : ('a * 'b * 'c) * Buffer =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    ((a, b, c), buf)

  let inline read4
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (readD : Reader<'d>)
    (buf : Buffer)
    : ('a * 'b * 'c * 'd) * Buffer =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    let d, buf = readD buf
    ((a, b, c, d), buf)

  let inline read5
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (readD : Reader<'d>)
    (readE : Reader<'e>)
    (buf : Buffer)
    : ('a * 'b * 'c * 'd * 'e) * Buffer =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    let d, buf = readD buf
    let e, buf = readE buf
    ((a, b, c, d, e), buf)

  let inline read6
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (readD : Reader<'d>)
    (readE : Reader<'e>)
    (readF : Reader<'f>)
    (buf : Buffer)
    : ('a * 'b * 'c * 'd * 'e * 'f) * Buffer =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    let d, buf = readD buf
    let e, buf = readE buf
    let f, buf = readF buf
    ((a, b, c, d, e, f), buf)

  let inline write2
    (writeA : Writer<'a>)
    (writeB : Writer<'b>)
    ((a, b) : ('a * 'b))
    (buf : Buffer)
    : Buffer =
    buf |> writeA a |> writeB b

  let inline write3
    (writeA : Writer<'a>)
    (writeB : Writer<'b>)
    (writeC : Writer<'c>)
    ((a, b, c) : ('a * 'b * 'c))
    (buf : Buffer)
    : Buffer =
    buf |> writeA a |> writeB b |> writeC c

  let inline write4
    (writeA : Writer<'a>)
    (writeB : Writer<'b>)
    (writeC : Writer<'c>)
    (writeD : Writer<'d>)
    ((a, b, c, d) : ('a * 'b * 'c * 'd))
    (buf : Buffer)
    : Buffer =
    buf |> writeA a |> writeB b |> writeC c |> writeD d

  let inline sizeBytes (bytes:Buffer) =
      sizeInt32 bytes.Count + bytes.Count

  let inline writeBytes (bytes : Buffer) buf =
    if isNull bytes.Array then
      writeInt32 -1 buf
    else
      let buf = writeInt32 bytes.Count buf
      Array.Copy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      buf |> shiftOffset bytes.Count

  let inline readBytes (buf : Buffer) : Buffer * Buffer =
    let length, buf = readInt32 buf
    if length = -1 then
      (empty, buf)
    else
      let arr = Buffer(buf.Array, buf.Offset, length)
      (arr, buf |> shiftOffset length)

  let inline sizeString (str:string) =
      if isNull str then sizeInt16 0s
      else
        // TODO: Do we need to support non-ascii values here? This currently
        // assumes each character is always encoded in UTF-8 by a single byte.
        sizeInt16 (int16 str.Length) + str.Length

  let inline writeString (str : string) (buf : Buffer) =
    if isNull str then
      writeInt16 -1s buf
    else
      let buf = writeInt16 (int16 str.Length) buf
      let read = Encoding.UTF8.GetBytes(str, 0, str.Length, buf.Array, buf.Offset)
      buf |> shiftOffset read

  let inline readString (buf : Buffer) : string * Buffer =
    let length, buf = readInt16 buf
    let length = int length
    if length = -1 then
      (null, buf)
    else
      let str = Encoding.UTF8.GetString (buf.Array, buf.Offset, length)
      (str, buf |> shiftOffset length)

  let inline sizeArray (a : 'a []) (size : 'a -> int) =
    sizeInt32 a.Length + (a |> Array.sumBy size)

  let inline writeArray (arr : 'a[]) (write : 'a -> Buffer -> Buffer) (buf : Buffer) : Buffer =
    if isNull arr then
      let buf = writeInt32 -1 buf
      buf
    else
      let n = arr.Length
      let buf = writeInt32 n buf
      Array.fold (fun buf elem -> write elem buf) buf arr

  let inline readArray (read : Buffer -> 'a * Buffer) (buf : Buffer) : 'a[] * Buffer =
    let n, buf = buf |> readInt32
    let mutable buf = buf
    let arr = [|
      for _i = 0 to n - 1 do
        let elem, buf' = read buf
        yield elem
        buf <- buf' |]
    (arr, buf)

  let inline writeArrayNoSize buf arr (write : Writer<'a>) =
    let mutable buf = buf
    for a in arr do
      buf <- write a buf
    buf

  let inline readArrayByteSize size buf (read : Reader<'a>) =
    let mutable buf = buf
    let mutable consumed = 0
    let arr = [|
      while consumed < size do
        let elem, buf' = read buf
        yield elem
        consumed <- consumed + (buf'.Offset - buf.Offset)
        buf <- buf' |]
    (arr, buf)