namespace Kafunk

open System
open System.Text

/// Big-endian operations on binary data.
module Binary =

  type Segment = ArraySegment<byte>

  type Reader<'a> = Segment -> 'a * Segment

  type Writer<'a> = 'a -> Segment -> Segment

  let empty : Segment =
    Segment()

  let inline zeros (count : int) : Segment =
    Segment(Array.zeroCreate count)

  /// Sets the size of the segment, leaving the offset the same.
  let inline resize (count : int) (a : Segment) =
    Segment(a.Array, a.Offset, count)

  /// Sets the offset, adjusting count as needed.
  let inline offset (offset : int) (a : Segment) =
    Segment(a.Array, offset, (a.Count - (offset - a.Offset)))

  /// Shifts the offset by the specified amount, adjusting count as needed.
  let inline shiftOffset (d : int) (a : Segment) : Segment =
    offset (a.Offset + d) a

  let inline ofArray (bytes : byte[]) : Segment =
    Segment(bytes, 0, bytes.Length)

  let inline toArray (s : Segment) : byte[] =
    let arr = Array.zeroCreate s.Count
    System.Buffer.BlockCopy(s.Array, s.Offset, arr, 0, s.Count)
    arr

  let inline toString (buf : Segment) : string =
    if isNull buf.Array then null
    else Encoding.UTF8.GetString(buf.Array, buf.Offset, buf.Count)

  let inline copy (src : Segment) (dest : Segment) (count : int) =
    System.Buffer.BlockCopy(src.Array, src.Offset, dest.Array, dest.Offset, count)

  let inline sizeBool (_:bool) = 1

  let inline peekBool (buf: Segment) : bool =
    System.Convert.ToBoolean(buf.Array.[buf.Offset])
  
  let inline readBool (buf: Segment) : bool * Segment =
    (peekBool buf, (buf |> shiftOffset 1))
  
  let inline pokeBool (x : bool) (buf : Segment) =
    buf.Array.[buf.Offset] <- System.Convert.ToByte(x)
    buf

  let inline writeBool (x: bool) (buf: Segment) =
    pokeBool x buf |> shiftOffset 1

  let inline sizeInt8 (_:int8) = 1

  let inline peekInt8Offset (buf : Segment) (offset: int) : int8 =
    int8 buf.Array.[buf.Offset + offset]

  let inline peekInt8 (buf : Segment) : int8 =
    peekInt8Offset buf 0

  let inline readInt8 (buf : Segment) : int8 * Segment =
    (peekInt8 buf, (buf |> shiftOffset 1))

  let inline pokeInt8 (x : int8) (buf : Segment) =
    buf.Array.[buf.Offset] <- byte x
    buf

  let inline writeInt8 (x : int8) (buf : Segment) =
    pokeInt8 x buf |> shiftOffset 1

  let inline sizeInt16 (_:int16) = 2

  let inline peekInt16 (buf : Segment) : int16 =
    let offset = buf.Offset
    let array = buf.Array
    (int16 array.[offset + 0] <<< 8) ||| (int16 array.[offset + 1])

  let inline readInt16 (buf : Segment) : int16 * Segment =
    (peekInt16 buf, buf |> shiftOffset 2)

  let inline pokeInt16 (x : int16) (buf : Segment) =
    let offset = buf.Offset
    let array = buf.Array
    array.[offset] <- byte (x >>> 8)
    array.[offset + 1] <- byte x
    buf

  let inline writeInt16 (x : int16) (buf : Segment) =
    pokeInt16 x buf |> shiftOffset 2

  let inline sizeInt32 (_:int32) = 4

  let inline peekInt32 (buf : Segment) : int32 =
    let offset = buf.Offset
    let array = buf.Array
    (int32 array.[offset + 0] <<< 24) |||
    (int32 array.[offset + 1] <<< 16) |||
    (int32 array.[offset + 2] <<< 8) |||
    (int32 array.[offset + 3] <<< 0)

  let inline readInt32 (buf : Segment) : int32 * Segment =
    (peekInt32 buf, (buf |> shiftOffset 4))

  let inline pokeInt32 (x : int32) (buf : Segment) =
    let offset = buf.Offset
    let array = buf.Array
    array.[offset + 0] <- byte (x >>> 24)
    array.[offset + 1] <- byte (x >>> 16)
    array.[offset + 2] <- byte (x >>> 8)
    array.[offset + 3] <- byte x
    buf

  let inline writeInt32 (x : int32) (buf : Segment) =
    pokeInt32 x buf |> shiftOffset 4

  let inline sizeInt64 (_:int64) = 8

  let inline peekInt64 (buf : Segment) : int64 =
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

  let inline readInt64 (buf : Segment) : int64 * Segment =
    (peekInt64 buf, (buf |> shiftOffset 8))

  let inline pokeInt64 (x : int64) (buf : Segment) =
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

  let inline writeInt64 (x : int64) (buf : Segment) =
    pokeInt64 x buf |> shiftOffset 8

  let inline read2
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (buf : Segment)
    : ('a * 'b) * Segment =
    let a, buf = readA buf
    let b, buf = readB buf
    ((a, b), buf)

  let inline read3
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (buf : Segment)
    : ('a * 'b * 'c) * Segment =
    let a, buf = readA buf
    let b, buf = readB buf
    let c, buf = readC buf
    ((a, b, c), buf)

  let inline read4
    (readA : Reader<'a>)
    (readB : Reader<'b>)
    (readC : Reader<'c>)
    (readD : Reader<'d>)
    (buf : Segment)
    : ('a * 'b * 'c * 'd) * Segment =
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
    (buf : Segment)
    : ('a * 'b * 'c * 'd * 'e) * Segment =
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
    (buf : Segment)
    : ('a * 'b * 'c * 'd * 'e * 'f) * Segment =
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
    (buf : Segment)
    : Segment =
    buf |> writeA a |> writeB b

  let inline write3
    (writeA : Writer<'a>)
    (writeB : Writer<'b>)
    (writeC : Writer<'c>)
    ((a, b, c) : ('a * 'b * 'c))
    (buf : Segment)
    : Segment =
    buf |> writeA a |> writeB b |> writeC c

  let inline sizeBytes (bytes:Segment) =
      sizeInt32 bytes.Count + bytes.Count

  let inline writeBytes (bytes:Segment) buf =
    if isNull bytes.Array then
      writeInt32 -1 buf
    else
      let buf = writeInt32 bytes.Count buf
      System.Buffer.BlockCopy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      buf |> shiftOffset bytes.Count

  let inline readBytes (buf:Segment) : Segment * Segment =
    let length, buf = readInt32 buf
    if length = -1 then
      (empty, buf)
    else
      let arr = Segment(buf.Array, buf.Offset, length)
      (arr, buf |> shiftOffset length)

  // TODO: Do we need to support non-ascii values here? This currently
  // assumes each character is always encoded in UTF-8 by a single byte.
  // We should also error out for strings wich are too long for the
  // int16 to represent.
  let inline sizeString (str:string) =
    if isNull str then sizeInt16 0s
    else sizeInt16 (int16 str.Length) + str.Length

  let inline writeString (str : string) (buf : Segment) =
    if isNull str then
      writeInt16 -1s buf
    else
      let buf = writeInt16 (int16 str.Length) buf
      let read = Encoding.UTF8.GetBytes(str, 0, str.Length, buf.Array, buf.Offset)
      buf |> shiftOffset read

  let inline readString (buf : Segment) : string * Segment =
    let length, buf = readInt16 buf
    let length = int length
    if length = -1 then
      (null, buf)
    else
      let str = Encoding.UTF8.GetString (buf.Array, buf.Offset, length)
      (str, buf |> shiftOffset length)

  let inline sizeArray (a : 'a []) (size : 'a -> int) =
    sizeInt32 a.Length + (a |> Array.sumBy size)

  let inline writeArray (arr : 'a[]) (write : Writer<'a>) (buf : Segment) : Segment =
    if isNull arr then
      writeInt32 -1 buf
    else
      let mutable buf = writeInt32 arr.Length buf
      for i = 0 to arr.Length - 1 do
        buf <- write arr.[i] buf
      buf

  let readArray (read : Reader<'a>) (buf : Segment) : 'a[] * Segment =
    let n, buf = readInt32 buf
    let mutable buf = buf
    let arr = Array.zeroCreate n
    for i = 0 to n - 1 do
      let elem, buf' = read buf
      arr.[i] <- elem
      buf <- buf'
    (arr, buf)

  let inline writeArrayNoSize buf arr (write : Writer<'a>) =
    let mutable buf = buf
    for a in arr do
      buf <- write a buf
    buf

  let readArrayByteSize (expectedSize:int) (buf:Segment) (read:int -> Segment -> Choice<'a * Segment, Segment>) =
    let mutable buf = buf
    let mutable consumed = 0
    let mutable i = 0
    let arr = ResizeArray<_>()
    while consumed < expectedSize && buf.Count > 0 do
      match read consumed buf with
      | Choice1Of2 (elem,buf') ->
        arr.Add elem
        consumed <- consumed + (buf'.Offset - buf.Offset)
        buf <- buf'
        i <- i + 1
      | Choice2Of2 buf' ->
        consumed <- consumed + (buf'.Offset - buf.Offset)
        buf <- buf'
    (arr.ToArray(), buf)

//[<Struct>]
type BinaryZipper (buf:ArraySegment<byte>) =
  
  let mutable buf = buf

  member __.Buffer 
    with get () = buf 
    and set b = buf <- b

  member __.ShiftOffset (n) =
    buf <- Binary.shiftOffset n buf

  member __.ReadBool () : bool =
    let r = Binary.peekBool buf
    r
  
  member __.WriteBool (x:bool) =
    buf <- Binary.writeBool x buf

  member __.PeekInt8Offset (offset:int) : int8 =
    Binary.peekInt8Offset buf offset

  member __.TryPeekInt8Offset (offset:int) : int8 =
    if buf.Array.Length > buf.Offset + offset then
      Binary.peekInt8Offset buf offset
    else
      0y

  member __.ReadInt8 () : int8 =
    let r = Binary.peekInt8 buf
    __.ShiftOffset 1
    r
  
  member __.WriteInt8 (x:int8) =
    buf <- Binary.writeInt8 x buf

  member __.PeekIn16 () = 
    Binary.peekInt16 buf

  member __.ReadInt16 () : int16 =
    let r = Binary.peekInt16 buf
    __.ShiftOffset 2
    r

  member __.WriteInt16 (x:int16) =
    buf <- Binary.writeInt16 x buf

  member __.PeekIn32 () : int32 = Binary.peekInt32 buf

  member __.ReadInt32 () : int32 =
    let r = Binary.peekInt32 buf
    __.ShiftOffset 4
    r

  member __.WriteInt32 (x:int32) =
    buf <- Binary.writeInt32 x buf

  member __.PeekIn64 () : int64 = Binary.peekInt64 buf

  member __.ReadInt64 () : int64 =
    let r = Binary.peekInt64 buf
    buf <- Binary.shiftOffset 8 buf
    r

  member __.WriteInt64 (x:int64) =
    buf <- Binary.writeInt64 x buf

  member __.ReadVarint () : int64 =
    /// Mutable counter indicating the number of bits that the next 7 bits should
    /// be shifted by into the final value.
    let mutable shiftBy = 0

    /// Mutable accumulator to store the final result.
    let mutable result = 0L

    /// Mutable flag to control the loop.
    let mutable loop = true

    /// A bitmask used to identify the high bit in a byte
    let msbMask = 0b1000_0000y

    while loop do
      let next = __.ReadInt8()

      let valueToShiftIntoResult =
        if (next &&& msbMask) = 0y then
          // If the highest bit is not set, then we're done.
          loop <- false
          next
        else
          // The MSB is only a flag indicating whether or not we should
          // keep scanning, it should be removed from the final value.
          next ^^^ msbMask

      result <- result ||| (int64 valueToShiftIntoResult <<< shiftBy)
      shiftBy <- shiftBy + 7

    // BUG: Kafka is currently doubling all varints?
    result / 2L

  member __.WriteBytes (bytes:ArraySegment<byte>) =
    if isNull bytes.Array then
      __.WriteInt32 -1
    else
      __.WriteInt32 bytes.Count |> ignore
      System.Buffer.BlockCopy(bytes.Array, bytes.Offset, buf.Array, buf.Offset, bytes.Count)
      __.ShiftOffset bytes.Count

  member __.ReadBytes () : ArraySegment<byte> =
    let length = __.ReadInt32 ()
    if length = -1 then
      //Binary.empty
      ArraySegment<byte>(buf.Array, buf.Offset, 0)
    else
      let arr = ArraySegment<byte>(buf.Array, buf.Offset, length)
      __.ShiftOffset length
      arr

  member __.ReadVarintBytes () : ArraySegment<byte> =
    let length = __.ReadVarint () |> int

    if length = -1 then
      ArraySegment<byte>(buf.Array, buf.Offset, 0)
    else
      let arr = ArraySegment<byte>(buf.Array, buf.Offset, length)
      __.ShiftOffset length
      arr

  member __.ReadArrayByteSize (expectedSize:int, read:int -> 'a option) =
    let mutable consumed = 0
    let arr = ResizeArray<_>()
    while consumed < expectedSize && buf.Count > 0 do
      let o' = buf.Offset
      match read consumed with
      | Some a -> arr.Add a
      | _ -> ()
      consumed <- consumed + (buf.Offset - o')
    arr.ToArray()

  member __.ReadString () : string =
    let length = __.ReadInt16 ()
    let length = int length
    if length = -1 then
      null
    else
      let str = Encoding.UTF8.GetString (buf.Array, buf.Offset, length)
      __.ShiftOffset length
      str

  member __.ReadArray (read:BinaryZipper -> 'a) : 'a[] =
    let n = __.ReadInt32 ()
    let arr = Array.zeroCreate n
    for i = 0 to n - 1 do
      arr.[i] <- read __
    arr

  member __.WriteArray (arr:'a[], write:BinaryZipper * 'a -> unit) =
    if isNull arr then
      __.WriteInt32 (-1)
    else
      __.WriteInt32 (arr.Length)
      for i = 0 to arr.Length - 1 do
        write (__,arr.[i])

  member __.WriteString (s:string) =
    if isNull s then
      __.WriteInt16 -1s
    else
      __.WriteInt16 (int16 s.Length)
      let read = Encoding.UTF8.GetBytes(s, 0, s.Length, buf.Array, buf.Offset)
      __.ShiftOffset read

