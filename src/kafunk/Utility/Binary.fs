namespace Kafunk

open System
open System.Text
open Prelude

// big-endian

module Binary =

  type Segment = ArraySegment<byte>

  type Reader<'a> = Segment -> 'a * Segment

  type Writer<'a> = 'a -> Segment -> Segment

  let empty : Segment =
    Segment()

  let inline zeros (count : int) : Segment =
    Segment(Array.zeroCreate count)

  // TODO: Now is a perfect time to fix these functions up to make a little
  // more sense. These should likely have better names or clearer semantics.

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

  let append (a : Segment) (b : Segment) : Segment =
    if a.Count = 0 then b
    elif b.Count = 0 then a
    else
      let arr = Array.zeroCreate (a.Count + b.Count)
      System.Buffer.BlockCopy(a.Array, a.Offset, arr, 0, a.Count)
      System.Buffer.BlockCopy(b.Array, b.Offset, arr, a.Count, b.Count)
      ofArray arr

  let partitionAt i (a : Segment) : Segment * Segment =
    let first = Segment(a.Array, a.Offset, a.Offset + i)
    let rest = Segment(a.Array, (a.Offset + i), (a.Count - i))
    first, rest

  let inline copy (src : Segment) (dest : Segment) (count : int) =
    System.Buffer.BlockCopy(src.Array, src.Offset, dest.Array, dest.Offset, count)

  let inline peek (read : Segment -> 'a * Segment) (buf : Segment) =
    let v, _ = read buf
    v

  let inline poke (write : 'a -> Segment -> Segment) (x : 'a) (buf : Segment) =
    write x buf |> ignore
    buf

  let inline sizeInt8 (_:int8) = 1

  let inline peekInt8 (buf : Segment) : int8 =
    int8 buf.Array.[buf.Offset]

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

  let inline write4
    (writeA : Writer<'a>)
    (writeB : Writer<'b>)
    (writeC : Writer<'c>)
    (writeD : Writer<'d>)
    ((a, b, c, d) : ('a * 'b * 'c * 'd))
    (buf : Segment)
    : Segment =
    buf |> writeA a |> writeB b |> writeC c |> writeD d

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
      try
        let arr = Segment(buf.Array, buf.Offset, length)
        (arr, buf |> shiftOffset length)
      with ex ->
        printfn "expected length=%i array length=%i offset=%i" length buf.Array.Length buf.Offset
        reraise ()      

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
      let n = arr.Length
      let buf = writeInt32 n buf
      Array.fold (fun buf elem -> write elem buf) buf arr

  let inline readArray (read : Reader<'a>) (buf : Segment) : 'a[] * Segment =
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

  let inline readArrayByteSize (expectedSize:int) (buf:Segment) (read:Segment -> ('a * Segment) option) =    
    let mutable buf = buf
    let mutable consumed = 0
    let arr = [|
      while consumed < expectedSize && buf.Count > 0 do        
        match read buf with
        | Some (elem,buf') ->
          yield elem
          consumed <- consumed + (buf'.Offset - buf.Offset)
          buf <- buf'
        | None ->
          buf <- Segment()
    |]
    (arr, buf)