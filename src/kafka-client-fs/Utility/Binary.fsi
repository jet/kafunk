namespace Kafunk

/// The binary module offers functions for working with octet arrays. The
/// primary concepts are read and write functions which operate on preallocated
/// arrays. The size* functions are used to decide what the byte-size of a
/// given structure would be if written. This is used when preallocating the
/// array of bytes used to back the buffer (represented as an
/// ArraySegment<byte> internally).
///
/// Since the segments returned are new values one can keep old Binary.Segment
/// instances to allow referencing to old frames of refernce. This comes into
/// use when calculating things like checksums after writing data. So, while
/// the underlying bytes are mutable, the segments themsevles are not treated
/// as such.
///
/// The peek and poke operations are just like read and write respectively
/// other than the fact that they don't return a new buffer segment window
/// adjusted to start right after the data read. Read and write are implemented
/// in terms of these lower level operations.
///
/// The types the buffer is designed to work with are signed. This is inherited
/// from the design documents of the Kafka protocol which is inheriting Java's
/// lack of unsigned types. So when you see int8 rather than byte, it's not a
/// mistake.
module Binary =

    /// Delimits an array of bytes with operators for operatoring on different
    /// word sizes in big endian format
    type Segment = System.ArraySegment<byte>

    /// A function which reads a value from a byte segment
    type Reader<'a> = Segment -> 'a * Segment

    /// A function which writes a value to a byte segment
    type Writer<'a> = 'a -> Segment -> Segment

    /// Create an empty buffer
    val empty : Segment

    /// Create a zero'ed buffer with the specified number of bytes
    val inline zeros : int -> Segment

    /// Set a new count for the given buffer by constructing a new buffer
    val inline resize : int -> Segment -> Segment

    /// Set the offset of the segment
    val inline offset : int -> Segment -> Segment

    /// Shifts the offset to the right by the specified amount
    val inline shiftOffset : int -> Segment -> Segment

    /// Create a copy of the byte array backing this buffer
    val inline ofArray : byte [] -> Segment

    val inline toArray : Segment -> byte []

    val inline toString : Segment -> string

    val append : Segment -> Segment -> Segment

    /// Partitions an array segment at the specified index
    val partitionAt : int -> Segment -> Segment * Segment

    val inline copy : Segment -> Segment -> int -> unit

    val inline peek : (Segment -> 'a * Segment) -> Segment -> 'a
    val inline poke : ('a -> Segment -> Segment) -> 'a -> Segment -> Segment

    val inline sizeInt8 : int8 -> int
    val inline peekInt8 : Segment -> int8
    val inline readInt8 : Segment -> int8 * Segment
    val inline pokeInt8 : int8 -> Segment -> Segment
    val inline writeInt8 : int8 -> Segment -> Segment

    val inline sizeInt16 : int16 -> int
    val inline peekInt16 : Segment -> int16
    val inline readInt16 : Segment -> int16 * Segment
    val inline pokeInt16 : int16 -> Segment -> Segment
    val inline writeInt16 : int16 -> Segment -> Segment

    val inline sizeInt32 : int32 -> int
    val inline peekInt32 : Segment -> int32
    val inline readInt32 : Segment -> int32 * Segment
    val inline pokeInt32 : int32 -> Segment -> Segment
    val inline writeInt32 : int32 -> Segment -> Segment

    val inline sizeInt64 : int64 -> int
    val inline peekInt64 : Segment -> int64
    val inline readInt64 : Segment -> int64 * Segment
    val inline pokeInt64 : int64 -> Segment -> Segment
    val inline writeInt64 : int64 -> Segment -> Segment

    /// Read a pair given size functions and a segment
    val inline read2 :
        Reader<'a> -> Reader<'b> ->
        Segment -> ('a * 'b) * Segment

    /// Read a 3-element tuple given size functions and a segment
    val inline read3 :
        Reader<'a> -> Reader<'b> -> Reader<'c> ->
        Segment -> ('a * 'b * 'c) * Segment

    /// Read a 4-element tuple given size functions and a segment
    val inline read4 :
        Reader<'a> -> Reader<'b> -> Reader<'c> -> Reader<'d> ->
        Segment -> ('a * 'b * 'c * 'd) * Segment

    /// Read a 5-element tuple given size functions and a segment
    val inline read5 :
        Reader<'a> -> Reader<'b> -> Reader<'c> -> Reader<'d> -> Reader<'e> ->
        Segment -> ('a * 'b * 'c * 'd * 'e) * Segment

    /// Read a 6-element tuple given size functions and a segment
    val inline read6 :
        Reader<'a> -> Reader<'b> -> Reader<'c> -> Reader<'d> -> Reader<'e> -> Reader<'f> ->
        Segment -> ('a * 'b * 'c * 'd * 'e * 'f) * Segment

    val inline write2 :
        Writer<'a> -> Writer<'b> ->
        'a * 'b -> Segment -> Segment

    val inline write3 :
        Writer<'a> -> Writer<'b> -> Writer<'c> ->
        'a * 'b * 'c -> Segment -> Segment

    val inline write4 :
        Writer<'a> -> Writer<'b> -> Writer<'c> -> Writer<'d> ->
        'a * 'b * 'c * 'd -> Segment -> Segment

    val inline sizeBytes : Segment -> int
    val inline writeBytes : Segment -> Segment -> Segment
    val inline readBytes : Segment -> Segment * Segment

    val inline sizeString : string -> int
    val inline writeString : string -> Segment -> Segment
    val inline readString : Segment -> string * Segment

    val inline sizeArray : 'a [] -> ('a -> int) -> int
    val inline writeArray : 'a [] -> Writer<'a> -> Segment -> Segment
    val inline readArray : Reader<'a> -> Segment -> 'a [] * Segment

    val inline writeArrayNoSize : Segment -> seq<'a> -> Writer<'a> -> Segment
    val inline readArrayByteSize : int -> Segment -> Reader<'a> -> 'a [] * Segment