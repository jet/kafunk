namespace Kafunk

/// CRC-32
module Crc =
    /// Calculate a crc32 checksum over a range of a byte array starting with
    /// an offset and a count and returning an 32 unsigned bits.
    val crc32 : byte[] -> offset:int -> count:int -> uint32