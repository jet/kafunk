[<Compile(Module)>]
module internal Kafunk.Compression

open Kafunk
open System.IO
open System.IO.Compression

let private Log = Log.create "Kafunk.Compression"

let private createMessage (value:Value) (compression:byte) =
  let attrs = compression |> int8
  Message.create value Binary.empty (Some attrs)

let private getMemoryStream () =
  // TODO: pool MemoryStreams
  new MemoryStream()   

let private getInputBytes (messageVer:ApiVersion) (ms:MessageSet) = 
  let inputBytes = MessageSet.Size (messageVer,ms) |> Array.zeroCreate
  let buf = Binary.ofArray inputBytes
  MessageSet.Write (messageVer,ms,BinaryZipper(buf))
  inputBytes

let private decompressMessage (messageVer:ApiVersion) (m:Message) (writeToOutputStream : byte[] -> MemoryStream -> unit) = 
  let inputBytes = m.value |> Binary.toArray
  use outputStream = getMemoryStream()
  
  writeToOutputStream inputBytes outputStream
  
  outputStream.Position <- 0L
  let output = outputStream.ToArray()
  // size is output array size divided by message set element size
  let bz = BinaryZipper(output |> Binary.ofArray)
  MessageSet.Read (messageVer, 0, 0s, output.Length, bz)

[<Compile(Module)>]
module GZip =
  
  let compress (messageVer:ApiVersion) (ms:MessageSet) =
    use outputStream = getMemoryStream()
    use gZipStream = new GZipStream(outputStream, CompressionMode.Compress)
    do
      let inputBytes = getInputBytes messageVer ms
      try
        gZipStream.Write(inputBytes, 0, inputBytes.Length)
        gZipStream.Close()
      with :? IOException as _ex ->
        Log.error "Couldn't write to gzip stream: %A" _ex
        reraise()
    createMessage (outputStream.ToArray() |> Binary.ofArray) CompressionCodec.GZIP

  let decompress (messageVer:ApiVersion) (m:Message) =
    let writeToOutputStream (inputBytes: byte[]) (outputStream: MemoryStream) = 
      do
        use inputStream = new MemoryStream(inputBytes)
        use gzipInputStream = new GZipStream(inputStream, CompressionMode.Decompress)
        try
          gzipInputStream.CopyTo(outputStream)
          gzipInputStream.Close()
        with :? IOException as _ex ->
          Log.error "Couldn't read from gzip stream: %A" _ex
          reraise()
    decompressMessage messageVer m writeToOutputStream

[<Compile(Module)>]
module Snappy =

  open Snappy

  let compress (messageVer:ApiVersion) (ms:MessageSet) =
    use outputStream = getMemoryStream()
    use snappyStream = new SnappyStream(outputStream, CompressionMode.Compress)
    do
      let inputBytes = getInputBytes messageVer ms
      try
        snappyStream.Write(inputBytes, 0, inputBytes.Length)
        snappyStream.Flush()
      with :? IOException as _ex ->
        Log.error "Couldn't write to snappy stream: %A" _ex
        reraise()
    createMessage (outputStream.ToArray() |> Binary.ofArray) CompressionCodec.Snappy

  let decompress (messageVer:ApiVersion) (m:Message) =
    let writeToOutputStream (inputBytes: byte[])  (outputStream: MemoryStream) = 
      do
          use inputStream = new MemoryStream(inputBytes)
          use snappyInputStream = new SnappyStream(inputStream, CompressionMode.Decompress)
          try
          snappyInputStream.CopyTo(outputStream)
          with :? IOException as _ex ->
          Log.error "Couldn't read from snappy stream: %A" _ex
          reraise()
    decompressMessage messageVer m writeToOutputStream

// The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
let compress (messageVer:int16) (compression:byte) (ms:MessageSet) =
  match compression with
  | CompressionCodec.None -> ms
  | CompressionCodec.GZIP -> MessageSet.ofMessage messageVer (GZip.compress messageVer ms)
  | CompressionCodec.Snappy -> MessageSet.ofMessage messageVer (Snappy.compress messageVer ms)
  | _ -> failwithf "Incorrect compression codec %A" compression

//let decompress (messageVer:int16) (ms:MessageSet) =
//  if ms.messages.Length = 0 then ms
//  else
//    let rs = ResizeArray<_>(ms.messages.Length)
//    for i = 0 to ms.messages.Length - 1 do
//      let msi = ms.messages.[i]
//      match (msi.message.attributes &&& (sbyte CompressionCodec.Mask)) |> byte with
//      | CompressionCodec.None -> 
//        rs.Add msi
//      | CompressionCodec.GZIP ->
//        let ms' = GZip.decompress messageVer msi.message
//        for j = 0 to ms'.messages.Length - 1 do
//          rs.Add(ms'.messages.[j])
//      | c -> 
//        failwithf "compression_code=%i not supported" c
//    MessageSet(rs.ToArray())

let decompress (messageVer:int16) (ms:MessageSet) =
  if ms.messages.Length = 0 then ms
  else
    ms.messages
    |> Array.Parallel.collect (fun msi -> 
      match (msi.message.attributes &&& (sbyte CompressionCodec.Mask)) |> byte with
      | CompressionCodec.None -> [|msi|]
      | CompressionCodec.GZIP ->
        let ms' = GZip.decompress messageVer msi.message
        ms'.messages
      | CompressionCodec.Snappy ->
        let ms' = Snappy.decompress messageVer msi.message
        ms'.messages
      | c -> failwithf "compression_code=%i not supported" c)
    |> MessageSet
    