namespace Kafunk

open Kafunk

[<Compile(Module)>]
module Compression =

  let private createMessage (value:Value) (compression:byte) =
    let attrs = compression |> int8
    Message.create value Binary.empty (Some attrs)

  [<Compile(Module)>]
  module GZip =

    open System.IO
    open System.IO.Compression

    // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
    let compress (messageVer:ApiVersion) (ms:MessageSet) =
      // TODO: pool MemoryStreams
      use outputStream = new MemoryStream()
      use gZipStream = new GZipStream(outputStream, CompressionMode.Compress)
      do
        let inputBytes = ms |> MessageSet.size |> Array.zeroCreate
        let buf = Binary.ofArray inputBytes
        MessageSet.write messageVer ms buf |> ignore
        try
          gZipStream.Write(inputBytes, 0, inputBytes.Length)
          gZipStream.Close()
        with :? IOException as ex ->
          // TODO: log this
          printfn "Couldn't write to gzip stream: %A" ex
          reraise()
      createMessage (outputStream.ToArray() |> Binary.ofArray) CompressionCodec.GZIP

    let decompress (messageVer:ApiVersion) (m:Message) =
      let inputBytes = m.value |> Binary.toArray
      // TODO: pool MemoryStreams
      use outputStream = new MemoryStream()
      do
        use inputStream = new MemoryStream(inputBytes)
        use gzipInputStream = new GZipStream(inputStream, CompressionMode.Decompress)
        try
          gzipInputStream.CopyTo(outputStream)
          gzipInputStream.Close()
        with :? IOException as ex ->
          // TODO: log this
          printfn "Couldn't read from gzip stream: %A" ex
          reraise()
      outputStream.Position <- 0L
      let output = outputStream.ToArray()
      // size is output array size divided by message set element size
      MessageSet.read (messageVer, 0, 0s, output.Length, (output |> Binary.ofArray))
      |> fst

  let compress (messageVer:int16) (compression:byte) (ms:MessageSet) =
    match compression with
    | CompressionCodec.None -> ms
    | CompressionCodec.GZIP -> MessageSet.ofMessage (GZip.compress messageVer ms)
    | CompressionCodec.Snappy -> failwithf "Snappy not supported yet!"
    | _ -> failwithf "Incorrect compression codec %A" compression

  let decompress (messageVer:int16) (ms:MessageSet) =
    if ms.messages.Length = 0 then ms
    elif ms.messages.Length > 1 then ms
    else
      let _,_,m = ms.messages.[0]
      match (m.attributes &&& (sbyte CompressionCodec.Mask)) |> byte with
      | CompressionCodec.None -> ms
      | CompressionCodec.GZIP -> GZip.decompress messageVer m
      | c -> failwithf "compression_code=%i not supported" c 