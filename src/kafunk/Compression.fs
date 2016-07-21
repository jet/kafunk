namespace Kafunk

open Kafunk
open System.IO
open System.IO.Compression

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Compression =

    let private createMessage value compression = 
        let attrs = compression |> sbyte |> Some
        Message.create value None attrs

    // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
    let gzip messages =
        use outputStream = new MemoryStream()
        use gZipStream = new GZipStream(outputStream, CompressionMode.Compress)
        do
            let messageSet = messages |> MessageSet.ofMessages
            let inputBytes = messageSet |> Protocol.MessageSet.size |> Array.zeroCreate
            let segment = Binary.ofArray inputBytes
            (Protocol.MessageSet.write messageSet segment) |> ignore
            try
                gZipStream.Write(inputBytes, 0, inputBytes.Length)
                gZipStream.Close()
            with :? IOException as ex ->
                // TODO: log this
                printfn "Couldn't write to gzip stream: %A" ex
                reraise()
        createMessage (outputStream.ToArray() |> Binary.ofArray) Protocol.CompressionCodec.GZIP

    let decompress (message:Protocol.Message) =
        match (message.attributes &&& (sbyte Protocol.CompressionCodec.Mask)) |> byte with
        | Protocol.CompressionCodec.GZIP ->
            let inputBytes = message.value |> Binary.toArray
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
            Protocol.MessageSet.read output.Length (output |> Binary.ofArray)
            |> fst
        | Protocol.CompressionCodec.None ->
            // TODO: logging?
            failwithf "Can't decompress uncompressed message..."
        | Protocol.CompressionCodec.Snappy ->
            // TODO: snappy implementation
            failwithf "Snappy codec not implemented..."
        | codec  ->
            // TODO: logging?
            failwithf "Unknown Codec: %i" codec
