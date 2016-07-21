namespace Kafunk

open Kafunk
open System.IO
open System.IO.Compression
open Snappy

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Compression =
    let compressMessages messages codec =
        let createMessage value = 
            let attrs = codec |> sbyte |> Some
            Message.create value None attrs

        use outputStream = new MemoryStream()
        use compressionStream = 
            match codec with
            | Protocol.CompressionCodec.GZIP -> new GZipStream(outputStream, CompressionMode.Compress) :> Stream
            | Protocol.CompressionCodec.Snappy -> new SnappyStream(outputStream, CompressionMode.Compress) :> Stream
            | _ -> failwithf "Unknown codec %A" codec

        do
            let messageSet = messages |> MessageSet.ofMessages
            let inputBytes = messageSet |> Protocol.MessageSet.size |> Array.zeroCreate
            let segment = Binary.ofArray inputBytes
            (Protocol.MessageSet.write messageSet segment) |> ignore
            try
                compressionStream.Write(inputBytes, 0, inputBytes.Length)
                compressionStream.Close()
            with :? IOException as ex ->
                // TODO: log this
                printfn "Couldn't write to gzip stream: %A" ex
                reraise()
        createMessage (outputStream.ToArray() |> Binary.ofArray)

    let decompressMessage (message:Protocol.Message) codec=
        let inputBytes = message.value |> Binary.toArray
        use outputStream = new MemoryStream()
        do
            use inputStream = new MemoryStream(inputBytes)
            use compressionStream =  
                match codec with
                | Protocol.CompressionCodec.GZIP -> new GZipStream(inputStream, CompressionMode.Decompress) :> Stream
                | Protocol.CompressionCodec.Snappy -> new SnappyStream(inputStream, CompressionMode.Decompress) :> Stream
                | _ -> failwithf "Unknown codec %A" codec
            try
                compressionStream.CopyTo(outputStream)
                compressionStream.Close()
            with :? IOException as ex ->
                // TODO: log this
                printfn "Couldn't read from gzip stream: %A" ex
                reraise()
        outputStream.Position <- 0L
        let output = outputStream.ToArray()
        // size is output array size divided by message set element size
        Protocol.MessageSet.read output.Length (output |> Binary.ofArray)
        |> fst
        
    let compress compression messages = 
        match compression with
        | Protocol.CompressionCodec.None -> MessageSet.ofMessages messages
        | Protocol.CompressionCodec.GZIP | Protocol.CompressionCodec.Snappy -> MessageSet.ofMessage <| compressMessages messages compression
        | _ -> failwithf "Incorrect compression codec %A" compression

    let decompress (messageSet: Protocol.MessageSet) = 
        match messageSet.messages with
        | [| (_,_,message) |] -> 
            match (message.attributes &&& (sbyte Protocol.CompressionCodec.Mask)) |> byte with
            | Protocol.CompressionCodec.GZIP | Protocol.CompressionCodec.Snappy as codec -> decompressMessage message codec
            | Protocol.CompressionCodec.None ->
                messageSet
            | codec  ->
                failwithf "Unknown Codec: %i" codec
        | _ -> messageSet
