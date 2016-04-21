namespace KafkaFs

open Kafka
open System.IO
open System.IO.Compression

module Compression =
    // The only thing that can be compressed is a MessageSet, not a single Message; this results in a message containing the compressed set
    let gzip messages =
        use outputStream = new MemoryStream()
        use gZipStream = new GZipStream(outputStream, CompressionMode.Compress) 
        do
            let messageSet = messages |> MessageSet.ofMessages
            let inputBytes = messageSet |> MessageSet.size |> Array.zeroCreate
            let segment = ArraySeg.ofArray inputBytes
            MessageSet.write(segment, messageSet) |> ignore
            try
                gZipStream.Write(inputBytes, 0, inputBytes.Length)
                gZipStream.Close() 
            with :? IOException as ex -> 
                // TODO: log this
                printfn "Couldn't write to gzip stream: %A" ex
                reraise()
        Message.create (outputStream.ToArray() |> ArraySeg.ofArray, 
          compression = Protocol.CompressionCodecs.GZIPCompressionCodec)  

    // TODO: is it true that we'll always have the size available here, or do we need to somehow get it out of the MessageSet itself (which may be nontrivial)?
    //       I think in each place that a MessageSet is serialized, the MessageSetSize comes along with it
    let decompress (message:Message) size =
        match message.attributes &&& Protocol.Compression.CompressionMask with
        | Protocol.Compression.GZIP ->
            let inputBytes = message.value |> ArraySeg.toArray
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
            MessageSet.read(output |> ArraySeg.ofArray, size)
            |> fst 
        | Protocol.Compression.None ->
            // TODO: logging?
            failwithf "Can't decompress uncompressed message..."
        | Protocol.Compression.Snappy ->
            // TODO: snappy implementation
            failwithf "Snappy codec not implemented..."
        | codec  -> 
            // TODO: logging?
            failwithf "Unknown Codec: %i" codec 
