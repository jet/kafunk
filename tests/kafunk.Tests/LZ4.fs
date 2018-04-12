namespace Kafunk.Tests

open NUnit.Framework
open Kafunk.Native
open System
open System.Text

module LZ4 =
    open System.Numerics

    [<Test>]
    let ``compress produce result smaller than original`` () =
        let text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit
            . Sed malesuada consectetur augue, vitae euismod dui imperdiet in. Nunc arcu felis, 
            luctus eleifend ipsum ac, iaculis commodo enim. Proin aliquet odio nisi, et efficitur 
            enim vehicula in. Nam vitae vestibulum risus. Donec egestas dapibus urna. Proin sit amet 
            tincidunt dui, eget condimentum erat. Morbi blandit enim non massa laoreet ultricies. 
            Vivamus consequat pharetra felis, sit amet feugiat metus porta iaculis."
        
        let data = text |> Encoding.UTF8.GetBytes |> ArraySegment
        
        
        let compressedBound = Lz4Framing.compressFrameBound data.Count
        let compressedBuffer = Array.zeroCreate compressedBound
        let compressed = Lz4Framing.compressFrame data compressedBuffer
        
        let decompressed = 
            compressed
            |> Lz4Framing.decompress 
            |> Encoding.UTF8.GetString

        // Compression is correct
        Assert.AreEqual(text, decompressed)

        // Compression produce result smaller than the original
        Assert.Less(compressed.Count, data.Count)

    [<Test>]
    let ``Array of any size compress correctly`` () =
        [0; 1; 63; 64; 65; 1000000]
        |> Seq.iter( fun size ->
            let data = Array.zeroCreate size
            (new Random()).NextBytes data

            let compressedBound = Lz4Framing.compressFrameBound data.Length
            let compressedBuffer = Array.zeroCreate compressedBound
            let compressed = Lz4Framing.compressFrame (new ArraySegment<byte>(data)) compressedBuffer
        
            let decompressed = 
                compressed
                |> Lz4Framing.decompress 

            Assert.AreEqual(data.Length, decompressed.Length)
            Assert.AreEqual(data, decompressed)
        )

