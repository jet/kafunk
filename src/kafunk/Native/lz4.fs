namespace Kafunk.Native
#nowarn "9"
module Lz4Framing =
    /// For C API details, see:
    /// https://github.com/lz4/lz4/blob/dev/lib/lz4frame.h
    module private native =
        open System
        open System.Runtime.InteropServices

        let LZ4F_VERSION = nativeint 100

        type LZ4F_errorCode_t = uint64

        // Shortcut these enums to int because we do not use them at this time
        type LZ4F_blockSizeID_t = int
        type LZ4F_blockMode_t = int
        type LZ4F_contentChecksum_t = int
        type LZ4F_frameType_t = int
        type LZ4F_blockChecksum_t = int

        [<StructLayout(LayoutKind.Sequential)>]
        type LZ4F_frameInfo_t  =
            struct 
                val blockSizeID: LZ4F_blockSizeID_t
                val blockMode: LZ4F_blockMode_t
                val contentChecksumFlag: LZ4F_contentChecksum_t
                val frameType: LZ4F_frameType_t
                val mutable contentSize: uint64
                val dictID: uint32
                val blockChecksumFlag: LZ4F_blockChecksum_t
            end

        [<StructLayout(LayoutKind.Sequential)>]
        type LZ4F_preferences_t = 
            struct
                val mutable frameInfo: LZ4F_frameInfo_t
                val compressionLevel: int32
                val autoFlush: uint32
                val reserved1: uint32
                val reserved2: uint32
                val reserved3: uint32
                val reserved4: uint32
            end

        [<StructLayout(LayoutKind.Sequential)>]
        type LZ4F_decompressOptions_t = 
            struct
                val mutable stableDst: uint32
                val reserved1: uint32
                val reserved2: uint32
                val reserved3: uint32
            end


        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_compressFrameBound(nativeint _srcSize, IntPtr _preferencesPtr);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_compressFrame(nativeint _dstBuffer, nativeint _dstCapacity,
                                    nativeint _srcBuffer, nativeint _srcSize,
                                    LZ4F_preferences_t& _preferences);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern uint32 LZ4F_isError(nativeint _code);
    
        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_getErrorName(nativeint _code);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_createDecompressionContext(nativeint& _dctxPtr, nativeint _version);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_freeDecompressionContext(nativeint _dctx);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_decompress(nativeint _dctx, nativeint _dstBuffer, nativeint& _dstSize, 
            nativeint _srcBuffer, nativeint& _srcSizePtr, nativeint _optionsPtr);

        [<DllImport("liblz4.so", CallingConvention=CallingConvention.Cdecl)>]
        extern nativeint LZ4F_getFrameInfo(nativeint _dctx, LZ4F_frameInfo_t& _frameInfoPtr, nativeint _srcBuffer, nativeint& _srcSizePtr);

    open System
    open FSharp.NativeInterop
    open native

    let private ensureNativeIsLoaded = Loader.load "liblz4.so"

    //
    // liblz4 error reporting
    //
    let private isError code =
        LZ4F_isError(code) <> 0u

    let private getErrorName code =
        let stringAddr = LZ4F_getErrorName(code)
        System.Runtime.InteropServices.Marshal.PtrToStringAnsi(stringAddr)


    let failIfError funcName code =
        if isError code then
            let error = getErrorName code
            failwithf "LZ4 native call '%s' failed: '%s'" funcName error
        else
            code

    //
    // Public API
    //
    let compressFrameBound (srcSize: int) : int = 
        ensureNativeIsLoaded.Value
        LZ4F_compressFrameBound((nativeint srcSize), IntPtr.Zero)
        |> int

    let compressFrame (src: ArraySegment<byte>) (dst: byte[]) = 
        ensureNativeIsLoaded.Value

        let mutable compressParams = LZ4F_preferences_t()
        compressParams.frameInfo.contentSize <- (uint64 src.Count)

        use srcPtr = fixed src.Array
        use dstPtr = fixed dst

        let res = 
            LZ4F_compressFrame(
                dstPtr |> NativePtr.toNativeInt, 
                (nativeint dst.Length), 
                NativePtr.add srcPtr src.Offset |> NativePtr.toNativeInt, (nativeint src.Count), 
                &compressParams
            )

        if isError res then
            let error = getErrorName res
            failwithf "LZ4 native call LZ4F_compressFrame failed: '%s'" error
        else 
            new ArraySegment<byte>(dst, 0, (int res))

    let decompress (src: ArraySegment<byte>): byte[] = 
        ensureNativeIsLoaded.Value

        let mutable ctx = IntPtr.Zero
        do LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION) |> failIfError "LZ4F_createDecompressionContext" |> ignore
        try
            // read frame info to get uncompressed size
            let mutable frameInfo = LZ4F_frameInfo_t()
            let mutable srcSize = nativeint src.Count
            use srcPtr = fixed src.Array
            let srcAddr = NativePtr.add srcPtr src.Offset |> NativePtr.toNativeInt
            
            do LZ4F_getFrameInfo(ctx, &frameInfo, srcAddr, &srcSize) |> failIfError "LZ4F_getFrameInfo" |> ignore

            let decompressedSize = frameInfo.contentSize
            if decompressedSize = 0UL then
                Array.empty
            else           
                // LZ4F_getFrameInfo have updated srcSize to consumed bytes
                let dataAddr = srcAddr + srcSize 
                srcSize <- (nativeint src.Count) - srcSize

                let decompressed = Array.zeroCreate<byte> (int decompressedSize)
                use decompressedPtr = fixed decompressed
                let decompressedAddr = decompressedPtr |> NativePtr.toNativeInt
                let mutable dstSize = nativeint decompressed.Length

                let before = sprintf "dstSize: %d; srcSize: %d" dstSize srcSize
                let res = LZ4F_decompress(ctx, decompressedAddr, &dstSize, dataAddr, &srcSize, IntPtr.Zero) |> failIfError "LZ4F_decompress"
                let after = sprintf "dstSize: %d; srcSize: %d" dstSize srcSize
                if res <> nativeint 0 then
                    failwithf "Expected LZ4F_decompress to return 0 but got %d. Buffer too small?\n  %s\n  %s" res before after
                else
                    decompressed
        finally
            // protect context from leaking
            do LZ4F_freeDecompressionContext(ctx) |> failIfError "LZ4F_freeDecompressionContext" |> ignore

    