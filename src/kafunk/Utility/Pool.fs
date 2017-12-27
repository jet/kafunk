namespace Kafunk

/// A buffer pool used for scenarios where allocations are scoped to a well known region.
type IBufferPool =
  
  /// Given a desired size, returns a buffer of that size.
  abstract member Alloc : int -> System.ArraySegment<byte>
  
  /// Given a buffer, returns it to the pool.
  abstract member Free : System.ArraySegment<byte> -> unit

/// Operations on buffer pools.
[<Compile(Module)>]
module BufferPool =

  /// A buffer pool managed automatically by the GC.
  let GC = 
    { new IBufferPool with
        member __.Alloc s = Binary.zeros s
        member __.Free _ = () }

#if NET45
  /// A buffer pool using System.ServiceModel.Channels.BufferManager.
  let bufferManager maxPoolSize maxBufferSize =
    let bm = System.ServiceModel.Channels.BufferManager.CreateBufferManager (maxPoolSize,maxBufferSize)
    { new IBufferPool with
        member __.Alloc s = bm.TakeBuffer s |> Binary.ofArray
        member __.Free b = bm.ReturnBuffer b.Array}  
#endif