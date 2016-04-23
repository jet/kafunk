namespace KafkaFs

open System

[<AutoOpen>]
module Shared =

  /// Determines whether the argument is a null reference.
  let inline isNull a = obj.ReferenceEquals(null, a)

  /// Given a value, creates a function with one ignored argument which returns the value.
  let inline konst x _ = x

  /// Active pattern for matching a Choice<'a, 'b> with Choice1Of2 = Success and Choice2Of2 = Failure
  let (|Success|Failure|) = function
      | Choice1Of2 a -> Success a
      | Choice2Of2 b -> Failure b



/// CRC-32.
module Crc =

  let private crc_table = [|
    0x00000000u; 0x77073096u; 0xEE0E612Cu; 0x990951BAu; 0x076DC419u; 0x706AF48Fu
    0xE963A535u; 0x9E6495A3u; 0x0EDB8832u; 0x79DCB8A4u; 0xE0D5E91Eu; 0x97D2D988u
    0x09B64C2Bu; 0x7EB17CBDu; 0xE7B82D07u; 0x90BF1D91u; 0x1DB71064u; 0x6AB020F2u
    0xF3B97148u; 0x84BE41DEu; 0x1ADAD47Du; 0x6DDDE4EBu; 0xF4D4B551u; 0x83D385C7u
    0x136C9856u; 0x646BA8C0u; 0xFD62F97Au; 0x8A65C9ECu; 0x14015C4Fu; 0x63066CD9u
    0xFA0F3D63u; 0x8D080DF5u; 0x3B6E20C8u; 0x4C69105Eu; 0xD56041E4u; 0xA2677172u
    0x3C03E4D1u; 0x4B04D447u; 0xD20D85FDu; 0xA50AB56Bu; 0x35B5A8FAu; 0x42B2986Cu
    0xDBBBC9D6u; 0xACBCF940u; 0x32D86CE3u; 0x45DF5C75u; 0xDCD60DCFu; 0xABD13D59u
    0x26D930ACu; 0x51DE003Au; 0xC8D75180u; 0xBFD06116u; 0x21B4F4B5u; 0x56B3C423u
    0xCFBA9599u; 0xB8BDA50Fu; 0x2802B89Eu; 0x5F058808u; 0xC60CD9B2u; 0xB10BE924u
    0x2F6F7C87u; 0x58684C11u; 0xC1611DABu; 0xB6662D3Du; 0x76DC4190u; 0x01DB7106u
    0x98D220BCu; 0xEFD5102Au; 0x71B18589u; 0x06B6B51Fu; 0x9FBFE4A5u; 0xE8B8D433u
    0x7807C9A2u; 0x0F00F934u; 0x9609A88Eu; 0xE10E9818u; 0x7F6A0DBBu; 0x086D3D2Du
    0x91646C97u; 0xE6635C01u; 0x6B6B51F4u; 0x1C6C6162u; 0x856530D8u; 0xF262004Eu
    0x6C0695EDu; 0x1B01A57Bu; 0x8208F4C1u; 0xF50FC457u; 0x65B0D9C6u; 0x12B7E950u
    0x8BBEB8EAu; 0xFCB9887Cu; 0x62DD1DDFu; 0x15DA2D49u; 0x8CD37CF3u; 0xFBD44C65u
    0x4DB26158u; 0x3AB551CEu; 0xA3BC0074u; 0xD4BB30E2u; 0x4ADFA541u; 0x3DD895D7u
    0xA4D1C46Du; 0xD3D6F4FBu; 0x4369E96Au; 0x346ED9FCu; 0xAD678846u; 0xDA60B8D0u
    0x44042D73u; 0x33031DE5u; 0xAA0A4C5Fu; 0xDD0D7CC9u; 0x5005713Cu; 0x270241AAu
    0xBE0B1010u; 0xC90C2086u; 0x5768B525u; 0x206F85B3u; 0xB966D409u; 0xCE61E49Fu
    0x5EDEF90Eu; 0x29D9C998u; 0xB0D09822u; 0xC7D7A8B4u; 0x59B33D17u; 0x2EB40D81u
    0xB7BD5C3Bu; 0xC0BA6CADu; 0xEDB88320u; 0x9ABFB3B6u; 0x03B6E20Cu; 0x74B1D29Au
    0xEAD54739u; 0x9DD277AFu; 0x04DB2615u; 0x73DC1683u; 0xE3630B12u; 0x94643B84u
    0x0D6D6A3Eu; 0x7A6A5AA8u; 0xE40ECF0Bu; 0x9309FF9Du; 0x0A00AE27u; 0x7D079EB1u
    0xF00F9344u; 0x8708A3D2u; 0x1E01F268u; 0x6906C2FEu; 0xF762575Du; 0x806567CBu
    0x196C3671u; 0x6E6B06E7u; 0xFED41B76u; 0x89D32BE0u; 0x10DA7A5Au; 0x67DD4ACCu
    0xF9B9DF6Fu; 0x8EBEEFF9u; 0x17B7BE43u; 0x60B08ED5u; 0xD6D6A3E8u; 0xA1D1937Eu
    0x38D8C2C4u; 0x4FDFF252u; 0xD1BB67F1u; 0xA6BC5767u; 0x3FB506DDu; 0x48B2364Bu
    0xD80D2BDAu; 0xAF0A1B4Cu; 0x36034AF6u; 0x41047A60u; 0xDF60EFC3u; 0xA867DF55u
    0x316E8EEFu; 0x4669BE79u; 0xCB61B38Cu; 0xBC66831Au; 0x256FD2A0u; 0x5268E236u
    0xCC0C7795u; 0xBB0B4703u; 0x220216B9u; 0x5505262Fu; 0xC5BA3BBEu; 0xB2BD0B28u
    0x2BB45A92u; 0x5CB36A04u; 0xC2D7FFA7u; 0xB5D0CF31u; 0x2CD99E8Bu; 0x5BDEAE1Du
    0x9B64C2B0u; 0xEC63F226u; 0x756AA39Cu; 0x026D930Au; 0x9C0906A9u; 0xEB0E363Fu
    0x72076785u; 0x05005713u; 0x95BF4A82u; 0xE2B87A14u; 0x7BB12BAEu; 0x0CB61B38u
    0x92D28E9Bu; 0xE5D5BE0Du; 0x7CDCEFB7u; 0x0BDBDF21u; 0x86D3D2D4u; 0xF1D4E242u
    0x68DDB3F8u; 0x1FDA836Eu; 0x81BE16CDu; 0xF6B9265Bu; 0x6FB077E1u; 0x18B74777u
    0x88085AE6u; 0xFF0F6A70u; 0x66063BCAu; 0x11010B5Cu; 0x8F659EFFu; 0xF862AE69u
    0x616BFFD3u; 0x166CCF45u; 0xA00AE278u; 0xD70DD2EEu; 0x4E048354u; 0x3903B3C2u
    0xA7672661u; 0xD06016F7u; 0x4969474Du; 0x3E6E77DBu; 0xAED16A4Au; 0xD9D65ADCu
    0x40DF0B66u; 0x37D83BF0u; 0xA9BCAE53u; 0xDEBB9EC5u; 0x47B2CF7Fu; 0x30B5FFE9u
    0xBDBDF21Cu; 0xCABAC28Au; 0x53B39330u; 0x24B4A3A6u; 0xBAD03605u; 0xCDD70693u
    0x54DE5729u; 0x23D967BFu; 0xB3667A2Eu; 0xC4614AB8u; 0x5D681B02u; 0x2A6F2B94u
    0xB40BBE37u; 0xC30C8EA1u; 0x5A05DF1Bu; 0x2D02EF8Du
    |]  
    
  let crc32 (buf:byte[], offset:int, length:int) =
    let mutable c = (0u ^^^ 0xffffffffu)
    for i = 0 to length - 1 do
      c <- crc_table.[int ((c ^^^ uint32 buf.[i + offset]) &&& 0xffu)] ^^^ (c >>> 8)
    c ^^^ 0xffffffffu







// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// errors


module Choice =
  
  let fold (f:'a -> 'c) (g:'b -> 'c) (c:Choice<'a, 'b>) : 'c =
    match c with
    | Choice1Of2 a -> f a
    | Choice2Of2 b -> g b

  let mapLeft (f:'a -> 'c) (c:Choice<'a, 'b>) : Choice<'c, 'b> =
    match c with
    | Choice1Of2 a -> Choice1Of2 (f a)
    | Choice2Of2 b -> Choice2Of2 b

  let inline mapSuccess f c = mapLeft f c

  let mapRight (f:'b -> 'c) (c:Choice<'a, 'b>) : Choice<'a, 'c> =
    match c with
    | Choice1Of2 a -> Choice1Of2 a
    | Choice2Of2 b -> Choice2Of2 (f b)  

  let inline mapError f c = mapRight f c

  let flattenLeft (c:Choice<Choice<'a, 'e>, 'e>) : Choice<'a, 'e> =
    match c with
    | Choice1Of2 (Choice1Of2 a) -> Choice1Of2 a
    | Choice1Of2 (Choice2Of2 e) | Choice2Of2 e -> Choice2Of2 e


// -----------------------------------------------------------------------------------------------------------------------------------------------------------





// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// array segments

/// Delimits a section of a one-dimensional array.
type ArraySeg<'a> = ArraySegment<'a>

/// Operations on array segments.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module ArraySeg =

  [<GeneralizableValue>]
  let empty<'a> = ArraySeg<_>()

  let inline ofCount (count:int) = ArraySeg<_>(Array.zeroCreate count)

  let inline setOffsetCount (offset:int) (count:int) (a:ArraySeg<'a>) =
    ArraySeg<_>(a.Array, offset, count)

  let inline setCount (count:int) (a:ArraySeg<'a>) =
    ArraySeg<_>(a.Array, a.Offset, count)

  /// Set the offset of the segment.
  let inline setOffset (offset:int) (a:ArraySeg<'a>) =
    ArraySeg<_>(a.Array, offset, a.Count - (offset - a.Offset))

  /// Shifts the offset to the right by the specified amount.
  let inline shiftOffset (d:int) (a:ArraySeg<'a>) : ArraySeg<'a> =
    setOffset (a.Offset + d) a

  let inline ofArray (bytes:'a[]) = ArraySeg<_>(bytes, 0, bytes.Length)

  let inline toArray (s:ArraySeg<'a>) : 'a[] =
    let arr = Array.zeroCreate s.Count
    Buffer.BlockCopy(s.Array, s.Offset, arr, 0, s.Count)
    arr

  let append (a:ArraySeg<'a>) (b:ArraySeg<'a>) : ArraySeg<'a> =
    if a.Count = 0 then b
    elif b.Count = 0 then a
    else
      let arr = Array.zeroCreate (a.Count + b.Count)
      Buffer.BlockCopy(a.Array, a.Offset, arr, 0, a.Count)
      Buffer.BlockCopy(b.Array, b.Offset, arr, a.Count, b.Count)
      ofArray arr

  /// Partitions an array segment at the specified index.
  let partitionAt i (a:ArraySegment<'a>) : ArraySegment<'a> * ArraySegment<'a> =
    let first = ArraySeg<_>(a.Array, a.Offset, a.Offset + i)
    let rest = ArraySeg<_>(a.Array, (a.Offset + i), (a.Count - i))
    first,rest

  let inline copy (src:ArraySeg<'a>) (dest:ArraySeg<'a>) (count:int) =
    Buffer.BlockCopy (src.Array, src.Offset, dest.Array, dest.Offset, count)


[<AutoOpen>]
module ArraySegEx =

  open System.Text
  open System.IO

  type Encoding with
    member x.GetString(data:ArraySegment<byte>) = x.GetString(data.Array, data.Offset, data.Count)

// -----------------------------------------------------------------------------------------------------------------------------------------------------------




[<AutoOpen>]
module BitConverterEx =

  type BitConverter with
    
    static member ToInt16BigEndian (buf:byte[], offset:int) =
      (int16 buf.[offset + 0] <<< 8) ||| (int16 buf.[offset + 1])
    static member GetBytesBigEndian (x:int16, buf:byte[], offset:int) =
      buf.[offset] <- byte (x >>> 8)
      buf.[offset + 1] <- byte x
    static member GetBytesBigEndian (x:int16) =
      let buf = Array.zeroCreate 2
      BitConverter.GetBytesBigEndian (x,buf,0)
      buf

    static member ToInt32BigEndian (buf:byte[], offset:int) =
          (int buf.[offset + 0] <<< 24) 
      ||| (int buf.[offset + 1] <<< 16) 
      ||| (int buf.[offset + 2] <<< 8) 
      ||| (int buf.[offset + 3] <<< 0)
    static member GetBytesBigEndian (x:int, buf:byte[], offset:int) =
      buf.[offset + 0] <- byte (x >>> 24)
      buf.[offset + 1] <- byte (x >>> 16)
      buf.[offset + 2] <- byte (x >>> 8)
      buf.[offset + 3] <- byte x
    static member GetBytesBigEndian (x:int) =
      let buf = Array.zeroCreate 4
      BitConverter.GetBytesBigEndian (x,buf,0)
      buf

    static member ToInt64BigEndian (buf:byte[], offset:int) =
          (int64 buf.[offset + 0] <<< 56) 
      ||| (int64 buf.[offset + 1] <<< 48) 
      ||| (int64 buf.[offset + 2] <<< 40) 
      ||| (int64 buf.[offset + 3] <<< 32) 
      ||| (int64 buf.[offset + 4] <<< 24) 
      ||| (int64 buf.[offset + 5] <<< 16) 
      ||| (int64 buf.[offset + 6] <<< 8) 
      ||| (int64 buf.[offset + 7])
    static member GetBytesBigEndian (x:int64, buf:byte[], offset:int) =
      buf.[offset + 0] <- byte (x >>> 56)
      buf.[offset + 1] <- byte (x >>> 48)
      buf.[offset + 2] <- byte (x >>> 40)
      buf.[offset + 3] <- byte (x >>> 32)
      buf.[offset + 4] <- byte (x >>> 24)
      buf.[offset + 5] <- byte (x >>> 16)
      buf.[offset + 6] <- byte (x >>> 8)
      buf.[offset + 7] <- byte x
    static member GetBytesBigEndian (x:int64) =
      let buf = Array.zeroCreate 8
      BitConverter.GetBytesBigEndian (x,buf,0)
      buf

    static member GetBytesBigEndian (x:uint32, buf:byte[], offset:int) =
      buf.[offset] <- byte (x >>> 24)
      buf.[offset + 1] <- byte (x >>> 16)
      buf.[offset + 2] <- byte (x >>> 8)
      buf.[offset + 3] <- byte x
    static member GetBytesBigEndian (x:uint32) =
      let buf = Array.zeroCreate 4
      BitConverter.GetBytesBigEndian (x,buf,0)
      buf




// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// pooling

open System.Collections.Concurrent

type ObjectPool<'a>(initial:int, create:unit -> 'a) =

  let pool = new ConcurrentStack<'a>(Seq.init initial (fun _ -> create()))

  member x.Push(a:'a) =
    pool.Push(a)

  member x.Pop() =
    let mutable a = Unchecked.defaultof<'a>
    if not (pool.TryPop(&a)) then
      failwith "out of sockets!"
      //create ()
    else a

// -----------------------------------------------------------------------------------------------------------------------------------------------------------




// -----------------------------------------------------------------------------------------------------------------------------------------------------------
// log

// TODO: implement!

type LogLevel = 
  | Trace | Info | Warn | Error | Fatal
  with 
    override x.ToString () =
      match x with 
      | Trace -> "TRACE" | Info -> "INFO" | Warn -> "WARN" | Error -> "ERROR" | Fatal -> "FATAL"

type Logger = {
  name : string
}


[<AutoOpen>]
module LoggerEx =

  type Logger with

    member inline ts.log (format, level:LogLevel) =
      let inline trace (m:string) = 
        Console.WriteLine(String.Format("{0:yyyy-MM-dd hh:mm:ss:ffff}|{1}|{2}|{3}", DateTime.Now, (level.ToString()), ts.name, m))
      Printf.kprintf trace format
        
    member inline ts.fatal format = ts.log (format, LogLevel.Fatal)

    member inline ts.error format = ts.log (format, LogLevel.Error)
   
    member inline ts.warn format = ts.log (format, LogLevel.Warn)

    member inline ts.info format = ts.log (format, LogLevel.Info)

    member inline ts.trace format = ts.log (format, LogLevel.Trace)



[<RequireQualifiedAccess>]
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Log =
  
  let create name = { name = name }



// -----------------------------------------------------------------------------------------------------------------------------------------------------------




// ------------------------------------------------------------------------------------------------------------------
// collection helpers


open System.Collections.Generic

/// Basic operations on dictionaries.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Dict =

  let tryGet k (d:#IDictionary<_,_>) =
    let mutable v = Unchecked.defaultof<_>
    if d.TryGetValue(k, &v) then Some v
    else None


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Array =
  
  let inline item i a = Array.get a i
  let inline singleton a = [|a|]


[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Seq =

  /// Creates a map from a sequnce based on a key and value selection function.
  let toKeyValueMap (key:'a -> 'Key) (value:'a -> 'Value) (s:seq<'a>) : Map<'Key, 'Value> =
      s
      |> Seq.map (fun a -> key a,value a)
      |> Map.ofSeq

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Map =
  
  let addMany (kvps:('a * 'b) seq) (m:Map<'a, 'b>) : Map<'a, 'b> =
    kvps |> Seq.fold (fun m (k,v) -> Map.add k v m) m
  



// ------------------------------------------------------------------------------------------------------------------
// dependent/dynamic references


/// A dependant variable.
type DVar<'a> = private { mutable cell : 'a ; event : Event<'a> }

/// Operations on dependant variables.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DVar = 

  open System.Threading

  /// Creates a DVar and initializes it with the specified value.
  let create (a:'a) : DVar<'a> =
    { cell = a ; event = new Event<'a>() }

  /// Gets the current value of a DVar.
  let get (d:DVar<'a>) : 'a =
    d.cell

  /// Puts a value into a DVar, triggering derived DVars to update.
  let put (a:'a) (d:DVar<'a>) : unit =    
    Interlocked.Exchange (&d.cell, a) |> ignore
    d.event.Trigger a

  /// DVar.put with arguments reversed.
  let inline set d a = 
    put a d

  /// Triggers a change in the DVar using its current value.
  let ping (d:DVar<'a>) =
    put (get d) d

  /// Publishes DVar changes as an event.
  let changes (d:DVar<'a>) : IEvent<'a> =
    d.event.Publish

  /// Subscribes a callback to changes to the DVar.
  let subs (d:DVar<'a>) (f:'a -> unit)  : unit =
    d |> changes |> Event.add f

  /// Invokes the callback with the current value of the DVar
  /// as well as all subsequent values.
  let iter (f:'a -> unit) (d:DVar<'a>) =
    f (get d)
    subs d f

  /// Creates DVar derived from the argument DVar through a pure mapping function.
  /// When the argument changes, the dependant variable changes.
  let map (f:'a -> 'b) (v:DVar<'a>) : DVar<'b> =
    let dv = create (f (get v))
    subs v (f >> set dv)
    dv

  /// Creates a DVar based on two argument DVars, one storing a function value
  /// and the other a value to apply the function to. The resulting DVar contains 
  /// the resulting value and changes based on the two DVars.
  let ap (f:DVar<'a -> 'b>) (v:DVar<'a>) : DVar<'b> =
    let b = (get f) (get v)
    let db = create b
    subs f <| fun f -> let b = f (get v) in put b db
    subs v <| fun a -> let f = get f in let b = f a in put b db
    db

  /// Combine values of two DVars using the specified function.
  let combineLatestWith (f:'a -> 'b -> 'c) (a:DVar<'a>) (b:DVar<'b>) : DVar<'c> =
    ap (ap (create f) a) b

  /// Combining two DVars into a single DVar containg tuples.
  let combineLatest (a:DVar<'a>) (b:DVar<'b>) : DVar<'a * 'b> =
    combineLatestWith (fun a b -> a,b) a b 
  
  /// Represents a DVar containing a function value as a function value which selects
  /// the implementation from the DVar on each invocation.
  let toFun (v:DVar<'a -> 'b>) : 'a -> 'b =
    fun a -> (get v) a

  /// Returns a function which reconfigures in response to changes in the DVar.
  let mapFun (f:'c -> ('a -> 'b)) (d:DVar<'c>) : 'a -> 'b =
    map f d |> toFun

  /// Creates a DVar given an initial value and binds it to an event.
  let ofEvent (initial:'a) (e:IEvent<'a>) : DVar<'a> =
    let dv = create initial
    e |> Event.add (fun a -> put a dv)
    dv

  /// Creates a DVar given an initial value and binds it to an observable.
  let ofObservable (initial:'a) (e:IObservable<'a>) : DVar<'a> =
    let dv = create initial
    e |> Observable.add (fun a -> put a dv)
    dv

  /// Binds a DVar to a ref such that its current value is assigned to the ref
  /// and the ref is bound to all subsequent updates of the DVar.
  let bindToRef (r:'a ref) (a:DVar<'a>) =
    r := (get a)
    subs a <| fun a -> r := a

  
  let update (f:'a -> 'a) (d:DVar<'a>) : unit =
    put (f (get d)) d

  let updateIf (f:'a -> 'a) (d:DVar<'a>) : unit =
    put (f (get d)) d





  let extract = get

  /// Creates a DVar<'b> using the value extracted from the argument DVar<'a>.
  /// Then, subscribes to the argument DVar and for every change, applies the same
  /// function and updates the resulting DVar.
  /// Each invocation of the function receives a reference to the argument DVar<'a>
  /// as it is at the time of change. As such, the function may subscribe the remainder
  /// of the change stream.
  /// 
  /// Example: Suppose a DVar is a configuration value and you wish to create a function
  ///   which depends on this configuration value. This constructor can be modeled as a function
  ///   DVar<'a> -> ('i -> 'o). DVar.extend applies this function to a DVar (e.g. from a configuration source)
  ///   and returns a DVar<'i -> 'o> which contains the desired function. We therefore get a 
  ///   transformation DVar<'a> -> DVar<'i -> 'o>. The contructor will usually bind certain dependencies
  ///   to the DVar. What the laws require is that each time this constructor is called:
  ///     - the argument DVar will contain the current value of the DVar at change time 
  ///     - it will publish the remainder of the changes to the DVar.
  ///
  /// The "context" in this instance of a Comonad is the state of the DVar immediately after
  /// the change which caused invocation.

  /// Law 1: extend extract = id
  ///   - Interpretation: extracting is revered by extending. If you simply extract the value from every DVar and 
  ///     return it, you get back the argument DVar.
  /// Law 2: extract << extend f = f : (DVar<'a> -> 'a)
  ///   - Interpretation: extending is reversed by extracting. In other words, this operation
  ///     is such that applying the function to an argument DVar and extracting a value from the resulting
  ///     DVar returns the same value as simply applying the function to the argument DVar. This essentially
  ///     ensures that the resulting DVar<'a> is initialized with the result of applying the function to
  ///     the current value of the arguement DVar. 
  /// Law 3: extend f << extend g = extend (f << extend g) where g:DVar<'a> -> 'b ; f:DVar<'a> -> 'b
  ///   - Interpretation: extension is associative with respect to function composition.
  ///     In other words, extending a DVar with a function which applies an argument function f
  ///     to the result of extending its argument DVar with a function g, is the same as extending
  ///     a DVar. This essentially means that when invoked from within a DVar context, one does not
  ///     get any special composition privileges.
  let extend (f:DVar<'a> -> 'b) (da:DVar<'a>) : DVar<'b> =
    let b = f da
    let db = create b
    subs da (fun _ -> let b = f da in put b db)
    db

    
  let choice (cd:Choice<DVar<'a>, DVar<'b>>) : DVar<Choice<'a, 'b>> =
    match cd with
    | Choice1Of2 da -> da |> map Choice1Of2
    | Choice2Of2 db -> db |> map Choice2Of2


  //let periodic (periodMs:int) (f:unit -> 'a) : DVar<'a> =
  
  let observe (d:DVar<'a>) : 'a * IEvent<'a> =
    (get d),(changes d)

  /// Left biased choose.
  let choose (first:DVar<'a>) (second:DVar<'a>) : DVar<'a> =
    let da = create (get first)
    Event.merge (changes first) (changes second) |> Event.add (set da)
    da




