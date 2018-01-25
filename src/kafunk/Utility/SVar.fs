[<AutoOpen>]
module internal Kafunk.SVar

open FSharp.Control
open System.Threading

/// A stream variable.
type SVar<'a> = private {
  state : MVar<'a>
  tail : SVarNode<'a> ref }

and SVarNode<'a> = {
  ivar : IVar<('a * SVarNode<'a>)> }

/// Operations on stream variables.
module SVar =
  open Kafunk.MVar.MVar
  
  let private createNode () =
    { ivar = IVar.create () }

  let private putNode (s:SVar<'a>) (a:'a) =
    let newTail = createNode ()
    let tail = Interlocked.Exchange(s.tail, newTail)
    IVar.put (a,newTail) tail.ivar

  /// Creates an empty stream variable.
  let create () : SVar<'a> = 
    { state = MVar.create () ; tail = ref (createNode ()) }

  /// Puts a new value into the stream variable.
  let put (a:'a) (s:SVar<'a>) : Async<unit> = async {
    let! a = MVar.put a s.state
    do putNode s a
    return () }

  /// Atomically updates a value in a stream variable.
  let updateAsync (f:'a -> Async<'a>) (s:SVar<'a>) : Async<'a> =
    s.state
    |> MVar.updateAsync (fun a -> async {
      let! a' = f a
      do putNode s a'
      return a' })

  /// Atomically updates a value in a stream variable.
  let putOrUpdateAsync (f:'a option -> Async<'a>) (s:SVar<'a>) : Async<'a> =
    s.state
    |> MVar.putOrUpdateAsync (fun a -> async {
      let! a' = f a
      do putNode s a'
      return a' })

  /// Atomically updates a value in a stream variable.
  let updateStateAsync (f:'a -> Async<'a * 's>) (s:SVar<'a>) : Async<'s> =
    s.state
    |> MVar.updateStateAsync (fun a -> async {
      let! a',st = f a
      do putNode s a'
      return a',st })

  /// Gets the current value of the stream variable.
  let get (s:SVar<'a>) : Async<'a> =
    MVar.get s.state

  let getFastUnsafe (s:SVar<'a>) : 'a option =
    MVar.getFastUnsafe s.state

  let rec private tapImpl (s:SVarNode<'a>) : AsyncSeq<'a> = 
    asyncSeq {
      let! (a,tl) = IVar.get s.ivar
      yield a
      yield! tapImpl tl }

  /// Returns a stream corresponding to changes in the stream variable from the time of invocation, starting with the 
  /// current value, if any.
  let tap (s:SVar<'a>) : AsyncSeq<'a> =
    let curr = MVar.getFastUnsafe s.state    
    let tail = 
      let tail = !s.tail
      match curr with
      | None -> tail
      | Some a -> { ivar = IVar.createFull (a,tail) }
    tapImpl tail
      
  /// Publishes an error to all waiting readers of a tapped stream.
  let error ex (s:SVar<'a>) =
    let tail = !s.tail
    IVar.error ex tail.ivar

  //let map (f:'a -> Async<'b>) (g:'b -> 'a -> Async<'a>) (s:SVar<'a>) : SVar<'b> =
  //  failwith ""