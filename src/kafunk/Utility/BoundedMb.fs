[<AutoOpen>]
module internal Kafunk.BoundedMb

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Collections.Generic

type BoundedMbReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Take of AsyncReplyChannel<'a>

[<Compile(Module)>]
module BoundedMbCond =

  /// Creates a condition based on a value the type of which forms a group (e,+,-).
  let group (G:Group<'g>) (f:'a -> 'g) (bound:'g -> bool) =
    let mutable value = Group.zero G
    { new IBoundedMbCond<'a> with
        member __.Add (a:'a) = value <- Group.add G value (f a)
        member __.Remove (a:'a) = value <- Group.subtract G value (f a)
        member __.Satisfied = bound value
        member __.Reset () = value <- Group.zero G }

  /// Creates a condition based on count.
  let count (capacity:int) = 
    group Group.intAdd (fun _ -> 1) (fun c -> c >= capacity)

/// A mailbox bounded by a condition.
type BoundedMb<'a> internal (cond:IBoundedMbCond<'a>) =

  let agent = MailboxProcessor.Start <| fun agent ->

    let queue = Queue<_>()

    let rec loop () = async {
      match queue.Count with
      | 0 -> do! tryReceive ()
      | _ ->
        if cond.Satisfied then
          do! trySend ()
        else
          do! trySendOrReceive ()
      return! loop () }

    and tryReceive () = 
      agent.Scan (function
        | Put (a,rep) -> Some (receive(a,rep))
        | _ -> None)

    and receive (a:'a, rep:AsyncReplyChannel<unit>) = async {
      queue.Enqueue a
      cond.Add a
      rep.Reply () }

    and trySend () = 
      agent.Scan (function
        | Take rep -> Some (send rep)
        | _ -> None)

    and send (rep:AsyncReplyChannel<'a>) = async {
      let a = queue.Dequeue ()
      cond.Remove a
      rep.Reply a }

    and trySendOrReceive () = async {
      let! msg = agent.Receive ()
      match msg with
      | Put (a,rep) -> return! receive (a,rep)
      | Take rep -> return! send rep }

    loop ()

  member __.Put (a:'a) =
    agent.PostAndAsyncReply (fun ch -> Put (a,ch))

  member __.Take () =
    agent.PostAndAsyncReply (fun ch -> Take ch)

  interface IDisposable with
    member __.Dispose () = (agent :> IDisposable).Dispose()

/// Operations on bounded mailboxes.
module BoundedMb =

  /// Creates a bounded mailbox with the specified capacity.
  let create (capacity:int) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(BoundedMbCond.count capacity)
    mq

  /// Creates a bounded mailbox bounded by the specified condition.
  /// If the condition return true, the mailbox blocks puts, otherwise accepts them.
  let createByCondition (condition:IBoundedMbCond<'a>) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(condition)
    mq

  /// Puts a message into the mailbox.
  /// If the mailbox is full, wait until it has room available.
  /// If the mailbox has vacancy, add the message to the buffer and return immediately.
  let inline put (a:'a) (mb:BoundedMb<'a>) : Async<unit> =
    mb.Put a

  /// Takes a message from
  let inline take (mb:BoundedMb<'a>) : Async<'a> =
    async.Delay mb.Take


//type BoundedMb2<'a> internal (cond:IBoundedMbCond<'a>) =
//  
//  let queue = new Queue<'a>()
//  let canTake = new ManualResetEventSlim(false)
//  let canPut = new ManualResetEventSlim(true)
//
//  let tryPut a =
//    lock cond (fun () ->
//      if not cond.Satisfied then
//        queue.Enqueue a
//        cond.Add a
//        canTake.Set () |> ignore
//        true
//      else
//        canPut.Reset () |> ignore
//        false)
//
//  let tryTake () =
//    lock cond (fun () ->
//      if queue.Count > 0 then
//        let a = queue.Dequeue ()
//        cond.Remove a
//        if queue.Count = 0 then
//          canTake.Reset () |> ignore
//        if not (cond.Satisfied) then
//          canPut.Set () |> ignore
//        Some a
//      else
//        None)
//
//  member __.Put (a:'a) = async {
//    if tryPut a then
//      return ()
//    else
//      let! ct = Async.CancellationToken
//      let rec loop () = async {
//        //let! _ = Async.AwaitWaitHandle canPut
//        canPut.Wait ct
//        if tryPut a then
//          return ()
//        else
//          return! loop () }
//      return! loop () }
//
//  member __.Take () = async {
//    match tryTake () with
//    | Some a -> return a
//    | None ->
//      let! ct = Async.CancellationToken
//      let rec loop () = async {
//        //let! _ = Async.AwaitWaitHandle canTake
//        canTake.Wait ct
//        match tryTake () with
//        | Some a -> return a 
//        | None -> return! loop () }
//      return! loop () }
//
///// Operations on bounded mailboxes.
//module BoundedMb2 =
//
//  /// Creates a bounded mailbox bounded by the specified condition.
//  /// If the condition return true, the mailbox blocks puts, otherwise accepts them.
//  let createByCondition (condition:IBoundedMbCond<'a>) : BoundedMb2<'a> =
//    let mq = new BoundedMb2<'a>(condition)
//    mq
//
//  /// Puts a message into the mailbox.
//  /// If the mailbox is full, wait until it has room available.
//  /// If the mailbox has vacancy, add the message to the buffer and return immediately.
//  let inline put (a:'a) (mb:BoundedMb2<'a>) : Async<unit> =
//    mb.Put a
//
//  /// Takes a message from
//  let inline take (mb:BoundedMb2<'a>) : Async<'a> =
//    async.Delay mb.Take
  

