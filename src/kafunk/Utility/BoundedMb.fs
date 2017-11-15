[<AutoOpen>]
module internal Kafunk.BoundedMb

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Collections.Generic

type BoundedMbReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Take of AsyncReplyChannel<'a>
  | GetAll of AsyncReplyChannel<'a[]>

[<Compile(Module)>]
module BoundedMbCond =

  /// Creates a condition based on a value the type of which forms a group (e,+,-).
  let group (G:Group<'g>) (f:'a -> 'g) (bound:'g -> bool) =
    let mutable value = Group.zero G
    { new IBoundedMbCond<'a> with
        member __.Add (a:'a) =  value <- Group.add G value (f a)
        member __.Remove (a:'a) = value <- Group.subtract G value (f a)
        member __.Satisfied = bound value
        member __.Reset () = value <- Group.zero G
        member __.WillSatisfy (a:'a) = Group.add G value (f a) |> bound }

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
        | GetAll rep -> Some (getAll rep)
        | _ -> None)

    and receive (a:'a, rep:AsyncReplyChannel<unit>) = async {
      queue.Enqueue a
      cond.Add a
      rep.Reply () }

    and trySend () = 
      agent.Scan (function
        | Take rep -> Some (send rep)
        | GetAll rep -> Some (getAll rep)
        | _ -> None)

    and send (rep:AsyncReplyChannel<'a>) = async {
      let a = queue.Dequeue ()
      cond.Remove a
      rep.Reply a }

    and getAll (rep:AsyncReplyChannel<'a[]>) = async {
      let xs = queue.ToArray ()
      for x in xs do
        cond.Remove x
      rep.Reply xs }

    and trySendOrReceive () = async {
      let! msg = agent.Receive ()
      match msg with
      | GetAll rep -> return! getAll rep
      | Put (a,rep) -> return! receive (a,rep)
      | Take rep -> return! send rep }

    loop ()

  member __.Put (a:'a) =
    agent.PostAndAsyncReply (fun ch -> Put (a,ch))

  member __.Take () =
    agent.PostAndAsyncReply (fun ch -> Take ch)

  member __.GetAll () =
    agent.PostAndAsyncReply (fun ch -> GetAll ch)

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

  /// Takes a message from the mailbox.
  /// If mailbox is empty, waits.
  let inline take (mb:BoundedMb<'a>) : Async<'a> =
    async.Delay mb.Take

  /// Returns all messages currently in the mailbox without waiting.
  let inline getAll (mb:BoundedMb<'a>) : Async<'a[]> =
    async.Delay mb.GetAll