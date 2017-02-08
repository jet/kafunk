[<AutoOpen>]
module Kafunk.BoundedMb

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Collections.Generic

type internal BoundedMbReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Take of AsyncReplyChannel<'a>

type internal BoundedMbCapacity<'a> =
  | Count of int
  | Condition of (seq<'a> -> bool)

type BoundedMb<'a> internal (capacity:BoundedMbCapacity<'a>) =

  let agent = MailboxProcessor.Start <| fun agent ->

    let queue =
      match capacity with
      | Count count -> Queue<_>(count)
      | Condition _ -> Queue<_>()

    let rec loop () = async {
      match queue.Count with
      | 0 -> do! tryReceive ()
      | n -> 
        match capacity with
        | Count count ->
          if count = n then
            do! trySend ()
          else
            do! trySendOrReceive () 
        | Condition cond ->
          if cond queue then
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
      rep.Reply () }

    and trySend () = 
      agent.Scan (function
        | Take rep -> Some (send rep)
        | _ -> None)

    and send (rep:AsyncReplyChannel<'a>) = async {
      let a = queue.Dequeue ()
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

  member __.TryTake (timeout:int option) =
    agent.PostAndTryAsyncReply ((fun ch -> Take ch), defaultArg timeout Timeout.Infinite)

  interface IDisposable with
    member __.Dispose () = (agent :> IDisposable).Dispose()

/// Operations on bounded mailboxes.
module BoundedMb =

  /// Creates a bounded mailbox with the specified capacity.
  let create (capacity:int) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(Count capacity)
    mq

  /// Creates a bounded mailbox bounded by the specified condition.
  /// If the condition return true, the mailbox blocks puts, otherwise accepts them.
  let createByCondition (condition:seq<'a> -> bool) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(Condition condition)
    mq

  /// Puts a message into the mailbox.
  /// If the mailbox is full, wait until it has room available.
  /// If the mailbox has vacancy, add the message to the buffer and return immediately.
  let inline put (a:'a) (mb:BoundedMb<'a>) : Async<unit> =
    mb.Put a

  /// Takes a message from
  let inline take (mb:BoundedMb<'a>) : Async<'a> =
    async.Delay mb.Take