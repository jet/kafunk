namespace Kafunk

// TODO: https://github.com/fsprojects/FSharpx.Async

open System
open System.Threading
open System.Collections.Generic

type internal BoundedMbReq<'a> =
  | Put of 'a * AsyncReplyChannel<unit>
  | Take of AsyncReplyChannel<'a>

type BoundedMb<'a> internal (capacity:int) =
  do if capacity < 1 then invalidArg "capacity" "must be positive"

  let agent = MailboxProcessor.Start <| fun agent ->

    let queue = new Queue<_>()

    let rec loop () = async {
      match queue.Count with
      | 0 -> do! tryReceive ()
      | n when n = capacity -> do! trySend ()
      | _ -> do! trySendOrReceive () 
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


module BoundedMb =

  let create (capacity:int) : BoundedMb<'a> =
    let mq = new BoundedMb<'a>(capacity)
    mq

  /// Puts a message into the mailbox.
  /// If the mailbox is full, wait until it has room available.
  /// If the mailbox has vacancy, add the message to the buffer and return immediately.
  let inline put (a:'a) (mb:BoundedMb<'a>) : Async<unit> =
    mb.Put a

  /// Takes a message from
  let inline take (mb:BoundedMb<'a>) : Async<'a> =
    async.Delay mb.Take