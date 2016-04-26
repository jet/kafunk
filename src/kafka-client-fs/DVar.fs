namespace KafkaFs

open System
open System.Threading

/// A dependant variable.
type DVar<'a> = private { mutable cell : 'a ; event : Event<'a> }

/// Operations on dependant variables.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DVar =

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

  let updateIfDistinctBy (key:'a -> 'k) (update:'a -> 'a) (d:DVar<'a>) : bool =
    let current = get d
    let currentKey = key current
    let updated = update current
    let updatedKey = key updated
    if currentKey <> updatedKey then
      put updated d
      true
    else
      false

  let updateIfDistinct (update:'a -> 'a) (d:DVar<'a>) : bool =
    updateIfDistinctBy id update d

  let distinctBy (key:'a -> 'k) (d:DVar<'a>) : DVar<'a> =
    let a = get d
    let mutable prevKey = key a
    let d' = create a
    subs d <| fun a ->
      let key = key a
      if key <> Interlocked.Exchange (&prevKey, key) then
        put a d'
    d'

  let distinct (d:DVar<'a>) : DVar<'a> =
    distinctBy id d

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
