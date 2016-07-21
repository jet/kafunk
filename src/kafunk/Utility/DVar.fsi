namespace Kafunk

/// A dependant variable.
type DVar<'a> = private { mutable cell: 'a; event: Event<'a> }

/// Operations on dependant variables.
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module DVar =

  /// Creates a DVar and initializes it with the specified value.
  val create : 'a -> DVar<'a>

  /// Gets the current value of a DVar.
  val get : DVar<'a> -> 'a

  /// Puts a value into a DVar, triggering derived DVars to update.
  val put : 'a -> DVar<'a> -> unit when 'a : not struct

  /// DVar.put with arguments reversed.
  val inline set : DVar<'a> -> 'a -> unit when 'a : not struct

  /// Triggers a change in the DVar using its current value.
  val ping : DVar<'a> -> unit when 'a : not struct

  /// Publishes DVar changes as an event.
  val changes : DVar<'a> -> IEvent<'a>

  /// Subscribes a callback to changes to the DVar.
  val subs : DVar<'a> -> ('a -> unit) -> unit

  /// Invokes the callback with the current value of the DVar as well as all
  /// subsequent values.
  val iter : ('a -> unit) -> DVar<'a> -> unit

  /// Creates DVar derived from the argument DVar through a pure mapping function.
  /// When the argument changes, the dependant variable changes.
  val map : ('a -> 'b) -> DVar<'a> -> DVar<'b> when 'b : not struct

  /// Combine values of two DVars using the specified function.
  val combineLatestWith :
    ('a -> 'b -> 'c) -> DVar<'a> -> DVar<'b> -> DVar<'c>
    when 'c : not struct

  /// Combining two DVars into a single DVar containg tuples.
  val combineLatest : DVar<'a> -> DVar<'b> -> DVar<'a * 'b>

  /// Represents a DVar containing a function value as a function value which selects
  /// the implementation from the DVar on each invocation.
  val toFun : DVar<('a -> 'b)> -> ('a -> 'b)

  /// Returns a function which reconfigures in response to changes in the DVar.
  val mapFun : ('c -> 'a -> 'b) -> DVar<'c> -> ('a -> 'b)

  /// Creates a DVar given an initial value and binds it to an event.
  val ofEvent : 'a -> IEvent<'a> -> DVar<'a> when 'a : not struct

  /// Creates a DVar given an initial value and binds it to an observable.
  val ofObservable : 'a -> System.IObservable<'a> -> DVar<'a> when 'a : not struct

  /// Binds a DVar to a ref such that its current value is assigned to the ref
  /// and the ref is bound to all subsequent updates of the DVar.
  val bindToRef : 'a ref -> DVar<'a> -> unit

  val update : ('a -> 'a) -> DVar<'a> -> unit when 'a : not struct

  val updateIfDistinctBy :
    ('a -> 'k) -> ('a -> 'a) -> DVar<'a> -> bool
    when 'a : not struct and 'k : equality

  val updateIfDistinct :
    ('a -> 'a) -> DVar<'a> -> bool
    when 'a : not struct and 'a : equality

  val distinctBy :
    ('a -> 'k) -> DVar<'a> -> DVar<'a>
    when 'a : not struct and 'k : equality and 'k : not struct

  val distinct : DVar<'a> -> DVar<'a> when 'a : not struct and 'a : equality

  val extract : (DVar<'a> -> 'a)

  /// Creates a DVar<'b> using the value extracted from the argument
  /// DVar<'a>. Then, subscribes to the argument DVar and for every change,
  /// applies the same function and updates the resulting DVar. Each
  /// invocation of the function receives a reference to the argument
  /// DVar<'a> as it is at the time of change. As such, the function may
  /// subscribe the remainder of the change stream.
  ///
  /// Example: Suppose a DVar is a configuration value and you wish to create
  ///   a function which depends on this configuration value. This
  ///   constructor can be modeled as a function DVar<'a> -> ('i -> 'o).
  ///   DVar.extend applies this function to a DVar (e.g. from a configuration
  ///   source) and returns a DVar<'i -> 'o> which contains the desired
  ///   function. We therefore get a transformation DVar<'a> -> DVar<'i -> 'o>.
  ///   The contructor will usually bind certain dependencies to the DVar. What
  ///   the laws require is that each time this constructor is called:
  ///     - the argument DVar will contain the current value of the DVar at
  ///       change time
  ///     - it will publish the remainder of the changes to the DVar.
  ///
  /// The "context" in this instance of a Comonad is the state of the DVar
  /// immediately after the change which caused invocation.
  ///
  /// Law 1:
  ///
  ///     extend extract = id
  ///
  ///   - Interpretation: extracting is revered by extending. If you simply
  ///     extract the value from every DVar and return it, you get back the
  ///     argument DVar.
  ///
  /// Law 2:
  ///
  ///     extract << extend f = f : (DVar<'a> -> 'a)
  ///
  ///   - Interpretation: extending is reversed by extracting. In other words,
  ///     this operation is such that applying the function to an argument
  ///     DVar and extracting a value from the resulting DVar returns the same
  ///     value as simply applying the function to the argument DVar. This
  ///     essentially ensures that the resulting DVar<'a> is initialized with
  ///     the result of applying the function to the current value of the
  ///     arguement DVar.
  ///
  /// Law 3:
  ///
  ///     extend f << extend g = extend (f << extend g)
  ///       where g:DVar<'a> -> 'b ; f:DVar<'a> -> 'b
  ///
  ///   - Interpretation: extension is associative with respect to function
  ///     composition. In other words, extending a DVar with a function which
  ///     applies an argument function f to the result of extending its
  ///     argument DVar with a function g, is the same as extending a DVar.
  ///     This essentially means that when invoked from within a DVar context,
  ///     one does not get any special composition privileges.
  val extend : (DVar<'a> -> 'b) -> DVar<'a> -> DVar<'b> when 'b : not struct

  val choice : Choice<DVar<'a>,DVar<'b>> -> DVar<Choice<'a,'b>>

  val observe : DVar<'a> -> 'a * IEvent<'a>

  /// Left biased choose.
  val choose : DVar<'a> -> DVar<'a> -> DVar<'a> when 'a : not struct