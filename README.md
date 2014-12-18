# future-streams

A `scala.concurrent.Future`-based model of streams with backpressure, with 
[Reactive Streams](http://www.reactive-streams.org) support. Synchronous stream operations are executed synchronously,
instead of putting each step in a separately scheduled `Future`.

## Purpose and design constraints

This library implements asynchronous streams with back-pressure in scala. It is a poor man's akka-streams, similar in 
design and purpose to akka-streams, but on a much smaller scale. This enabled me to write, test and ship it quickly,
because I couldn't wait for akka-streams to be ready for production. Akka-streams should be ready in the next few months,
and then this library will be obsolete; I expect to switch to using akka-streams myself at some future date.

For this reason, future-streams doesn't include some good features and design choices, because they would take too much
work to implement and test.
 
future-streams can support converters to/from Reactive Streams and pass the RS 1.0-RC1 TCK, but I haven't yet written 
that code.

## High-level description

Note: the Scaladoc for the main types, such as `Source` and `Sink`, is the authoritative reference for details of
behavior. This is only a summary.

A stream contains one or more `StreamInput`s and one or more `StreamOutput`s, and zero or more `Transform`ers and 
`Connector`s. These are connected to one another in a directed manner, forming a digraph in which we can speak of 
components 'upstream' and 'downstream' relative to each node.

Stream components, which are also called (graph) nodes, come in several types. There are some common traits these
types can implement:

1.  A `Source[Out]` produces elements of type `Out`.
2.  A `Sink[In]` consumes elements.

There are four main node types:

1.  A `StreamInput[Out] extends Source[Out]` introduces new elements into the stream from elsewhere. It has no upstream 
    connection.
2.  A `StreamOutput[In] extends Sink[In]` consumes stream elements. It has no downstream connection.
3.  A `Transform[In, Out] extends Sink[In] with Source[Out]` transforms the stream in some way. Output elements
    don't always correspond one-to-one to input elements.
4.  A `Connector[In, Out]` is a special node type which can connect to more than one upstream or downstream node.
    This serves to fan-out or fan-in stream connections. The Connector doesn't extend `Source[In] with Sink[Out]`
    because it isn't a single Source or Sink; instead it makes multiple Sources and Sinks available as methods.

There is also an auxiliary type called `Pipe[In, Out]`, which represents a Sink linked to a Source with a black box
in the middle. It can abstract away sub-graphs of the stream model which are already internally connected.
    
### Modeling and running the stream

Instances of these node types are case classes (e.g. `IteratorSource` is a kind of `StreamInput`). These instances
are, therefore, immutable; this allows them to be co- and contra-variant in the expected ways (e.g. `Source[+Out]`).
Creating these instances, and linking them together to form a graph using methods such as `Source.connect(sink)`,
does not cause the stream to start executing.

When you have finished building the stream model from these components, forming a fully connected graph in which every
`Source` is connected to a `Sink` and vice versa, you need to call the `build()` method on any of the stream components.
This method materializes the components into state machine implementations and starts running them by scheduling one or
more Futures. It returns a `RunningStream`, the live stream's public API.

Each component in a running stream is modeled by the `RunningStreamComponent` trait, with sub-traits for running inputs,
outputs etc. A running component has a member `completion: Future[Unit]` which is completed when the member has processed
the EOF signal. This happens when the component's `onComplete` handler (which can be supplied by the user) completes,
and the component has finished passing the EOF signal downstream.
 
`RunningStream` contains methods (overloads of `get` and `apply`) which return the `RunningStreamComponent` instance
corresponding to the model case class instance passed in.

In practice, the timing of the completion of different components in the same stream is not intended to be a useful,
strongly specified kind of behavior. In particular, downstream components may complete before or after upstream ones,
because some component implementation may wait for their downstream to complete before completing themselves.

The whole `RunningStream` also has a global completion event (`RunningStream.completion: Future[Unit]`) which completes
when all components have completed.

#### Stream components cannot be reused

Although stream component model types are case classes, they *cannot* be reused in other streams, and the same stream 
model cannot be built and run more than once. Although this is a desirable feature (which exists in akka-streams), it 
would take too much effort to implement, even though the design itself might be simple. 

### Stream results

A `StreamOutput[In, Res]` has two type parameters: in addition to the stream element type `In`, there is the result type
`Res`. This optional result is returned by the output's `onComplete` handler (which is supplied by the user when 
constructing the component). 

Unlike in future-streams v1, this result cannot be completed before whole component completes. Components that wish
to provide data earlier should take or provide callbacks or Promises when their model is constructed.
 
### Stream failures

A stream component is considered to fail if any of its user-provided onXxx methods throw exceptions or return a failed
Future.

If any stream component fails, the whole stream will immediately fail as well; failures cannot be compartmentalized.
Components which want to recover from failures must do so inside their implementation.

When a stream fails, the `onError` callbacks of all components are called in parallel. This is *concurrent* with any 
ongoing calls to their `onNext` and `onComplete` methods. In fact, `onNext` and/or `onComplete` may be called again after
`onError` has been called (or while it is running), due to race conditions.

However, if a component has already completed (`onComplete` has finished and the `completion` promise has been fulfilled),
then `onError` is guaranteed not to be called for that component.

Each call to `onError` is scheduled as a separate Future, so implementations can take as long as they need.

The `RunningStreamComponent.completion` future of each component that hasn't completed yet, and the 
`RunningStream.completion` future of the entire stream, are all failed with the exact original exception of the original 
failing component. This happens *before* the execution of the `onError` handler of that component.

A stream can also be failed deliberately from the outside by calling `RunningStream.fail(throwable)`. This causes the 
same behavior as if a stream component had failed with this exception.

## Examples

### A simple processing pipeline

    Source.from(1 to 100).map(_ + 1).foreach(println(_)).buildResult()
    
This returns a `Future[Unit]` which is the result of the `StreamOutput` that corresponds to the `foreach` statement.
It will complete when all 100 elements have been printed. The whole stream runs synchronously, because it was built
from synchronous components, so the `println(_)` function will see the `map(_+1)` function on its calling stack.
However, the stream as a whole still runs in a Future.

### Scatter-gather

Here is a more complex graph, which also demonstrates combining stream parts defined separately:
    
    implicit ec = ExecutionContext.global
    
    var state = 0
    val source = Source.generate(AsyncFunc(Future {
      state += 1
      if (state > 5) throw new EndOfStreamException
      else state
    }))
  
    val mapper = Transform.flatMap((i: Int) => 1 to i)
  
    val scattered = source.scatter(3)
    val merge = Connector.merge[Int](3)
    for ((output, input) <- scattered.outputs zip merge.inputs)
      output.flatMap((i: Int) => 1 to i).connect(input)
  
    val result = merge.output.toList().buildResult()

The `result` is a `Future[List[Int]]`. The order of elements in it is indeterminate, because the three pipelines after
`scatter` run in parallel.

### Pipelines

The `Pipe[In, Out]` abstraction allows libraries to provide pipe segments which can then be used as building blocks
by the user code.

In this example, we might have various stream transformations available:

    trait MyTransform {
      def apply(): Pipe[Int, Int]
    }

    class Ceiling(max: Int) extends MyTransform {
      override def apply(): Pipe[Int, Int] = Pipe(Transform((i: Int) => Math.max(i, max)))
    }

    object WeirdExample extends MyTransform {
      override def apply(): Pipe[Int, Int] =
        Transform((i: Int) => i + 1).pipe(_ / 2).pipe(_ * 3)
    }

And then user code could compose them generically:

    val transforms: Seq[Pipe[Int, Int]] = ???

    

## Low-level detail

### Non-concurrent methods

Non-concurrency is a calling convention of multiple methods (or functions).

Two or more methods are said to be called non-concurrently with respect to one another, if each new call to one of them
begins (from the point of view of the callee) after the previous call to one of them has completed. 

If any of the methods are asynchronous, a call completing means the Future returned by the call is completed.
  
This calling convention applies to the `onNext` and `onComplete` (but not `onError`) methods of any *one* stream component.

This calling convention guarantees that each subsequent call will 
[see all memory effects](https://github.com/reactive-streams/reactive-streams/issues/53#issuecomment-43916232)
of the previously completed call (including effects of code that ran in a Future returned by the previous call).
This means mutable state can be used with ordinary variable fields, without `@volatile` annotations or `AtomicReference`.

### Func

The type `com.fsist.util.concurrent.Func[-A, +B]` abstracts over synchronous functions `A => B` and asynchronous ones 
`A => Future[B]`, which are represented by `SyncFunc` and `AsyncFunc` respectively.

It provides efficient combinators for composition of functions (`func1.compose(func2)` or `func1 ~> func2`), error 
recovery (using either try/catch or Future.recover as appropriate), and other useful idioms. 

This abstraction is convenient, because most code doesn't need to care if a particular function is asynchronous or not.
However, function invocation can be as much as 10 times slower than method invocation, because the JVM is bad at inlining
function calls (see [here](https://groups.google.com/d/msg/scala-user/a96iW30_FFM/QZFKzbN8R4QJ) and 
[here](http://www.azulsystems.com/blog/cliff/2011-04-04-fixing-the-inlining-problem)). For this reason, in hotspots 
I implement traits (which compiles to methods) instead of using `Func`s.

### FutureStreamBuilder

Stream components are defined as case classes, so they're immutable. In particular, they don't change when connected
to one another. 

The mutable state representing the structure of the stream model being built is stored in instances of the mutable
(but concurrent-safe) class `FutureSreamBuilder`. This class also implements the `build` method that materializes a
stream model.

Every stream component model case class has a parameter of type `FutureStreamBuilder`. Constructor methods take an implicit
builder instance with a default value of `new FutureStreamBuilder`. Components belonging to different builders can be
connected to one another. When a stream is materialized and run, it does not matter how components are distributed across
different builders, and it doesn't matter which builder you use to call `build`.

## Differences from the previous version of this library

This is the second version of future-streams, and is a complete redesign from the ground up. Without going into too much
detail about the old version, these are the major *design* differences:

1.  Not every stream component implements the public Reactive Streams interfaces (Publisher/Subscriber); only explicitly
    constructed converters do (e.g. `Source.fromPublisher`). This allows the internal communication between stream
    components to be much faster, and to have slightly different semantics from RS. It also makes the implementation much
    simpler.
2.  Synchronous functions and stream components receive first-class support, and are called and combined synchronously, 
    without the expense of scheduling Futures.
3.  There is a separate model or blueprint API (`Source`, `Sink`, `Transform`, `Connector`) and a runtime API 
    (`StateMachine`, `FutureStream`), bridged by the `FutureStreamBuilder`. This is done for some of the same reasons
    as the materialization phase in akka-streams: to allow models to use the expected variance (e.g. `Source[+Out]`),
    and to reuse models where appropriate.
4.  Results can only be computed by `StreamOutput` components (and not by other `Sink`s), and are only available once
    the component has completed (which in practice is usually when the whole stream completes). This is intended as a
    convenience, not as a core feature. Components that wish to expose results earlier than that are expected to simply
    take, or return, a Promise in their model, or a user callback in the form of an extra Func, which they can then
    fulfill or call whenever they want to.
5.  `StreamInput`, `StreamOutput` and the various types of `Transform` and `Connector` all have dedicated implementations
    in the library core (in subclasses of `StateMachine`). The core model traits and classes are all respectively sealed
    and final. Other abstractions are built on top of that in user code.
    This contrasts with the v1 model, where the only first-class types were `Source` and `Sink`, and all implementations
    had the same status.
    This allows us keep the core implementation simple, fast and correct. The variety of component implementations in v1
    made the library unmanageable.

### Removed features

These minor features or combinators are no longer directly supported (most have workarounds):

1.  There is no explicit support for cancellation. Asynchronous cancellation with e.g. a CancelToken can be achieved by
    failing the stream from outside.
2.  `Source.flatten`, `Sink.flatten` and `Pipe.flatten`, which convert a `Future[Source]` to a `Source` etc., are no
    longer available. All stream components need to exist concretely when the stream is materialized.
    It would be possible to implement `flatten` in the future-streams library core, but so far I'm trying to do without.
3.  Adapters for java.io and java.nio are no longer included, because they are not part of the core concern of this library.
    (They are still just as easy to write.)
