# future-streams

A `scala.concurrent.Future`-based implementation of [Reactive Streams](http://www.reactive-streams.org).

## Purpose and design constraints

I wrote this library because I needed a scala model for streams with backpressure and transformations. I couldn't wait
for other implementations of Reactive Streams, like [akka-streams](http://typesafe.com/activator/template/akka-stream-scala), 
to make a production-quality release.

I chose to implement on top of the abstraction of Futures, because they are already used in many other libraries and 
are a good basis for interoperability of asynchronous code outside of Akka Actors.

I chose to implement Reactive Streams, to be compatible with other libraries and perhaps even allow me to port my own
codebase to another library in the future. The design and implementation is guided by Reactive Streams, rather than
being some other more complex design that can be converted to/from Reactive Streams interfaces when necessary.
 
The library implements the Reactive Streams API (both 0.3, and 0.4 as soon as that is released). It should interoperate 
with other implementations, although I haven't tested that yet. Once such other implementations are stable, it may
make sense for me as well for others to switch to another implementation. There's nothing this library does that could
not *in principle* be done with akka-streams, but that does not mean it *will* be done there.

## Description without reference to Reactive Streams ##

The Scaladoc for the two main types - `Source` and `Sink` - is the master reference. This is a summary only.

A `Source[T]` is an asynchronous push-based producer of `T` elements. It may eventually produce an EOF token or an exception, 
after which it will not produce any more elements.

To consume elements, you need a `Sink[T]`. To transform the stream between the two, you can use a `Pipe[A, B]`, but that
is merely a shorthand for `Sink[B] with Source[A]`.

The stream is asynchronous, push-based, and obeys backpressure. If it were not asynchronous, you could use a `scala.io.Source`.
If it were not push-based, you could use a model similar to `java.io.InputStream` or `java.nio.Channel`. If it did not
support backpressure, you could use [Rx](https://github.com/Netflix/RxJava).

Here's a simple example of a stream:

```
Source(1 to 1000) >> Pipe.map(_ * 2) >>| Sink.foreach(println(_))
```

How is this different from `(1 to 1000) map (_ * 2) foreach (println(_))`? Mainly by its extreme inefficiency. Each step
along the way - producing an element from the Range, mapping it, and printing it - will run asynchronously in a separate
Future. Normally you wouldn't want that.

Here's a more useful example:

```
val src: File = ???; val dest: File = ???
val fut : Future[Unit] = Nio.source(src) >> Pipe.map( some transform on ByteString ) >>| Nio.sink(dest)
```

This passes data between two files, transforming it on the way. The whole expression returns a `Future[Unit]` that 
completes when the operation does.

## Differences from generic Reactive Streams

This is a faithful implementation of Reactive Streams, with `Source` corresponding to `Publisher/Producer` and `Sink` to
`Subscriber/Consumer`, but it obeys some additional constraints.

1. *Cold streams*. All Sources produced by this library are 'cold'. This means they never drop data because a subscriber
   is not ready to receive it; instead they wait for the subscriber. If no subscriber is present (including when one
   unsubscribes), a Source will wait for another to subscribe.
   This is suitable for sources that produce data from storage or is calculated when it is requested by a deterministic
   function. It is unsuitable for sources that produce data tied to a clock (e.g. a clock) or data that becomes 
   irrelevant if not processed quickly (e.g. a stock ticker).
2. *Singlecast*. All Sources support exactly one subscriber. This makes both the API and the implementation much simpler.
   Multicast behavior can still be obtained via explicit methods that split a Source into several Sources.
3. Once a Source enters an error state (and calls `onError` on its subscriber), it will never recover.

It also adds some features:

1. *Cancellation*. Each Source instance has a `CancelToken` (essentially a `Promise[Unit]`) which can be signalled 
   asynchronously to abort that source.
2. *Result calculation*. The Sink type is really `Sink[T, R]` where `R` is the result type which the sink computes.
   This may be `Unit` for sinks that have side effects, or it may be something else, such as a hash of all the data
   passed in. The calculated result is available from `Sink.result: Future[R]`.
   Since a `Pipe` is also a `Sink`, pipe segments can also calculate intermediate results.
   
And in the future:

3. Explicit closing of a Source (via `Source.close()` method). This allows closing all the Sources in a pipeline
   when a Sink wishes to shut down the pipeline without observing an EOF. The Sources can then close IO resources
   they may hold open.
   
## Typical use

As demonstrated above, a pipeline can be built using the various constructor methods on the `Source`, `Pipe`, and `Sink`
companion objects.
 
There are some simple ways of constructing Sources and Sinks. A Source can be built from an existing `scala.io.Source` 
or `Iterable`, or from a generator function:
```
def generate[T](stepper: => Option[T]): Source[T]
def generateM[T](stepper: => Future[Option[T]]): Source[T]
```

(An `Option[T]` is used to signal EOF when `None` is returned.) 

A Pipe can be built as a `map` operation with many asynchronous variants, ranging from the simple 
`def map[A, B](f: A => B): Pipe[A, B, Unit]` to the complex 
`def flatMapInput[A, B](f: Option[A] => Future[Option[B]]): Pipe[A, B, Unit]`.

Finally, a Sink can be built using constructors like `foreach` and `foreachM`.

Once you have the different elements, you can combine them: `source >> pipe >> pipe >>| sink`.

### More useful endpoints for Sources and Sinks

Sources and Sinks can be built from `java.nio.AsynchronousByteChannel`s (sockets and files).
They can also be built from `java.io.InputStream`s and `OutputStream`s, but since these are inherently blocking,
they will consume a thread for each such source or sink, and will not scale.

These are provided by the `ByteSink` and `ByteSource` objects. They use `akka.util.ByteString` as their element type,
which requires a dependency on `akka-actor`. For this reason, they are packaged separately, in the `bytestreams` module.

Additionally, the class `AsyncQueue` represents a queue whose `dequeue()` method returns a Future[T]. Enqueueing is
synchronous. This allows bridging between data sources that push elements without back-pressure support (e.g. by sending
actor messages); those messages can be enqueued, and the queue's `source()` method will return a `Source` that will 
produce them. Of course, the queue is unbounded, so if you don't consume the elements quickly enough, you will run out
of memory.

If you have a producer that does support back-pressure and expects to call an `enqueue(t): Future[Unit]` method,
you can use a `BoundedAsyncQueue` instead.

### State machines

Each Source and Sink instance is (potentially) mutable, and obeys the strict concurrency guaranteed specified by
Reactive Streams. This lets us write Future-based state machines. I should really come up with better examples
that actually require asynchronicity, but this should at least demonstrate the mechanism.

```
val eof = None

val fibonacci: Source[Int] = new SourceImpl[String] {
    private val end : Int = 100
    
    private var a: Int = 1
    private var b: Int = 1
    override protected def produce(): Future[Option[T]] = {
        val ret = a + b
        a = b
        b = ret
        if (ret < end) Future.successful(Some(ret))
        else Future.successful(eof)
    }
}

val multiplier: Pipe[Int, Int, Unit] = new PipeSegment[Int, Int, Unit] {
    override protected def process(t: Option[A]): Future[Boolean] = async {
        await(emit(t map (_ * 2)))
        t.isEmpty // Returns true if done
    }
}

val summer : Sink[Int, Int] = new SinkImpl[Int, Int] {
    private var sum: Int = 0
    override protected def process(input: Option[T]): Future[Boolean] = {
        input match {
            case Some(i) => 
                sum += i
                Future.successful(false)
            case eof =>
                resultPromise.success(sum)
                Future.successful(true)
        }
    }
}

```

The API isn't pretty and will change soon, but I hope the intent comes through.

## Performance issues

The streams I actually use it with are all IO-bound: files, sockets, HTTP messages, database queries. The chunks of
data are relatively large and few. Therefore, the performance of the implementation has taken a second seat to simplicity
and correctness. Although I plan to improve performance significantly in the future, it probably will never equal that
of akka-streams. 

As an upper bound to performance, the Scala Future and Promise objects are relatively expensive.
Also, non-default ExecutionContext configurations may need to be used to achieve good performance with my usage patterns.
It's possible to assign a different `ExecutionContext` to each `Source`, `Pipe` and `Sink` in a pipeline, and this may
be used to optimize specific use cases.

The biggest immediate problem with multi-stage pipelines is that all stages are currently fully asynchronous
(that is, they are `Pipe`s, and `Pipe extends Source with Sink`). The Reactive Streams specification assumes an
asynchronous boundary between all such components, which means even synchronous transformations like `map` require each
element to be wrapped in a new Future. (The current code uses `Future.map` with even already-completed futures, and that
at least could be partially eliminated.)

The next version of this library will include a mechanism for synchronous transformations of existing `Source`s, which
should alleviate this issue. After that change, I expect the performance of the library to be good enough in IO-bound
scenarios to satisfy my personal use cases, so I can't promise I will evolve the library beyond that point.

The library also uses several underlying utility classes - `AsyncQueue`, `BoundedAsyncQueue` and `AsyncSemaphore` -
whose implementation could probably be optimized.

## Current state

Everything described above is implemented. However, there are insufficient tests at present, and there may be 
undiscovered bugs. I consider the library to be beta quality.

No API stability guarantee is made. 

## Future plans

Apart from many small changes and cleanups necessary, the one large change planned is synchronous transformations.
These would be expressed as `source.map(f: A => B)`, `source.foreach(f: A => Unit)`, etc. Unlike explicitly creating 
an equivalent Pipe or Sink, they would run synchronously on the Source's own stack.

The semantics of such a `map` method would be subtly different from `map` methods in other libraries. They would modify
the original object mapped, because the new Source would be effectively subscribed to the old one, and you could not 
subscribe something else to the mapped `Source` anymore. (If something was already subscribed, mapping it would fail.)
For this reason, I may end up naming the `map` method in this example something else that makes it explicit that the
original object is being modified, like `map!`.
