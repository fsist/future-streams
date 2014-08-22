package com.fsist.stream

import com.fsist.stream.PipeSegment.{WithoutResult, Passthrough}
import com.fsist.util.FastAsync._
import com.fsist.util.concurrent.{CanceledException, BoundedAsyncQueue, CancelToken}
import com.typesafe.scalalogging.slf4j.Logging
import org.reactivestreams.api.Producer
import org.reactivestreams.spi.{Publisher, Subscriber}

import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** A source asynchronously publishes a sequence of zero or more elements of type `T`, followed by an EOF token.
  *
  * Instances can be created from external data (Iterables, generator functions, other Reactive Streams publishers, etc)
  * via methods on the companion object. They can also be implemented as Future-based state machines by extending
  * [[com.fsist.stream.SourceImpl]].
  *
  * = Relation to Reactive Streams =
  *
  * This type extends the Reactive Streams contract of `org.reactivestreams.api.Producer` and
  * `org.reactivestreams.spi.Publisher`. This means it only publishes elements
  * once the connected subscriber ([[com.fsist.stream.Sink]]) has requested them via
  * `org.reactivestreams.spi.Subscription.requestMore()`.
  *
  * (The separation between Producer and Publisher will be removed in Reactive Streams 0.4, and we don't
  * separate them.)
  *
  * This type has additional restrictions beyond the requirements of `org.reactivestreams.spi.Publisher`:
  *
  * 1. It is a 'cold' publisher. This means it never drops elements; it always waits for the connected Sink to request them.
  *    This makes it suitable to represent data that already exists (in memory, in a file, etc) or can be read or
  *    calculated on demand. It is not suitable to represent 'hot' data that is must be dropped if it is not processed
  *    in time, such as a timer producing a tick event every second.
  *
  * 2. Whenever no Sink is subscribed (both on creation of the Source, and if the Sink subsequently unsubscribes),
  *    the Source pauses and waits. Only when a Sink is subscribed and has requested an item will the next item be
  *    produced and published.
  *
  * 3. Only one Sink can be subscribed at a time. This is a natural fit for 'cold' streams: each new sink expects
  *    to read the whole stream from the start, not from the point in time when it subscribed, so each sink should
  *    have its own source. If you need to subscribe multiple sinks to one source, use the `fanout` method.
  *
  * This type also adds methods that a Reactive Streams publisher does not have: see `onSourceDone`.
  *
  * = Representations of the element stream =
  *
  * Reactive Streams has three methods the publisher calls: `onNext`, `onComplete` and `onError`.
  *
  * This type (e.g. in `produce`) uses Option[T] where Some(t) indicates an element (sent to onNext), None indicates
  * EOF (sent to onComplete), and a thrown exception indicates failure (sent to onError). Asynchronous components
  * express the next state of the Source as a Future[Option[T]].
  *
  * = Parametrization with ExecutionContext and CancelToken =
  *
  * All instances of this type have a `scala.concurrent.ExecutionContext` and a [[com.fsist.util.concurrent.CancelToken]]
  * specified at creation. Futures created by an instance will run in its ExecutionContext;
  * you can allocate different ECs to different instances to gain fine control over scheduling. If the CancelToken
  * of a source is cancelled, the source fails with a [[com.fsist.util.concurrent.CanceledException]] (see below regarding Source
  * states).
  *
  * = Possible states =
  *
  * Once a source has completed (published EOF), failed (published an exception), or been canceled, it will never do
  * anything again. These events can be observed via `SourceDone`, which completes when the source does,
  * or fails with the corresponding exception.
  *
  * A Source can only have one subscriber at once. Attempts to add a second subscriber, or to add a subscriber to a
  * source that has already completed or failed, will fail with a `java.lang.IllegalStateException`. Note that this exception
  * will not be thrown by the `subscribe` method synchronously; according to the Reactive Streams contract, it will
  * be delivered asynchronously to the attempted subscriber, calling its onError instead of onSubscribe.
  */
trait Source[T] extends Producer[T] with Publisher[T] with NamedLogger {
  /** All futures created by this Source will run in this context. */
  implicit def ec: ExecutionContext

  /** Canceling this token will fail the source with a [[com.fsist.util.concurrent.CanceledException]]. */
  implicit def cancelToken: CancelToken

  /** Returns a future that is completed when the source has generated EOF or experienced an error (in which case
    * the future is failed). If the source is canceled, this future fails with a [[com.fsist.util.concurrent.CanceledException]].
    */
  def onSourceDone: Future[Unit]

  /** @return the currently connected subscriber. */
  def subscriber: Option[Subscriber[T]]

  /// Beyond this line are methods with concrete implementations.

  /** Connects the source to a pipe and returns the pipe. */
  def >>[B](pipe: Pipe[T, B, _]): pipe.type = {
    subscribe(pipe)
    pipe
  }

  /** Connects the source to a sink. Returns the sink's `resultOnDone`. */
  def >>|[R](sink: Sink[T, R]): Future[R] = {
    subscribe(sink)
    sink.resultOnDone
  }

  /** If `next` is Some pipe, connect this source to the pipe and return the pipe (as a Source[T]).
    * If `next` is None, do nothing and return `this`. */
  def >>?(next: Option[Pipe[T, T, _]]): Source[T] = next match {
    case Some(pipe) => this >> pipe
    case None => this
  }

  /** Returns a Source that produces the output of `this` source and then the `next` one. */
  def ++(next: Source[T]): Source[T] = Source.concat(Seq(this, next))

  /** Take up to a set amount of elements from the stream.
    *
    * Note that due to the buffering in the current implementation, the original source (`this`) may actually produce
    * more than `count` elements. Only `count` elements will be emitted by the new Source returned from this method,
    * but if you intend to reuse the original Source after the new one unsubscribes from it, some elements may be lost.
    * In that case, consider improving this implementation.
    *
    * @param cancelOrigWhenDone if true, the orig Source (`this`) will be `cancel()`ed when enough elements have been
    *                           taken. If false, it will be merely unsubscribed from.
    */
  def take(count: Int, cancelOrigWhenDone: Boolean = false)(implicit ecc: ExecutionContext, cancel: CancelToken): Source[T] = {
    require(count >= 0)

    val orig = this

    val pipe = new WithoutResult[T, T] {
      private var taken: Int = 0
      private var canceledUpstream = false

      override def cancelToken: CancelToken = cancel
      override def ec: ExecutionContext = ecc
      override protected def process(input: Option[T]): Future[Boolean] = async {
        input match {
          case Some(t) =>
            if (taken < count) {
              taken += 1
              fastAwait(emit(input))
              false

            }
            else {
              cancelSubscription()
              if (cancelOrigWhenDone) {
                orig.cancelToken.cancel()
              }

              fastAwait(emit(None))
              true
            }
          case None =>
            fastAwait(emit(None))
            true
        }
      }

      override def onError(cause: Throwable): Unit = cause match {
        case _: CanceledException if canceledUpstream => // Do nothing
        case _ => super.onError(cause)
      }
    }
    this >>| pipe

    pipe
  }

  /** Skip a set amount of elements from the stream. */
  def skip(count: Int)(implicit ecc: ExecutionContext, cancel: CancelToken): Source[T] = {
    require(count >= 0)

    val pipe = new WithoutResult[T, T] {
      var skipped: Int = 0

      override def cancelToken: CancelToken = cancel
      override def ec: ExecutionContext = ecc
      override protected def process(input: Option[T]): Future[Boolean] = async {
        input match {
          case Some(t) =>
            if (skipped < count) {
              skipped += 1
              false
            }
            else {
              fastAwait(emit(input))
              false
            }
          case None =>
            fastAwait(emit(None))
            true
        }
      }
    }
    this >>| pipe
    pipe
  }

  // The following methods are shortcuts for Sink constructors

  /** Shortcut for `this >>| Sink.foreach` */
  def foreach(f: T => Unit)(implicit ecc: ExecutionContext): Future[Unit] = this >>| Sink.foreach(f)(ecc)

  /** Shortcut for `this >>| Sink.foreachInput` */
  def foreachInput(f: Option[T] => Unit)(implicit ecc: ExecutionContext): Future[Unit] = this >>| Sink.foreachInput(f)(ecc)

  /** Shortcut for `this >>| Sink.foreachInputM` */
  def foreachInputM(f: Option[T] => Future[Unit])(implicit ecc: ExecutionContext): Future[Unit] = this >>| Sink.foreachInputM(f)(ecc)

  /** Shortcut for `this >>| Sink.foreachM` */
  def foreachM(f: T => Future[Unit])(implicit ecc: ExecutionContext): Future[Unit] = this >>| Sink.foreachM(f)(ecc)

  /** Shortcut for `this >>| Sink.discard` */
  def discard()(implicit ecc: ExecutionContext): Future[Unit] = this >>| Sink.discard(ecc)

  /** Shortcut for `this >>| Sink.collect` */
  def collect()(implicit ecc: ExecutionContext) : Future[List[T]] = this >>| Sink.collect()(ecc)

  /** Shortcut for `this >>| Sink.fold` */
  def fold[S, R](initialState: S)(folder: (S, T) => S)(finalStep: S => R)(implicit ecc: ExecutionContext): Future[R] = this >>| Sink.fold(initialState)(folder)(finalStep)(ecc)

  /** Shortcut for `this >>| Sink.foldM` */
  def foldM[S, R](initialState: S)(folder: (S, T) => Future[S])(finalStep: S => Future[R])(implicit ecc: ExecutionContext): Future[R] = this >>| Sink.foldM(initialState)(folder)(finalStep)(ecc)
}

object Source extends Logging {

  /** Create a Source that generates elements from this Iterator. */
  def from[T](iter: Iterator[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override def ec: ExecutionContext = ecc
    override def cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = Future.successful(
      if (iter.hasNext) Some(iter.next)
      else None
    )
  } named "Source.from"

  /** Create a Source that generates elements from this Iterable. */
  def from[T](iter: Iterable[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    from(iter.iterator)

  /** Create a Source that generates these elements. */
  def apply[T](ts: T*)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = from(ts.toIterable)

  /** Creates a Source that emits EOF right away. */
  def empty[T](implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = apply()

  /** Create a Source that will call this function to produce each new element.
    *
    * This is equivalent to implementing `SinkImpl.produce`, see there. The function will be called non-concurrently.
    * It should return `Some(t)` to generate an element `t` or `None` to indicate EOF.
    */
  def generateM[T](stepper: => Future[Option[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override def ec: ExecutionContext = ecc
    override def cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = stepper
  } named "Source.generateM"

  /** Create a Source that will call this function to produce each new element.
    *
    * This is equivalent to `generateM` and `SinkImpl.produce`, except the function is synchronous. It will be called
    * non-concurrently. It should return `Some(t)` to generate an element `t` or `None` to indicate EOF.
    */
  def generate[T](stepper: => Option[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new SourceImpl[T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      protected def produce(): Future[Option[T]] = Future(stepper)
    } named "Source.generate"

  /** Wrap an existing Producer (from another implementation) in a Source.
    *
    * Internally, this creates a Pipe that is subscribed to the original Producer.
    *
    * TODO: this could be implemented more cheaply with introducing an extra Pipe and its async queue.
    */
  def from[T](producer: Producer[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = {
    val pipe = Passthrough[T]()
    producer.produceTo(pipe)
    pipe named "Source.from(Producer)"
  }

  /** Returns a Source that outputs the concatenated output of all these sources.
    *
    * TODO: this could be implemented more cheaply without introducing an extra Pipe and its async queue.
    */
  def concat[T](sources: Iterable[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = {
      val queue = new BoundedAsyncQueue[Try[Option[T]]](1)
      var done = false

      async {
        // Await must not be used under a nested function
        fastAwait(sources.foldLeft(success) {
          case (prevFuture, source) =>
            prevFuture flatMap (_ => source >>| Sink.foreachM(t => queue.enqueue(Success(Some(t)))))
        })
        done = true
        fastAwait (queue.enqueue(Success(None)))
      } recoverWith {
        case NonFatal(e) => queue.enqueue(Failure(e))
      }

      def next(): Future[Option[T]] = async {
        fastAwait(queue.dequeue()) match {
          case Success(s @ Some(t)) => s
          case Success(None) if done => None
          case Success(None) => fastAwait(next())
          case Failure(e) => throw e
        }
      }

      Source.generateM(next) named "Source.concat"
    }

  /** Converts a Future[Source] to a Source that will start producing elements after the `future` completes. */
  def flatten[T](future: Future[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Source[T] ={
    val pipe = PipeSegment.Passthrough[T]() named "Source.flatten internal pipe"
    future onSuccess {
      case source => source >>| pipe
    }
    future onFailure {
      case e => pipe.onError(e)
    }
    pipe
  } named "Source.flatten"

  /** A Source that allows pushing data through it by calling the extra methods defined here.
    *
    * Each overload of `push` returns a future. It can complete before the source actually produces this element,
    * but if you call push() faster than the Source can produce elements (due to backpressure from the connected Sink),
    * the returned Futures will not complete. (The created Source has an internal queue size of 1.)
    *
    * The `push` methods do not have to be called non-concurrently.
    */
  trait Pusher[T] extends Source[T] {
    def push(t: T) : Future[Unit]
    /** If None is passed, the source produces EOF. */
    def push(input: Option[T]) : Future[Unit]
  }

  /** Creates a Source that produces the data that is `push`ed into it. See the `push` methods defined on the [[Pusher]] trait.
    *
    * @param bufferSize size of the internal buffer of the Source (for elements pushed in but not yet sent to consumers).
    *                   Must be at least 1.
    */
  def pusher[T](bufferSize: Int = 1)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Pusher[T] = new SourceImpl[T] with Pusher[T] {
    require(bufferSize >= 1)
    private val queue = new BoundedAsyncQueue[Option[T]](bufferSize)
    @volatile private var seenEof : Boolean = false

    override def cancelToken: CancelToken = cancel
    override def ec: ExecutionContext = ecc
    override protected def produce(): Future[Option[T]] = queue.dequeue()

    def push(t: T) : Future[Unit] = push(Some(t))
    def push(input: Option[T]) : Future[Unit] = {
      // Of course this isn't precise; a few elements might be enqueued after EOF and will be stuck in the queue forever
      if (seenEof) {
        if (input.isEmpty) return success
        else Future.failed(new IllegalStateException(s"EOF was already pushed, cannot push more elements"))
      }

      if (input.isDefined) logger.trace(s"Pushing element")
      else {
        logger.trace(s"Pushing EOF")
        seenEof = true
      }

      queue.enqueue(input)
    }
  } named "Source.pusher"
}
