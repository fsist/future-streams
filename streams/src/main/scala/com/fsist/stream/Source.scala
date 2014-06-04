package com.fsist.stream

import com.fsist.util.{BoundedAsyncQueue, CancelToken}
import com.typesafe.scalalogging.slf4j.Logging
import org.reactivestreams.api.{Consumer, Producer}
import org.reactivestreams.spi.{Subscriber, Publisher}
import scala.concurrent.{Promise, ExecutionContext, Future}
import com.fsist.stream.PipeSegment.Passthrough
import scala.async.Async._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

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
  * All instances of this type have a `scala.concurrent.ExecutionContext` and a [[com.fsist.util.CancelToken]]
  * specified at creation. Futures created by an instance will run in its ExecutionContext;
  * you can allocate different ECs to different instances to gain fine control over scheduling. If the CancelToken
  * of a source is cancelled, the source fails with a [[com.fsist.util.CanceledException]] (see below regarding Source
  * states).
  *
  * = Possible states =
  *
  * Once a source has completed (published EOF), failed (published an exception), or been canceled, it will never do
  * anything again. These events can be observed via onSourceDone`, which completes when the source does,
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

  /** Canceling this token will fail the source with a [[com.fsist.util.CanceledException]]. */
  implicit def cancelToken: CancelToken

  /** Returns a future that is completed when the source has generated EOF or experienced an error (in which case
    * the future is failed). If the source is canceled, this future fails with a [[com.fsist.util.CanceledException]].
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
    * TODO: this could be implemented more cheaply with introducing an extra Pipe and its async queue.
    */
  def concat[T](sources: Iterable[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = {
      val queue = new BoundedAsyncQueue[Try[Option[T]]](1)
      var done = false

      async {
        // Await must not be used under a nested function
        await(sources.foldLeft(success) {
          case (prevFuture, source) => prevFuture flatMap (_ => source >>| Sink.foreachM(t => queue.enqueue(Success(Some(t)))))
        })
        done = true
        await (queue.enqueue(Success(None)))
      } recoverWith {
        case NonFatal(e) => queue.enqueue(Failure(e))
      }

      def next(): Future[Option[T]] = async {
        await(queue.dequeue()) match {
          case Success(s @ Some(t)) => s
          case Success(None) if done => None
          case Success(None) => await(next())
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
}
