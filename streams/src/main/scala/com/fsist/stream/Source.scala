package com.fsist.stream

import com.fsist.util.{BoundedAsyncQueue, CancelToken}
import com.typesafe.scalalogging.slf4j.Logging
import org.reactivestreams.api.{Consumer, Producer}
import org.reactivestreams.spi.{Subscription, Subscriber, Publisher}
import scala.async.Async._
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.control.NonFatal

/** A source asynchronously publishes a sequence of zero or more elements of type `T`, followed by an EOF token.
  *
  * = Relation to Reactive Streams =
  *
  * This type extends the Reactive Streams contract of [[org.reactivestreams.api.Producer]] and
  * [[org.reactivestreams.spi.Publisher]]. This means it only publishes elements
  * once the connected subscriber ([[com.fsist.stream.Sink]]) has requested them via
  * [[org.reactivestreams.spi.Subscription]]`.requestMore()`.
  *
  * (The separation between [[Producer]] and [[Publisher]] will be removed in Reactive Streams 0.4, and we don't
  * separate them.)
  *
  * This type has additional restrictions beyond the requirements of [[org.reactivestreams.spi.Publisher]]:
  *
  * 1. It is a 'cold' publisher. This means it never drops elements; it always waits for the connected Sink to request them.
  *    This makes it suitable to represent data that already exists (in memory, in a file, etc) or can be read or
  *    calculated on demand. It is not suitable to represent 'hot' data that is must be dropped if it is not processed
  *    in time, such as a timer producing a tick event every second.
  * 2. Whenever no Sink is subscribed (both on creation of the Source, and if the Sink subsequently unsubscribes),
  *    the Source pauses and waits. Only when a Sink is subscribed and has requested an item will the next item be
  *    produced and published.
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
  * All instances of this type have an [[scala.concurrent.ExecutionContext]] and a [[com.fsist.util.CancelToken]]
  * specified at creation. Futures created by an instance will run in its [[scala.concurrent.ExecutionContext]];
  * you can allocate different ECs to different instances to gain fine control over scheduling. If the CancelToken
  * of a source is cancelled, the source fails with a [[com.fsist.util.CanceledException]] (see below regarding Source
  * states).
  *
  * = Possible states =
  *
  * Once a source has completed (published EOF), failed (published an exception), or been canceled, it will never do
  * anything again. These events can be observed via `onSourceDone`, which completes when the source does,
  * or fails with the corresponding exception.
  *
  * A Source can only have one subscriber at once. Attempts to add a second subscriber, or to add a subscriber to a
  * source that has already completed or failed, will fail with an [[IllegalStateException]]. Note that this exception
  * will not be thrown by the `subscribe` method synchronously; according to the Reactive Streams contract, it will
  * be delivered asynchronously to the attempted subscriber, calling its onError instead of onSubscribe.
  *
  * = Creating instances =
  *
  * Instances can be created from external data (Iterables, generator functions, other Reactive Streams publishers, etc)
  * via methods on the companion object. Instances can also be built from existing instances using methods on this type.
  */
trait Source[T] extends Producer[T] with Publisher[T] with NamedLogger {
  /** All futures created by this Source will run in this context. */
  implicit def ec: ExecutionContext

  /** Canceling this token will fail the source with a [[com.fsist.util.CanceledException]]. */
  implicit def cancelToken: CancelToken

  /** Returns a future that is completed when the source has generated end-of-input or experienced an error (in which case
    * the future is failed). If the source is canceled, fails with a [[com.fsist.util.CanceledException]].
    */
  def onSourceDone: Future[Unit]

  /** @return the currently connected subscriber. */
  def subscriber: Option[Subscriber[T]]

  /// Beyond this line are methods with concrete implementations.

  /** See [[Pipe.mapSink]] */
  def map[K](f: T => K): Source[K] = this >> Pipe.map(f)

  /** See [[Pipe.flatMapSink]] */
  def flatMap[K](f: T => Future[K]): Source[K] = this >> Pipe.flatMap(f)

  /** See [[Pipe.mapInput]] */
  def mapInput[K](f: Option[T] => Option[K]): Source[K] = this >> Pipe.mapInput(f)

  /** See [[Pipe.flatMapInput]] */
  def flatMapInput[K](f: Option[T] => Future[Option[K]]): Source[K] = this >> Pipe.flatMapInput(f)

  /** Combines a source with a pipe and returns a new source representing both. */
  def >>[B](pipe: Pipe[T, B, _]): Source[B] = {
    val prev = this
    prev.subscribe(pipe)

    new Source[B] {
      override def ec: ExecutionContext = prev.ec
      override def cancelToken: CancelToken = prev.cancelToken

      override def onSourceDone: Future[Unit] = pipe.onSourceDone
      override def subscribe(subscriber: Subscriber[B]): Unit = pipe.subscribe(subscriber)
      override def produceTo(consumer: Consumer[B]): Unit = pipe.produceTo(consumer)
      override def getPublisher: Publisher[B] = pipe.getPublisher
      override def subscriber: Option[Subscriber[B]] = pipe.subscriber
    } named s"${prev.name} >> ${pipe.name}"
  }

  /** Subscribe the sink to this source, start running, and return the sink's result (`Sink.resultOnDone`). */
  def >>|[R](sink: Sink[T, R])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Future[R] = {
    subscribe(sink)
    sink.resultOnDone
  }

  /** Creates a Source that publishes all elements in this Source, and after it completes, all elements in the `next` Source.
    */
  def concat(next: Source[T]): Source[T] = Source.concat(Seq(this, next))

  /** Alias for `concat`. */
  def ++(next: Source[T]): Source[T] = concat(next)

  /** If `next` is Some pipe, return `pipe >> next.get`, discarding the pipe's result; otherwise return this source. */
  def >>?(next: Option[Pipe[T, T, _]]): Source[T] = next match {
    case Some(pipe) => this >> pipe
    case None => this
  }
}

object Source extends Logging {

  /** Create a Source that generates elements from this Iterator. */
  def from[T](iter: Iterator[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override def ec: ExecutionContext = ecc
    override def cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = Future {
      if (iter.hasNext) Some(iter.next)
      else None
    }
  } named "Source.from"

  /** Create a Source that generates elements from this Iterable. */
  def from[T](iter: Iterable[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    from(iter.iterator)

  /** Create a Source that generates these elements. */
  def apply[T](ts: T*)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = from(ts.toIterable)

  /** Create an empty Source. */
  def empty[T](implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = apply()

  /** Create a Source that will call this function to produce each new element.
    *
    * The function should return Some(t) as a new element, None to indicate the end of stream, or fail with an error
    * that will be propagated to Sinks.
    *
    * The function will not be called again until after each previously returned Future completes.
    * It will never be called again after it returns None or fails.
    *
    * If the function throws an exception synchronously, it will be treated as if it returned a failed Future.
    */
  def generateM[T](stepper: => Future[Option[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override def ec: ExecutionContext = ecc
    override def cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = stepper
  } named "Source.generateM"

  /** Create a Source that will call this function to produce each new element.
    *
    * The function should return Some(t) as a new element, None to indicate the end of stream, or throw an exception
    * that will be propagated to Sinks.
    *
    * The function will not be called in parallel. It will never be called again after it returns None or fails.
    */
  def generate[T](stepper: => Option[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new SourceImpl[T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      protected def produce(): Future[Option[T]] = try {
        Future(stepper)
      }
      catch {
        case NonFatal(e) => Future.failed(e)
      }
    } named "Source.generate"

  /** Wrap an existing Producer in a Source. */
  def from[T](producer: Producer[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new Source[T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      /** Completed when the producer sends EOF or error */
      private val completion = Promise[Unit]()

      override def onSourceDone: Future[Unit] = completion.future

      override def produceTo(consumer: Consumer[T]): Unit = {
        producer.produceTo(consumer)
      }

      override def getPublisher: Publisher[T] = producer.getPublisher

      override def subscribe(subscriber: Subscriber[T]): Unit = {
        producer.getPublisher.subscribe(subscriber)
      }

      /** TODO to implement this we'd need to wrap the Subscriber in a thin interface that would be subscribed,
        * so as to capture the onSubscribe etc. calls to know who is really subscribed.
        * And even then we couldn't support multiple subscribers, while the Producer could.
        */
      override def subscriber: Option[Subscriber[T]] = ???
    } named "Source.from(Producer)"

  /** Returns a Source that outputs the concatenated output of all these sources.
    *
    * TODO: replace with a custom Producer/Consumer impl that passes through calls directly and doesn't involve an extra
    * async stage and queue. This should become a PML for Producer which doesn't even depend on our custom Source.
    */
  def concat[T](sources: Iterable[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new Source[T] with NamedLogger {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private val iter = sources.iterator
      private var iterCompleted : Boolean = false

      private val pipe = new PipeSegment.Passthrough[T] {
        override def ec: ExecutionContext = ecc
        override def cancelToken: CancelToken = cancel

        override def process(input: Option[T]): Future[Boolean] =
          input match {
            case s: Some[T] => emit(s) map (_ => false)
            case None =>
              if (iterCompleted) {
                emit(None) map (_ => true)
              }
              else {
                falseFuture
              }
          }
      } named "Source.concat internal pipe"

      // Start
      runNext()

      private def runNext(): Unit = {
        if (iter.hasNext) {
          val source = iter.next()
          logger.trace(s"switching to next source ${source.name}")
          source.subscribe(pipe)
          source.onSourceDone map { _ =>
            // If the source fails, it will notify the pipe of completion
            logger.trace(s"source ${source.name} done")
            runNext()
          }
        }
        else {
          logger.trace(s"out of sources")
          iterCompleted = true
          pipe.onComplete()
        }
      }

      override def onSourceDone: Future[Unit] = pipe.onSourceDone
      override def subscribe(subscriber: Subscriber[T]): Unit = pipe.subscribe(subscriber)
      override def produceTo(consumer: Consumer[T]): Unit = pipe.produceTo(consumer)
      override def getPublisher: Publisher[T] = pipe
      override def subscriber: Option[Subscriber[T]] = pipe.subscriber
    } named "Source.concat"

  /** Converts a Future[Source] to a Source that will start producing elements after the `future` completes. */
  def flatten[T](future: Future[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken) : Source[T] ={
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
