package com.fsist.stream

import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Subscription, Subscriber}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.Some
import scala.util.control.NonFatal
import com.fsist.util.{BoundedAsyncQueue, CancelToken}
import scala.async.Async._
import com.fsist.util.FastAsync._

/** A Sink receives data from a [[com.fsist.stream.Source]] and asynchronously signals it back when it is ready to receive more.
  * It may have some side effects, and/or calculate a result of type `R`.
  *
  * This result is available from the future returned by the `result` method, but see also `onSinkDone` and `resutOnDone`.
  * Sinks which don't calculate a result value substitute `R = Unit`.
  *
  * Instances can be created using methods on the companion object (e.g. `foreach`). They can also be implemented as
  * Future-based state machines by extending [[SinkImpl]].
  *
  * This type extends the Reactive Streams contract of `org.reactivestreams.api.Consumer` and
  * `org.reactivestreams.spi.Subscriber`.
  */
trait Sink[T, R] extends Consumer[T] with Subscriber[T] with NamedLogger {
  /** Default EC used by mapping operations */
  implicit def ec: ExecutionContext

  /** The result computed by this sink. This future should always complete eventually (deterministically)
    * once the sink has received EOF.
    *
    * This future may also complete before the sink receives an EOF. Whether the sink continues to receive more data
    * after that (by calling requestMore), and what it does with that data, depends on the sink implementation.
    * A standalone Sink will typically stop requesting more data. A Sink which is part of a Pipe will typically keep
    * requesting and forwarding data to downstream subscribers.
    */
  def result: Future[R]

  /** Returns a future that is completed when the sink receives end-of-input and finishes processing it.
    */
  def onSinkDone: Future[Unit]

  /** Returns the result published by `result`, but only once `onSinkDone` is completed. */
  def resultOnDone: Future[R] = onSinkDone flatMap (_ => result)

  /** Cancel our linked subscription (if any). The sink may NOT be resubscribed, but this frees the Source
    * to have a different Sink subscribed. */
  def cancelSubscription(): Unit

    /** Creates a new Sink wrapper that asynchronously maps the original sink's result once it is available.
    *
    * NOTE that this does not create a *separate* sink. In other words, the original sink and this sink cannot
    * be subscribed to different sources; any calls to subscribe(), onNext(), etc. on either one of them affect
    * both equally.
    *
    * This is called `mapResult` and not simply `map` to distinguish it from `Source.map` on Pipe.
    */
  def mapResult[K](func: R => K): Sink[T, K] = {
    val orig = this
    val mappedResult = orig.result.map(func) // Schedule `func` run even if noone accesses the returned Sink.result
    new Sink[T, K] {
      override def result: Future[K] = mappedResult

      override def onSinkDone: Future[Unit] = orig.onSinkDone

      override def onError(cause: Throwable): Unit = orig.onError(cause)

      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)

      override def onComplete(): Unit = orig.onComplete()

      override def onNext(element: T): Unit = orig.onNext(element)

      override def getSubscriber: Subscriber[T] = orig.getSubscriber

      override def ec: ExecutionContext = orig.ec

      override def cancelSubscription(): Unit = orig.cancelSubscription()
    }
  }

  /** Creates a new Sink wrapper that asynchronously maps the original sink's result once it is available.
    *
    * NOTE that this does not create a *separate* sink. In other words, the original sink and this sink cannot
    * be subscribed to different sources; any calls to subscribe(), onNext(), etc. on either one of them affect
    * both equally.
    *
    * This is called `flatMapResult` and not simply `flatMap` to distinguish it from `Source.flatMap` on Pipe.
    */
  def flatMapResult[K](func: R => Future[K]): Sink[T, K] = {
    val orig = this
    val mappedResult = orig.result.flatMap(func) // Schedule `func` run even if noone accesses the returned Sink.result
    new Sink[T, K] {
      // Lazy vals ensure we only call `func` once
      override def result: Future[K] = mappedResult

      override def onSinkDone: Future[Unit] = orig.onSinkDone

      override def onError(cause: Throwable): Unit = orig.onError(cause)

      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)

      override def onComplete(): Unit = orig.onComplete()

      override def onNext(element: T): Unit = orig.onNext(element)

      override def getSubscriber: Subscriber[T] = orig.getSubscriber

      override def ec: ExecutionContext = orig.ec

      override def cancelSubscription(): Unit = orig.cancelSubscription()
    }
  }

}

/** Contains factory methods for creating [[Sink]] instances.
  *
  * In the various families of methods (foreachXXX, foldXXX), an `M` suffix denotes a method returning a Future (the M
  * stands for Monadic and is a style I inherited from the play-iteratees library). An `Input` suffix denotes a method
  * that receives an `Option[T]` rather than a raw `T`; this lets it see EOF tokens explicitly.
  */
object Sink {

  /** Creates a Sink that runs `f` for each input. `f` is called non-concurrently.
    * It will never be called again if it fails (throws an exception).
    *
    * NOTE that `f` is run asynchronously in the supplied ExecutionContext.
    */
  def foreach[T](f: T => Unit)(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(input: Option[T]): Future[Boolean] = try {
      Future {
        if (input.isDefined) f(input.get)
        input.isEmpty
      }
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreach"

  /** Creates a Sink that runs `f` for each input. `f` is called non-concurrently. It is called with None to signify EOF.
    * It will never be called again if it fails (throws an exception).
    *
    * NOTE that `f` is run asynchronously in the supplied ExecutionContext.
    */
  def foreachInput[T](f: Option[T] => Unit)(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(t: Option[T]): Future[Boolean] = try {
      Future {
        f(t)
        t.isEmpty
      }
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreachInput"

  /** Creates a Sink that runs `f` for each input. `f` is called non-concurrently. It is called with None to signify EOF.
    * It will never be called again if it fails (throws an exception).
    */
  def foreachInputM[T](f: Option[T] => Future[Unit])(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(t: Option[T]): Future[Boolean] = try {
      async {
        fastAwait(f(t))
        t.isEmpty
      }
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreachInputM"

  /** Creates a Sink that runs `f` for each input. `f` is called non-concurrently.
    * It will never be called again if it fails (throws an exception).
    */
  def foreachM[T](f: T => Future[Unit])(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(input: Option[T]): Future[Boolean] = try {
      async {
        fastAwait(input match {
          case Some(t) => f(t)
          case None => success
        })
        input.isEmpty
      }
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreachM"

  /** Returns a sink that discards all input. */
  def discard[T](implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(t: Option[T]): Future[Boolean] = if (t.isDefined) falseFuture else trueFuture
  } named "Sink.discard"

  /** Returns a Sink that collects all results in a List[T]. */
  def collect[T]()(implicit ecc: ExecutionContext): Sink[T, List[T]] = new SinkImpl[T, List[T]] {
    override def ec: ExecutionContext = ecc

    private val buffer = ListBuffer[T]()

    override protected def process(t: Option[T]): Future[Boolean] = {
      if (t.isDefined) {
        buffer += t.get
        falseFuture
      } else {
        resultPromise.success(buffer.toList)
        trueFuture
      }
    }
  } named "Sink.collect"

  /** Returns a Sink that implements a `fold`. `f` is called non-concurrently.
    * It will never be called again if it fails (throws an exception).
    *
    * NOTE that `f` is run asynchronously in the supplied ExecutionContext. */
  def fold[T, S, R](initialState: S)(folder: (S, T) => S)(finalStep: S => R)(implicit ecc: ExecutionContext): Sink[T, R] = new SinkImpl[T, R] {
    override def ec: ExecutionContext = ecc

    private var state: S = initialState

    override protected def process(input: Option[T]): Future[Boolean] = Future {
      input match {
        case Some(t) =>
          state = folder(state, t)
          false
        case None =>
          resultPromise.success(finalStep(state))
          true
      }
    }
  } named "Sink.fold"

  /** Returns a Sink that implements an asynchronous fold().  `f` is called non-concurrently.
    * It will never be called again if it fails (throws an exception).
    */
  def foldM[T, S, R](initialState: S)(folder: (S, T) => Future[S])(finalStep: S => Future[R])(implicit ecc: ExecutionContext): Sink[T, R] = new SinkImpl[T, R] {
    override def ec: ExecutionContext = ecc

    private var state: S = initialState

    override protected def process(input: Option[T]): Future[Boolean] = async {
      input match {
        case Some(t) =>
          val newState = fastAwait(folder(state, t))
          state = newState
          false
        case None =>
          val result = fastAwait(finalStep(state))
          resultPromise.success(result)
          true
      }
    }
  } named "Sink.foldM"

  /** Returns a Sink which is already done, with a result that is immediately available. It never requests more data from a source. */
  def done[T, R](r: R)(implicit ecc: ExecutionContext): Sink[T, R] = new Sink[T, R] {
    override def result: Future[R] = Future.successful(r)

    override def onSinkDone: Future[Unit] = success

    override def onError(cause: Throwable): Unit =
      logger.error(s"Notified about error, but we're not subscribed to anything: $cause")

    override def onSubscribe(subscription: Subscription): Unit = ()

    override def onComplete(): Unit = ()

    override def onNext(element: T): Unit = ()

    override def getSubscriber: Subscriber[T] = this

    override def ec: ExecutionContext = ecc

    override def cancelSubscription(): Unit = ()
  } named "Sink.done"

  /** Convert a Future[Sink] to an actual Sink. The returned Sink will not request more than one element before the
    * `future` completes. */
  def flatten[T, R](future: Future[Sink[T, R]])(implicit ecc: ExecutionContext): Sink[T, R] = {
    val forwarder = PipeSegment.Passthrough[T] named "Sink.flatten internal pipe"
    future onSuccess {
      case sink => forwarder >>| sink
    }
    future onFailure {
      case e => forwarder.onError(e)
    }
    forwarder flatMapResult (_ => future flatMap (_.result))
  } named "Sink.flatten"

  /** A sink that exposes elements for external code to pull asynchronously. Return type of `Sink.puller`. */
  trait Puller[T] extends Sink[T, Unit] {
    /** Returns a Future that yields the next element.
      *
      * The future completes with Some element, None for EOF, or fails due to an upstream error.
      *
      * Note that after a future completes with None (EOF) or fails, all subsequent futures returned by this method
      * may never complete. Thus, the method should be called non-concurrently for safety.
      */
    def pull(): Future[Option[T]]
  }

  /** Returns a Sink that exposes elements via the `Puller.pull` method. It has an internal queue of size `bufferSize`
    * and will not process more elements than that until they are `pull`ed out. */
  def puller[T](bufferSize: Int = 1)(implicit ecc: ExecutionContext): Puller[T] =
    new SinkImpl[T, Unit] with Puller[T] {
      require(bufferSize >= 1)
      private val queue = new BoundedAsyncQueue[Option[T]](bufferSize)

      resultPromise trySuccess (())

      override def ec: ExecutionContext = ecc

      override protected def process(input: Option[T]): Future[Boolean] = async {
        fastAwait(queue.enqueue(input))
        input.isEmpty
      }

      override def pull(): Future[Option[T]] = queue.dequeue()
    } named "Sink.puller"
}
