package com.fsist.stream

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import com.fsist.stream.SinkImpl.WithoutResult
import com.fsist.stream.Source.Pusher
import com.fsist.util.FastAsync._
import com.fsist.util.concurrent.{AsyncQueue, CancelToken, BoundedAsyncQueue}
import org.reactivestreams.{Subscription, Subscriber}

import scala.async.Async._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.control.NonFatal

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
trait Sink[T, R] extends Subscriber[T] with NamedLogger {
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

      override def ec: ExecutionContext = orig.ec

      override def cancelSubscription(): Unit = orig.cancelSubscription()
    }
  }

  /** Returns a new `Source.Pusher` that pushes to this sink. */
  def toPusher(): Pusher[T] = {
    val p = Source.pusher[T]()
    p >>| this
    p
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
        resultPromise.trySuccess(buffer.toList)
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
          resultPromise.trySuccess(finalStep(state))
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
          resultPromise.trySuccess(result)
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

    override def ec: ExecutionContext = ecc

    override def cancelSubscription(): Unit = ()
  } named "Sink.done"

  /** Convert a Future[Sink] to an actual Sink. */
  def flatten[T, R](future: Future[Sink[T, R]])(implicit ecc: ExecutionContext): Sink[T, R] =
    new SinkImpl[T, R] {
      override def ec: ExecutionContext = ecc

      private val pusherFuture = async {
        val nextSink = fastAwait(future)
        val pusher = Source.pusher[T]()
        (pusher >>| nextSink) recover {
          case NonFatal(e) =>
            failSink(e, true)
        }
        pusher
      }

      override protected def failSink(e: Throwable, internal: Boolean): Unit = async {
        super.failSink(e, internal)
        val nextSink = fastAwait(future)
        nextSink.onError(e)
      }

      override protected def process(input: Option[T]): Future[Boolean] = async {
        val pusher = fastAwait(pusherFuture)
        fastAwait(pusher.push(input))

        // Make sure this Sink doesn't complete before the target Sink does, so users can await
        // (source >>| Sink.flatten(future)) safely
        if (input.isEmpty) {
          val nextSink = fastAwait(future)
          resultPromise.trySuccess(fastAwait(nextSink.resultOnDone))
        }

        input.isEmpty
      }
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

  /** Sends each input element to some one of the `sinks` given. The stream is parallelized upto the number of `sinks`.
    *
    * The order in which elements are distributed to sinks is undefined, except that as long as at least one sink
    * is able to accept an element, the returned scatterer sink will forward it and not wait for a different sink.
    *
    * When an EOF or an upstream error is received, it is forwarded to all sinks.
    *
    * The returned sink doesn't complete until all of the original sinks have completed.
    */
  def scatter[T](sinks: IndexedSeq[Sink[T, _]])(implicit ecc: ExecutionContext): Sink[T, Unit] =
    new WithoutResult[T] {
      override def ec: ExecutionContext = ecc

      private val queue = new AsyncQueue[Pusher[T]]()

      for (sink <- sinks) {
        val pusher = Source.pusher[T]() named s"pusher to ${sink.name}"

        // Fire and forget
        (pusher >>| sink) recover {
          case NonFatal(e) => failSink(e, true)
        }

        queue.enqueue(pusher)
      }

      override protected def process(input: Option[T]): Future[Boolean] = async {
        input match {
          case some: Some[_] =>
            val pusher = fastAwait(queue.dequeue())
            logger.trace(s"Scattering to ${pusher.name}")
            pusher.push(some) map (_ => queue.enqueue(pusher)) recover {
              case NonFatal(e) => failSink(e, true)
            }
            false
          case None =>
            logger.trace(s"Scattering EOF")

            var count = sinks.size
            while (count > 0) {
              val pusher = fastAwait(queue.dequeue())
              logger.trace(s"Scattering EOF to ${pusher.name}")
              fastAwait(pusher.push(None))
              count -= 1
            }

            logger.trace(s"Scattered EOF to all pushers, waiting for sinks to terminate")

            // Wait for all the original sinks to complete
            fastAwait(Future.sequence(sinks map (_.onSinkDone)))

            logger.trace(s"All sinks completed")

            true
        }
      }

      override def onError(cause: Throwable): Unit = {
        super.onError(cause)
        for (sink <- sinks) {
          try {
            sink.onError(cause)
          }
          catch {
            case NonFatal(e) =>
              logger.error(s"Error in downstream sink's `onError`: $e")
          }
        }
      }
    } named "Sink.scatter"

  /** Returns a sink that distributes its input to all of these sinks.
    *
    * The returned sink does not complete until all of the component sinks have completed. */
  def duplicate[T](sinks: Traversable[Sink[T, _]])(implicit ec: ExecutionContext): Sink[T, Unit] = {
    val pushers = sinks map { sink =>
        val pusher = Source.pusher[T]()
        pusher >>| sink
        pusher
    }

    Sink.foreachInputM[T] {
      case some @ Some(t) =>
        Future.sequence(pushers.map(_.push(some))) map (_ => ())
      case None => async {
        fastAwait(Future.sequence(pushers.map(_.push(None))) map (_ => ()))
        // And explicitly wait for the underlying sinks to complete
        fastAwait(Future.sequence(sinks map (_.onSinkDone)))
      }

    }.named("Sink.duplicate")
  }

  /** A sink that never requests any data and never completes, unless failed by its source. */
  def block[T]()(implicit ecc: ExecutionContext) : Sink[T, Unit] = new Sink[T, Unit] {
    private val donePromise = Promise[Unit]()
    private val sub = new AtomicReference[Option[Subscription]](None)

    override def ec: ExecutionContext = ecc
    override def result: Future[Unit] = success

    override def onSinkDone: Future[Unit] = donePromise.future

    override def cancelSubscription(): Unit = {
      sub.getAndSet(None) match {
        case Some(sub) =>
          sub.cancel()
        case None =>
      }
    }

    override def onError(cause: Throwable): Unit = donePromise.tryFailure(cause)
    override def onSubscribe(subscription: Subscription): Unit = sub.set(Some(subscription))
    override def onComplete(): Unit = ()
    override def onNext(element: T): Unit = ()
  }
}
