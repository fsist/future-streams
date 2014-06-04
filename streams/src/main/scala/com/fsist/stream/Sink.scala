package com.fsist.stream

import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Subscription, Subscriber}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.Some
import scala.util.control.NonFatal

/** This trait combines the ReactiveStreams SPI Consumer and API Subscriber. Please read the ReactiveStreams
  * documentation.
  *
  * In addition, a Sink can calculate a result of type `R`. This result is available from the future returned by the
  * `onSinkDone` method, which completes once the sink receives end-of-input and also finishes any additional
  * asynchronous processing it may carry out.
  *
  * Sinks which don't calculate a result value substitute `R = Unit`.
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

  /** Creates a new Sink wrapper that asynchronously maps the original sink's result once it is available.
    *
    * NOTE that this does NOT create a *separate* sink. In other words, the original sink and this sink cannot
    * be subscribed to different sources; any calls to subscribe(), onNext(), etc. on either one of them affect
    * both equally.
    *
    * TODO this requires an implicit ExecutionContext, but most of the time we already have a SinkImpl that has
    * an attached EC; how to reuse it then? Should SinkImpl provide an overload of this method without the
    * implicit parameter - is that a legal overload?
    *
    * This is called `mapSink` and not simply `map` to distinguish it from [[Source.map]] on the Pipe.
    */
  def mapSink[K](func: R => K)(implicit ecc: ExecutionContext) : Sink[T, K] = {
    val orig = this
    val mappedResult = orig.result.map(func)(ecc) // Schedule `func` run even if noone accesses the returned Sink.result
    new Sink[T, K] {
      override def result: Future[K] = mappedResult
      override def onSinkDone: Future[Unit] = orig.onSinkDone
      override def onError(cause: Throwable): Unit = orig.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)
      override def onComplete(): Unit = orig.onComplete()
      override def onNext(element: T): Unit = orig.onNext(element)
      override def getSubscriber: Subscriber[T] = orig.getSubscriber
      override def ec: ExecutionContext = ecc
    }
  }

  /** Creates a new Sink wrapper that asynchronously maps the original sink's result once it is available.
    *
    * NOTE that this does NOT create a *separate* sink. In other words, the original sink and this sink cannot
    * be subscribed to different sources; any calls to subscribe(), onNext(), etc. on either one of them affect
    * both equally.
    *
    * This is called `flatMapSink` and not simply `flatMap` to distinguish it from [[Source.flatMap]] on the Pipe.
    */
  def flatMapSink[K](func: R => Future[K])(implicit ecc: ExecutionContext) : Sink[T, K] = {
    val orig = this
    val mappedResult = orig.result.flatMap(func)(ecc) // Schedule `func` run even if noone accesses the returned Sink.result
    new Sink[T, K] {
      // Lazy vals ensure we only call `func` once
      override def result: Future[K] = mappedResult
      override def onSinkDone: Future[Unit] = orig.onSinkDone
      override def onError(cause: Throwable): Unit = orig.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)
      override def onComplete(): Unit = orig.onComplete()
      override def onNext(element: T): Unit = orig.onNext(element)
      override def getSubscriber: Subscriber[T] = orig.getSubscriber
      override def ec: ExecutionContext = ecc
    }
  }

}

object Sink {
  /** Creates a Sink that runs `f` for each input. `f` will only be called again after the previous call completes.
    * It will never be called again if it fails (throws).
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

  /** Creates a Sink that runs `f` for each input. It is called with None to signify EOF.
    *
    * `f` will only be called again after the previous call completes. It will never be called again if it fails (throws).
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

  /** Creates a Sink that runs `f` for each input. It is called with None to signify EOF.
    *
    * `f` will only be called again after the future returned by the previous call completes.
    * The Sink will complete once it receives end-of-input.
    *
    * It will never be called again if it fails (throws or returns a failed future).
    */
  def foreachInputM[T](f: Option[T] => Future[Unit])(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(t: Option[T]): Future[Boolean] = try {
      f(t) map (_ => t.isEmpty)
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreachInputM"

  /** Creates a Sink that runs `f` for each input. `f` will only be called again after the future returned by the
    * previous call completes. The Sink will complete once it receives end-of-input.
    *
    * It will never be called again if it fails (throws or returns a failed future).
    */
  def foreachM[T](f: T => Future[Unit])(implicit ecc: ExecutionContext): Sink[T, Unit] = new SinkImpl.WithoutResult[T] {
    override def ec: ExecutionContext = ecc

    protected def process(input: Option[T]): Future[Boolean] = try {
      (input match {
        case Some(t) => f(t)
        case None => Future.successful(())
      }) map (_ => input.isEmpty)
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  } named "Sink.foreachM"

  /** Creates a sink that discards all input. */
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

  /** Returns a Sink that implements a fold(). The end-of-input is not passed explicitly to the folder. */
  def fold[T, S, R](initialState: S)(folder: (S, T) => S)(finalStep: S => R)(implicit ecc: ExecutionContext): Sink[T, R] = new SinkImpl[T, R] {
    override def ec: ExecutionContext = ecc

    private var state : S = initialState

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

  /** Returns a Sink that implements an asynchronous fold(). The end-of-input is not passed explicitly to the folder. */
  def foldM[T, S, R](initialState: S)(folder: (S, T) => Future[S])(finalStep: S => Future[R])(implicit ecc: ExecutionContext): Sink[T, R] = new SinkImpl[T, R] {
    override def ec: ExecutionContext = ecc

    private var state : S = initialState

    override protected def process(input: Option[T]): Future[Boolean] = {
      input match {
        case Some(t) =>
          folder(state, t) map {newState =>
            state = newState
            false
          }
        case None =>
          finalStep(state) map { result =>
            resultPromise.success(result)
            true
          }
      }
    }
  } named "Sink.foldM"

  /** A Sink which is already done, with a result that is immediately available. It never requests more data from a source. */
  def done[T, R](r: R)(implicit ecc: ExecutionContext) : Sink[T, R] = new Sink[T, R] {
    override def result: Future[R] = Future.successful(r)
    override def onSinkDone: Future[Unit] = success
    override def onError(cause: Throwable): Unit =
      logger.error(s"Notified about error, but we're not subscribed to anything: $cause")
    override def onSubscribe(subscription: Subscription): Unit = ()
    override def onComplete(): Unit = ()
    override def onNext(element: T): Unit = ()
    override def getSubscriber: Subscriber[T] = this
    override def ec: ExecutionContext = ecc
  } named "Sink.done"

  /** Convert a Future[Sink] to an actual Sink. The returned Sink will not request more than one element before the
    * `future` completes. */
  def flatten[T, R](future: Future[Sink[T, R]])(implicit ecc: ExecutionContext) : Sink[T, R] = {
    val forwarder = PipeSegment.Passthrough[T] named "Sink.flatten internal pipe"
    future onSuccess {
      case sink => forwarder >>| sink
    }
    future onFailure {
      case e => forwarder.onError(e)
    }
    forwarder flatMapSink(_ => future flatMap(_.result))
  } named "Sink.flatten"
}
