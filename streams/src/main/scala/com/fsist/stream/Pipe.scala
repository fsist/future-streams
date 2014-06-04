package com.fsist.stream

import com.fsist.util.{CancelToken, BoundedAsyncQueue, AsyncQueue}
import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Subscriber, Publisher, Subscription}
import scala.Some
import scala.async.Async._
import scala.concurrent.{Promise, Future, ExecutionContext}
import com.fsist.stream.PipeSegment.{Passthrough, WithoutResult}
import scala.util.control.NonFatal

/** A Pipe is a combination of Source and Sink. A Pipe usually does some sort of transformation or intermediate
  * calculation, but it can be any combination of Source and Sink.
  *
  *
  */
trait Pipe[A, B, R] extends Sink[A, R] with Source[B] {

  /** Joins two pipes so the output of `this` becomes the input of `next`, and returns a pipe representing both. */
  def |>[C, R2](next: Pipe[B, C, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Pipe[A, C, (R, R2)] = {
    val prev = this
    new Pipe[A, C, (R, R2)] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      prev.subscribe(next)

      override def onSinkDone: Future[Unit] = Future.sequence(Seq(prev.onSinkDone, next.onSinkDone)) map (_ => ())

      override def onSourceDone: Future[Unit] = Future.sequence(Seq(prev.onSourceDone, next.onSourceDone)) map (_ => ())
      override def subscribe(subscriber: Subscriber[C]): Unit = next.subscribe(subscriber)
      override def getSubscriber: Subscriber[A] = prev.getSubscriber
      override def onError(cause: Throwable): Unit = prev.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = prev.onSubscribe(subscription)
      override def onComplete(): Unit = prev.onComplete()
      override def onNext(element: A): Unit = prev.onNext(element)
      override def produceTo(consumer: Consumer[C]): Unit = next.produceTo(consumer)
      override def getPublisher: Publisher[C] = next.getPublisher
      override def result: Future[(R, R2)] = async {
        val prevr = await(prev.result)
        val nextr = await(next.result)
        (prevr, nextr)
      }
      override def subscriber: Option[Subscriber[C]] = next.subscriber
    } named s"${prev.name} >> ${next.name}"
  }

  /** Like `>>`, but discards the result of the original Pipe.
    * TODO once we support sync combinations, use those on `>>` instead of a separate implementation
    */
  def |>>[C, R2](next: Pipe[B, C, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Pipe[A, C, R2] = {
    val prev = this
    new Pipe[A, C, R2] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      prev.subscribe(next)

      override def onSinkDone: Future[Unit] = next.onSinkDone
      override def onSourceDone: Future[Unit] = Future.sequence(Seq(prev.onSourceDone, next.onSourceDone)) map (_ => ())
      override def subscribe(subscriber: Subscriber[C]): Unit = next.subscribe(subscriber)
      override def getSubscriber: Subscriber[A] = prev.getSubscriber
      override def onError(cause: Throwable): Unit = prev.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = prev.onSubscribe(subscription)
      override def onComplete(): Unit = prev.onComplete()
      override def onNext(element: A): Unit = prev.onNext(element)
      override def produceTo(consumer: Consumer[C]): Unit = next.produceTo(consumer)
      override def getPublisher: Publisher[C] = next.getPublisher
      override def result: Future[R2] = next.result
      override def subscriber: Option[Subscriber[C]] = next.subscriber
    } named s"${prev.name} |>> ${next.name}"
  }

  /** Combines a pipe with a sink and returns a sink representing both. */
  def |>[R2](next: Sink[B, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Sink[A, (R, R2)] = {
    val prev = this
    new Sink[A, (R, R2)] {
      prev.subscribe(next)

      override def onSinkDone: Future[Unit] = Future.sequence(Seq(prev.onSinkDone, next.onSinkDone)) map (_ => ())
      override def getSubscriber: Subscriber[A] = prev.getSubscriber
      override def onError(cause: Throwable): Unit = prev.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = prev.onSubscribe(subscription)
      override def onComplete(): Unit = prev.onComplete()
      override def onNext(element: A): Unit = prev.onNext(element)
      override def result: Future[(R, R2)] = async {
        val left = await(prev.result)
        val right = await(next.result)
        (left, right)
      }
      override def ec: ExecutionContext = ecc
    } named s"${prev.name} >> ${next.name}"
  }

  /** Like `>>`, but discards the result of the original Pipe.
    * TODO once we support sync combinations, use those on `>>` instead of a separate implementation
    */
  def |>>[R2](next: Sink[B, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Sink[A, R2] = {
    val prev = this
    new Sink[A, R2] {
      prev.subscribe(next)

      override def onSinkDone: Future[Unit] = next.onSinkDone
      override def getSubscriber: Subscriber[A] = prev.getSubscriber
      override def onError(cause: Throwable): Unit = prev.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = prev.onSubscribe(subscription)
      override def onComplete(): Unit = prev.onComplete()
      override def onNext(element: A): Unit = prev.onNext(element)
      override def result: Future[R2] = next.result
      override def ec: ExecutionContext = ecc
    } named s"${prev.name} |>> ${next.name}"
  }

  /** Creates a wrapper Pipe that maps the result of the original Pipe.
    *
    * This wrapper is not a separate Pipe; both pipes will have the same subscribers and see the same input.
    */
  def mapResultPipe[R2](func: R => R2): Pipe[A, B, R2] = {
    val orig = this
    val mappedResult = orig.result.map(func)(ec) // Schedule `func` run even if noone accesses the returned Sink.result
    new Pipe[A, B, R2] {
      override def result: Future[R2] = mappedResult
      override def onSinkDone: Future[Unit] = orig.onSinkDone
      override def cancelToken: CancelToken = orig.cancelToken
      override def onSourceDone: Future[Unit] = orig.onSourceDone
      override def ec: ExecutionContext = orig.ec
      override def subscribe(subscriber: Subscriber[B]): Unit = orig.subscribe(subscriber)
      override def produceTo(consumer: Consumer[B]): Unit = orig.produceTo(consumer)
      override def getPublisher: Publisher[B] = orig.getPublisher
      override def getSubscriber: Subscriber[A] = orig.getSubscriber
      override def onError(cause: Throwable): Unit = orig.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)
      override def onComplete(): Unit = orig.onComplete()
      override def onNext(element: A): Unit = orig.onNext(element)
      override def subscriber: Option[Subscriber[B]] = orig.subscriber
    }
  } named s"$name.mapResultPipe"

  /** Creates a wrapper Pipe that maps the result of the original Pipe.
    *
    * This wrapper is not a separate Pipe; both pipes will have the same subscribers and see the same input.
    */
  def flatMapResultPipe[R2](func: R => Future[R2]): Pipe[A, B, R2] = {
    val orig = this
    val mappedResult = orig.result.flatMap(func)(ec) // Schedule `func` run even if noone accesses the returned Sink.result
    new Pipe[A, B, R2] {
      override def result: Future[R2] = mappedResult
      override def onSinkDone: Future[Unit] = orig.onSinkDone
      override def cancelToken: CancelToken = orig.cancelToken
      override def onSourceDone: Future[Unit] = orig.onSourceDone
      override def ec: ExecutionContext = orig.ec
      override def subscribe(subscriber: Subscriber[B]): Unit = orig.subscribe(subscriber)
      override def produceTo(consumer: Consumer[B]): Unit = orig.produceTo(consumer)
      override def getPublisher: Publisher[B] = orig.getPublisher
      override def getSubscriber: Subscriber[A] = orig.getSubscriber
      override def onError(cause: Throwable): Unit = orig.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = orig.onSubscribe(subscription)
      override def onComplete(): Unit = orig.onComplete()
      override def onNext(element: A): Unit = orig.onNext(element)
      override def subscriber: Option[Subscriber[B]] = orig.subscriber
    }
  } named s"$name.flatMapResultPipe"
}

object Pipe {
  /** Maps input to output elements. None means end of input.
    *
    * `f` is treated as non reentrant; it will not be called again before the last future it returned completes.
    */
  def flatMapInput[A, B](f: Option[A] => Future[Option[B]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    new PipeSegment.WithoutResult[A, B] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel
      override protected def process(input: Option[A]): Future[Boolean] = f(input) flatMap(emit) map (_ => input.isEmpty)
      override def subscriber: Option[Subscriber[B]] = ???
    } named "Pipe.flatMapInput"

  /** Maps input to output elements. EOF is not represented explicitly.
    *
    * `f` is treated as non reentrant; it will not be called again before it completes.
    */
  def flatMap[A, B](f: A => Future[B])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      case Some(a) => f(a) map Some.apply
      case None => Future.successful(None)
    } named "Pipe.flatMap"


  /** Maps input to output elements.
    *
    * None means end of input.
    *
    * `f` is treated as non reentrant; it will not be called again before it completes.
    */
  def mapInput[A, B](f: Option[A] => Option[B])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      x => Future(f(x))
    } named "Pipe.mapInput"

  /** Maps input to output elements.
    *
    * None means end of input.
    *
    * `f` is treated as non reentrant; it will not be called again before it completes.
    */
  def map[A, B](f: A => B)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      case Some(a) => Future(Some(f(a)))
      case None => Future.successful(None)
    } named "Pipe.map"

  trait Blocked {
    def unblock(): Unit
  }

  /** Builds a no-op pipe that will be paused at first, not letting elements through, until unblock() is called. */
  def blocker[A]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, A, Unit] with Blocked =
    new PipeSegment[A, A, Unit] with Blocked {
      resultPromise.success(())

      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private val pause = Promise[Unit]()
      override def unblock(): Unit = pause.trySuccess(())

      override protected def process(t: Option[A]): Future[Boolean] = {
        (if (pause.isCompleted) emit(t)
        else pause.future flatMap { _ => emit(t)}) map (_ => t.isEmpty)
      }
    } named "Pipe.blocker"

  /** A pipe that produces the first element of the stream as its result, and passes the stream along unmodified
    * (including the first element). The result is None if the stream was empty. */
  def tapOne[T]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, T, Option[T]] =

    new PipeSegment[T, T, Option[T]] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      override protected def process(input: Option[T]): Future[Boolean] = {
        resultPromise.trySuccess(input)
        emit(input) map (_ => input.isEmpty)
      }
    } named "Pipe.tapOne"

  /** A pipe segment that logs each stream element using `logger.trace` without modifying it. Useful for debugging. */
  def log[T](clue: String)()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, T, Unit] =
    new PipeSegment.WithoutResult[T, T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel
      override protected def process(t: Option[T]): Future[Boolean] = {
        logger.trace(s"$clue: $t")
        emit(t) map (_ => t.isEmpty)
      }
    } named s"Pipe.log($clue)"

  trait NoEof[T] extends Pipe[T, T, Unit] {
    /** Causes the pipe to send an EOF token and close (no longer accept data from other sources).
      *
      * MUST NOT be called if another Source may send an unrelated data item concurrently.
      */
    def close(): Future[Unit]
  }

  /** A pipe that passes elements through but discards EOF tokens, until you call `close` on it. */
  def noEof[T]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : NoEof[T] =
    new WithoutResult[T, T] with NoEof[T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      @volatile private var closed : Boolean = false

      override def close(): Future[Unit] = async {
        closed = true
        // TODO may be buggy: a Sink isn't supposed to support re-subscription
        Source() >>| this
      }

      override protected def mayDiverge: Boolean = true

      override protected def process(input: Option[T]): Future[Boolean] = async {input match {
        case some @ Some(t) =>
          await(emit(some))
          false
        case None =>
          if (closed) {
            val _ = await(emit(None)) // For some reason the val _ is needed to silence an exception
            true
          }
          else {
            logger.trace(s"Swallowing EOF")
            false
          }
      }
      }
    } named "Pipe.noEof"

  /** @return a pipe that passes through all data unmodified and a new Source that produces the same data that passes
    *         through the pipe. */
  def tap[T]()(implicit ecc: ExecutionContext, cancel: CancelToken): (Pipe[T, T, Unit], Source[T]) = {
    val queue = new BoundedAsyncQueue[Option[T]](1)

    val source = new SourceImpl[T] {
      override def cancelToken: CancelToken = cancel
      override def ec: ExecutionContext = ecc
      override protected def produce(): Future[Option[T]] = queue.dequeue()
    }

    val pipe = new PipeSegment.WithoutResult[T, T] {
      override def cancelToken: CancelToken = cancel
      override def ec: ExecutionContext = ecc
      override protected def process(input: Option[T]): Future[Boolean] = async {
        await(Future.sequence(Seq(queue.enqueue(input), emit(input))))
        input.isEmpty
      }
    }

    (pipe, source)
  }
}

/** A base implementation of a Pipe that decouples input and output: there doesn't have to be a correspondence between
  * input and output elements.
  *
  * The implementation should provide the `process` method, which obeys the contract of [[SinkImpl.process]],
  * and can call the method `emit` defined here.
  */
trait PipeSegment[A, B, R] extends Pipe[A, B, R] with SourceImpl[B] with SinkImpl[A, R] {
  override implicit def ec: ExecutionContext // Declared to reconcile the identical declarations from SourceImpl and SinkImpl

  private val outbox = new BoundedAsyncQueue[Option[B]](1)

  def produce(): Future[Option[B]] = outbox.dequeue()

  // Pass errors downstream
  override protected def failSink(e: Throwable, internal: Boolean): Unit = {
    super.failSink(e, internal)
    failSource(e)
  }

  /** Emits an item. The proccesor must wait for the returned future to complete, meaning the item was emitted,
    * before doing more processing or emitting more items. */
  protected def emit(output: Option[B]): Future[Unit] = {
//        logger.trace(s"Emitting $output")
    outbox.enqueue(output)
  }

  /** Convenience overload that emits a sequence of items. */
  protected def emit(outputs: TraversableOnce[Option[B]]) : Future[Unit] = {
    outputs.foldLeft(success){
      case (fut, next) => fut flatMap (_ => emit(next))
    }
  }
}

object PipeSegment {

  /** A speciailization of PipeSegment that doens't calculate a result R. */
  trait WithoutResult[A, B] extends PipeSegment[A, B, Unit] {
    // The result completes right away
    resultPromise.success(())
  }

  /** A pipe segment that does not modify its input in any way and does not calculate any result.
    *
    * This is a building block for child classes, and is not useful on its own. It could be used to build e.g. a buffering
    * pipe component.
    */
  trait Passthrough[T] extends WithoutResult[T, T] {
    override def process(input: Option[T]): Future[Boolean] = {
      emit(input) map (_ => input.isEmpty)
    }
  }

  object Passthrough {
    /** A pipe segment that does not modify its input in any way and does not calculate any result. */
    def apply[T]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Passthrough[T] =
      new Passthrough[T] {
        override def ec: ExecutionContext = ecc
        override def cancelToken: CancelToken = cancel
      }
  }

}