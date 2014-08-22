package com.fsist.stream

import com.fsist.stream.PipeSegment.Passthrough
import com.fsist.util.FastAsync._
import com.fsist.util.concurrent.{BoundedAsyncQueue, CancelToken, CanceledException}
import org.reactivestreams.api.{Consumer, Processor}
import org.reactivestreams.spi.{Publisher, Subscriber, Subscription}

import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future, Promise}

/** A Pipe is a combination of a [[Source]] and a [[Sink]]. It usually does some sort of transformation or intermediate
  * calculation, but it can be any combination of Source and Sink.
  *
  * Instances can be created using methods on the companion object, such as `map`. They can also be implemented as
  * Future-based state machines by extending [[PipeSegment]].
  */
trait Pipe[A, B, R] extends Sink[A, R] with Source[B] with Processor[A, B] {

  /** Joins two pipes so the output of `this` becomes the input of `next`, and returns a pipe representing both.
    *
    * This is different from `this >> next` (treating `this` as a Source) in that the returned Pipe has as its result
    * the combined results of `this` and `next`.
    *
    * Note on subscriptions: subscribing the returned pipe to a Source is identical to subscribing the original `this`
    * pipe to that source. Subscribing a Sink to the returned pipe is identical to subscribe it to the orginal `next`
    * pipe.
    */
  def |>[C, R2](next: Pipe[B, C, R2]): Pipe[A, C, (R, R2)] = {
    val prev = this
    new Pipe[A, C, (R, R2)] {
      override implicit def ec: ExecutionContext = prev.ec
      override implicit def cancelToken: CancelToken = prev.cancelToken

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
        val prevr = fastAwait(prev.result)
        val nextr = fastAwait(next.result)
        (prevr, nextr)
      }
      override def subscriber: Option[Subscriber[C]] = next.subscriber
      override def cancelSubscription(): Unit = prev.cancelSubscription()
    } named s"${prev.name} >> ${next.name}"
  }

  /** Subscribes a sink to `this` pipe and returns a new sink representing both.
    *
    * This is different from `this >>| sink; sink` in that the result of the returned sink is the combined result
    * of both `this` original pipe and the original `sink`.
    *
    * Note on subscriptions: subscribing the returned pipe to a Source is identical to subscribing the original `this`
    * pipe to that source.
    */
  def |>[R2](sink: Sink[B, R2]): Sink[A, (R, R2)] = {
    val prev = this
    new Sink[A, (R, R2)] {
      prev.subscribe(sink)

      override def onSinkDone: Future[Unit] = Future.sequence(Seq(prev.onSinkDone, sink.onSinkDone)) map (_ => ())
      override def getSubscriber: Subscriber[A] = prev.getSubscriber
      override def onError(cause: Throwable): Unit = prev.onError(cause)
      override def onSubscribe(subscription: Subscription): Unit = prev.onSubscribe(subscription)
      override def onComplete(): Unit = prev.onComplete()
      override def onNext(element: A): Unit = prev.onNext(element)
      override def result: Future[(R, R2)] = async {
        val left = fastAwait(prev.result)
        val right = fastAwait(sink.result)
        (left, right)
      }
      override implicit def ec: ExecutionContext = prev.ec
      override def cancelSubscription(): Unit = prev.cancelSubscription()
    } named s"${prev.name} >> ${sink.name}"
  }

  /** Like `|>`, but discards the result of the original Pipe.
    */
  def |>>[C, R2](next: Pipe[B, C, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Pipe[A, C, R2] =
    this |> next mapResultPipe (_._2)

  /** Like `|>`, but discards the result of the original Sink.
    */
  def |>>[R2](next: Sink[B, R2])(implicit ecc: ExecutionContext, cancel: CancelToken = cancelToken): Sink[A, R2] =
    this |> next mapResult (_._2)

  /** Creates a wrapper Pipe that maps the result of the original Pipe.
    *
    * This wrapper is not a separate Pipe; both pipes will have the same subscribers and see the same input.
    *
    * This method is named `mapResultPipe` to distinguish it from the future method `Source.map` and the current
    * method `Sink.mapResult` (which would return only a Sink and not a Pipe).
    */
  def mapResultPipe[R2](func: R => R2): Pipe[A, B, R2] = {
    val orig = this
    val mappedResult = async {
      val result = fastAwait(orig.result)
      func(result) // Schedule `func` run even if noone accesses the returned Sink.result
    }

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
      override def cancelSubscription(): Unit = orig.cancelSubscription()
    }
  } named s"$name.mapResultPipe"

  /** Creates a wrapper Pipe that maps the result of the original Pipe.
    *
    * This wrapper is not a separate Pipe; both pipes will have the same subscribers and see the same input.
    *
    * This method is named `flatMapResultPipe` to distinguish it from the future method `Source.flatMap` and the current
    * method `Sink.flatMapResult` (which would return only a Sink and not a Pipe).
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
      override def cancelSubscription(): Unit = orig.cancelSubscription()
    }
  } named s"$name.flatMapResultPipe"
}

/** Contains factory methods for creating [[Pipe]] instances.
  *
  * In the various families of methods (mapXXX, etc), an `Input` suffix denotes a method
  * that receives an `Option[T]` rather than a raw `T`; this lets it see EOF tokens explicitly.
  */
object Pipe {
  /** Maps input to output elements. None means end of input. `f` is called non concurrently. */
  def flatMapInput[A, B](f: Option[A] => Future[Option[B]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    new PipeSegment.WithoutResult[A, B] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel
      override protected def process(input: Option[A]): Future[Boolean] = async {
        val p = fastAwait(f(input))
        fastAwait(emit(p))
        input.isEmpty
      }
    } named "Pipe.flatMapInput"

  /** Maps input to output elements. EOF is not represented explicitly. `f` is called non concurrently.
    */
  def flatMap[A, B](f: A => Future[B])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      case Some(a) => async {
        Some(fastAwait(f(a)))
      }
      case None => noneFuture
    } named "Pipe.flatMap"


  /** Maps input to output elements. None means end of input. `f` is called non concurrently.
    */
  def mapInput[A, B](f: Option[A] => Option[B])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      x => Future(f(x))
    } named "Pipe.mapInput"

  /** Maps input to output elements. None means end of input. `f` is called non concurrently.
    */
  def map[A, B](f: A => B)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, B, Unit] =
    flatMapInput[A, B] {
      case Some(a) => Future(Some(f(a)))
      case None => noneFuture
    } named "Pipe.map"

  /** A pipe that does not pass elements until `unblock` is called. */
  trait Blocked {
    def unblock(): Unit
  }

  /** Returns a no-op pipe that will be paused at first, not letting elements through, until unblock() is called. */
  def blocker[A]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[A, A, Unit] with Blocked =
    new PipeSegment[A, A, Unit] with Blocked {
      resultPromise.success(())

      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private val pause = Promise[Unit]()
      override def unblock(): Unit = pause.trySuccess(())

      override protected def process(t: Option[A]): Future[Boolean] = async {
        fastAwait(pause.future)
        fastAwait(emit(t))
        t.isEmpty
      }
    } named "Pipe.blocker"

  /** Returns a pipe that produces the first element of the stream as its result, and passes the stream along unmodified
    * (including the first element). The result is None if the stream was empty. */
  def tapOne[T]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, T, Option[T]] =

    new PipeSegment[T, T, Option[T]] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      override protected def process(input: Option[T]): Future[Boolean] = async {
        resultPromise.trySuccess(input)
        fastAwait(emit(input))
        input.isEmpty
      }
    } named "Pipe.tapOne"

  /** Returns a pipe that logs each stream element using `logger.trace` without modifying it. Useful for debugging. */
  def log[T](clue: String)()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, T, Unit] =
    new PipeSegment.WithoutResult[T, T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel
      override protected def process(t: Option[T]): Future[Boolean] = async {
        logger.trace(s"$clue: $t")
        fastAwait(emit(t))
        t.isEmpty
      }
    } named s"Pipe.log($clue)"

  /** @return a pipe that passes through all data unmodified and a new Source that produces the same data that passes
    *         through the pipe. This is a building block for circumventing the singlecast restriction of Source. */
  def tap[T]()(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): (Pipe[T, T, Unit], Source[T]) = {
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
        fastAwait(Future.sequence(Seq(queue.enqueue(input), emit(input))))
        input.isEmpty
      }
    }

    (pipe, source)
  }

  /** Creates a pipe that buffers up to `bufSize` elements, which can increase efficiency when the soure and/or sink are
    * not CPU-bound. */
  def buffer[T](bufSize: Int)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, T, Unit] =
    new Passthrough[T] {
      override protected def bufferSize: Int = bufSize
      override implicit def cancelToken: CancelToken = cancel
      override implicit def ec: ExecutionContext = ecc
    }

  /** Converts a Future[Pipe] to a Pipe that will start passing elements after the `future` completes. */
  def flatten[T, S, R](futurePipe: Future[Pipe[T, S, R]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Pipe[T, S, R] =
    new Pipe[T, S, R] with SinkImpl[T, R] with SourceImpl[S] {
      override def cancelToken: CancelToken = cancel
      override def ec: ExecutionContext = ecc

      private val pusher = Source.pusher[T]()
      private val puller = Sink.puller[S]()

      private val build = async {
        val inner = fastAwait(futurePipe)
        pusher >> inner >>| puller
        resultPromise.completeWith(inner.result)
      }

      override protected def process(input: Option[T]): Future[Boolean] = async {
        fastAwait(build) // Fail if the original future failed
        fastAwait(pusher.push(input))

        if (input.isEmpty) fastAwait(result) // Before returning 'true' the resultPromise must be completed by the inner pipe
        input.isEmpty
      }

      override protected def produce(): Future[Option[S]] = async {
        fastAwait(build) // Fail if the original future failed
        fastAwait(puller.pull())
      }
    } named "Pipe.flatten"
}

/** Base for Future-based mutable state machine implementations of [[Pipe]].
  *
  * Concrete implemenetations need to supply the `process` method. It has the same signature and requirements as
  * `SinkImpl.process`, but can additionally use the `emit` method to produce elements to output. These elements don't
  * need to correspond in quantity or EOF-ness to the input elements.
  *
  * Note well the calling requirements in the doc comment of `emit`.
  */
trait PipeSegment[A, B, R] extends Pipe[A, B, R] with SourceImpl[B] with SinkImpl[A, R] {
  override implicit def ec: ExecutionContext // Declared to reconcile the identical declarations from SourceImpl and SinkImpl

  protected def bufferSize: Int = 1
  private val outbox = new BoundedAsyncQueue[Option[B]](bufferSize)

  /** @see `SinkImpl.produce` and `PipeSegment.emit`. */
  def produce(): Future[Option[B]] = outbox.dequeue()

  // Pass errors downstream
  override protected def failSink(e: Throwable, internal: Boolean): Unit = {
    super.failSink(e, internal)
    failSource(e)
  }

  cancelToken.future map { _ =>
    if (! onSinkDone.isCompleted) {
      logger.trace(s"PipeSegment was canceled")

      // SourceImpl already cancels its part, we need to cancel SinkImpl only
      super.failSink(new CanceledException(), true)
    }
  }

  /** Emits an item to the output of this pipe. DO NOTE the following requirements:
    *
    * 1. This method MUST be called non-concurrently.
    * 2. This method MUST be called only from within `produce` or from inside a Future returned by `produce`.
    * 3. The future returned by `produce` MUST NOT complete before all the calls to `emit` it started have completed.
    */
  protected def emit(output: Option[B]): Future[Unit] = {
    //        logger.trace(s"Emitting $output")
    outbox.enqueue(output)
  }

  /** Convenience overload that emits a sequence of items. */
  protected def emit(outputs: TraversableOnce[Option[B]]): Future[Unit] = {
    outputs.foldLeft(success) {
      case (fut, next) => async {
        fastAwait(fut)
        fastAwait(emit(next))
      }
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
    override def process(input: Option[T]): Future[Boolean] = async {
      fastAwait(emit(input))
      input.isEmpty
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