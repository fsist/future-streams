package com.fsist.stream

import com.fsist.util.FutureOps._
import com.fsist.util.{BugException, AsyncQueue}
import org.reactivestreams.spi.{Subscriber, Subscription}
import scala.async.Async._
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.util.control.NonFatal

/** Base implementation of Sink. Concrete implementations need to supply the `process` method, and can optionally
  * override `onComplete` and/or `onError`. Instances can also be created using the factory methods on [[Sink]].
  *
  * To keep the design simple, there is a write-once cell for the subscription. The methods onError, onComplete
  * and onNext will only be called when that cell has been set. A nicer design might have separate types, one without
  * the subscription and one with, but that conflicts with the ReactiveStreams API design.
  */
trait SinkImpl[T, R] extends Sink[T, R] {
  implicit def ec: ExecutionContext

  /** Set exactly once when we are subscribed. A Sink doesn't support resubscribing to a different Source. */
  @volatile private var subscription: Subscription = _

  private val buffer: AsyncQueue[Option[T]] = new AsyncQueue[Option[T]]()

  /** This promise's future is exposed by our implementation of `result`. The implementation of `process()` should complete
    * this future as soon as the result is ready, even if the sink doesn't choose to stop yet (i.e. even if `process()`
    * keeps returning `false`. */
  protected val resultPromise: Promise[R] = Promise[R]()

  override def result: Future[R] = resultPromise.future

  /** This is signalled once the sink has completed all processing and corresponds to `onSinkDone`.
    *
    * It is set by code in this class only; it is deliberately hidden from subclasses. Subclasses should influence this
    * process only by returning true or false from `process`.
    */
  private val sinkDonePromise: Promise[Unit] = Promise[Unit]()

  /** Start running the state machine when this value is first accessed. This normally happens on the first subscription.
    *
    * Note that the val type is Unit. We don't want to preserve the original Future returned by the first invocation of
    * nextStep, we just want the lazy val logic.
    */
  private[this] lazy val started: Unit =
  // Start on the EC, not on the thread that accessed the lazy val!
  // This creates a Futuer[Future[Unit]], but we don't care.
    Future(nextStep() recover {
      case NonFatal(e) => failSink(e, true)
    })(ec)

  /** This central method is called whenever the sink fails irreversibly due to an error in `process` or, theoretically,
    * an error or bug in the code that wraps `process` such as `nextStep`. It exists to let subclasses override it
    * and so be notified about errors. Overriding impelmentations SHOULD call this method.
    *
    * This method is also called by the default implementation of `onError`.
    *
    * @param internal if true, the error occurred in this sink. If false, it came from an upstream Source via `onError`.
    */
  protected def failSink(e: Throwable, internal: Boolean): Unit = {
    if (internal) {
      logger.error(s"Processing failed: $e")
    }
    else {
      logger.error(s"Failing due to upstream error: $e")
    }

    if (!resultPromise.tryFailure(e)) {
      logger.error(s"Result promise already completed, error may be ignored: $e")
    }
    if (!sinkDonePromise.tryFailure(e)) {
      logger.error(s"onSinkDone promise already completed, error will be ignored: $e")
    }
  }

  /** Returns a future that is completed when the sink receives end-of-input and finishes all processing. */
  def onSinkDone: Future[Unit] = sinkDonePromise.future //flatMap (_ => resultPromise.future)

  /** If overridden to be true, we don't log warnings about a Sink continuing when its Source has completed. */
  protected def mayDiverge: Boolean = false

  private def nextStep(): Future[Unit] = async {
    logger.trace(s"Dequeueing input...")
    // TODO make cancelable, eg in case of async call to onError
    val input = await(buffer.dequeue())
    //    logger.trace(s"Processing $input")

    val isDone = await(try {
      process(input).toTry
    }
    catch {
      case NonFatal(e) =>
        logger.error(s"process($input) failed with $e")
        Future.failed(e)
    })
    //    logger.trace(s"process($input) produced $isDone")

    if (isDone.isFailure) {
      logger.trace(s"Failed")
      cancelSubscription()
      throw isDone.failed.get
    }
    else if (isDone.get) {
      // EOF
      logger.trace(s"Sink is done")
      sinkDonePromise.success(())

      if (!resultPromise.isCompleted) {
        val err = s"Implementation of process() did not complete the resultPromise before returning `true`. " +
          s"This is a bug. Forcing the resultPromise to fail."
        logger.error(err)
        resultPromise.tryFailure(BugException(err))
      }
    }
    else if (input.isDefined) {
      logger.trace(s"Requesting more input")
      requestMore(1)
      await(nextStep())
    }
    else {
      if (! mayDiverge) logger.warn(s"Divergence: the Source has completed, but process() expects more input")
      // Allow ourselves to be resubscribed to another source
      await(nextStep())
    }
  }

  /** Cancel our linked subscription (if any). */
  def cancelSubscription(): Unit = {
    subscription match {
      case null =>
      case sub => sub.cancel()
    }
  }

  private def requestMore(n: Int): Unit = subscription.requestMore(n)

  def onSubscribe(subscription: Subscription): Unit = {
    // Make sure the state machine is running
    started

    this.subscription = subscription
    subscription.requestMore(1)
  }

  /** By default, logs the error; can be overridden. */
  def onError(cause: Throwable): Unit = {
    failSink(cause, false)
  }

  def onComplete(): Unit = {
    logger.trace(s"Enqueueing end-of-input signal")
    buffer.enqueue(None)
  }
  def onNext(element: T): Unit = {
    logger.trace(s"Enqueuing input element")
    buffer.enqueue(Some(element))
  }

  def getSubscriber: Subscriber[T] = this

  /** Called to process the next element. This always runs on the `ec` ExecutionContext, so it can be a synchronous
    * implementation if desired.
    *
    * Treated as non-reentrant; guaranteed not to be called again until the future returned from the previous call has
    * completed.
    *
    * None is passed in to signal the end of input.
    *
    * The implementation MUST complete `resultPromise` deterministically at some point after a future it returns
    * completes with `true`. Otherwise, onSinkDone will not complete either, since it exposes the same result.
    *
    * @return a future whose value can be `true` to signal that the Sink should stop (unsubscribe, call `result` and
    *         complete the `onSinkDone` future) or `false` to signal that the Sink should continue.
    *         If you return `false` after seeing end-of-input (`None`), and the user doesn't resubscribe us to another
    *         Source later, then this Sink will never complete.
    *         If the future returned fails, the Sink is treated as having failed and this function will not be called
    *         again.
    */
  protected def process(input: Option[T]): Future[Boolean]
}

object SinkImpl {

  /** A specialization of SinkImpl which does not calculate a resut. */
  trait WithoutResult[T] extends SinkImpl[T, Unit] {
    resultPromise.success(())
  }

}
