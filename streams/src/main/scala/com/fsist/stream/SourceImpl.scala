package com.fsist.stream

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream.SourceImpl.{LinkedSubscription, SubInfo, SubscriberInfo, Unsubscribed}
import com.fsist.util.FastAsync._
import com.fsist.util._
import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Publisher, Subscriber, Subscription}

import scala.annotation.tailrec
import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

/** Base for Future-based mutable state machine implementations of [[Source]].
  *
  * Concrete implementations need to supply the `produce` method. This method will be called non-concurrently until
  * it produces EOF or fails.
  *
  * If you have no mutable state to store between calls to `produce`, this can also be achieved more easily and
  * idiomatically using `Source.generateM`.
  */
trait SourceImpl[T] extends Source[T] {

  cancelToken.future map {
    _ =>
      if (done.tryFailure(new CanceledException())) {
        logger.trace(s"Source was canceled")
        val sub = subInfo.get()
        sub.subscriber match {
          case Left(SubscriberInfo(subscriber, _)) => subscriber.onError(new CanceledException())
          case _ =>
        }
      }
      else logger.trace(s"Token was canceled after Source had completed, ignoring")
  }

  def getPublisher: Publisher[T] = this

  def produceTo(consumer: Consumer[T]): Unit = {
    subscribe(consumer.getSubscriber)
  }

  private val subInfo: AtomicReference[SubInfo[T]] = new AtomicReference[SubInfo[T]](SubInfo.none[T])

  /** Must be implemented to produce the next input element to be sent to subscribers. This method is called non-concurrently.
    *
    * If a future returns None, the source will emit EOF. If it fails (or the method throws an exception synchronously),
    * the source will emit an error. In either case the source will be terminated and this method  will never be called again.
    *
    * If this method does any non-trivial processing, it SHOULD do it inside the returned Future and not synchronously.
    */
  protected def produce(): Future[Option[T]]

  private val done: Promise[Unit] = Promise[Unit]()

  def onSourceDone: Future[Unit] = done.future

  def subscriber: Option[Subscriber[T]] = subInfo.get.subscriber match {
    case Left(SubscriberInfo(subscriber, _)) => Some(subscriber)
    case _ => None
  }

  /** This lazy val ensures is forced the first time someone subscribes to this source.
    * The alternative would be to start this future as soon as the source instance is created. However, that has few
    * benefits and can cause unpleasant bugs in subclasses with initialization logic.
    */
  private lazy val started: Unit = {
    logger.trace(s"Starting source")

    // Fire and forget
    Future {
      nextStep()
    } recover { case NonFatal(e) => failSource(e)}
  }

  /** Notify all subscribers of this failure and refuse future subscriptions. */
  protected def failSource(e: Throwable): Unit = {
    if (e.isInstanceOf[CanceledException])
      logger.trace(s"Source canceled")
    else
      logger.error(s"Error in source: $e")

    done.tryFailure(e)
    val prevSub = subInfo.getAndSet(SubInfo.none[T])
    prevSub.subscriber match {
      case Left(SubscriberInfo(sub, _)) => sub.onError(e)
      case _ =>
    }
  }

  private def nextStep(): Future[Unit] = {
    cancelToken.throwIfCanceled

    val fut = try {
      logger.trace(s"Calling produce()")
      produce()
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error in produce(): $e")
        Future.failed(e)
    }
    async {
      fastAwait(fut) match {
        case Some(t) =>
          logger.trace(s"Produced element $t")
          publishToSub(t)
        case None =>
          logger.trace(s"Produced EOF")
          subInfo.get.subscriber match {
            case Left(SubscriberInfo(sub, _)) => sub.onComplete()
            case _ =>
              logger.trace(s"No subscriber to see our EOF")
          }
          done.success(())
          done.future
      }
    }
  }

  private def publishToSub(t: T): Future[Unit] = async {
    val info = subInfo.get // Local copy to avoid races with writers
    info.subscriber match {
      case Left(SubscriberInfo(sub, subscription)) =>
        val subName = if (logger.underlying.isTraceEnabled) {
          if (sub.isInstanceOf[Sink[T, _]])
            sub.asInstanceOf[Sink[T, _]].name
          else
            sub.toString
        }
        else ""

        logger.trace(s"Waiting for subscriber '$subName' to request more data before publishing")

        val canceled = cancelToken.futureFail
        val requested = info.requestedCount.decrement()
        val unsubscribed = subscription.unsubscribed

        // Attempt to short-circuit
        if (requested.isCompleted && requested.value.get.isSuccess) {
          logger.trace(s"Publishing element to subscriber $subName")
          sub.onNext(t)
        }
        else {
          fastAwait(Future.firstCompletedOf(Seq(canceled, requested, unsubscribed))) match {
              // If `canceled` completes first, it will fail with a CanceledException which will be thrown out of here
            case Unsubscribed =>
              logger.trace(s"Subscriber $subName unsubscribed while we waited for it to request more data")
            case () =>
              logger.trace(s"Publishing element to subscriber $subName")
              sub.onNext(t)
          }
        }

        fastAwait(nextStep)

      case Right(promise) =>
        logger.trace(s"Waiting for subscribers before publishing")
        fastAwait(promise.future)
        if (done.isCompleted) {
          fastAwait(done.future)
        }
        else {
          fastAwait(publishToSub(t))
        }
    }
  }

  @tailrec
  final def subscribe(subscriber: Subscriber[T]): Unit =
    if (onSourceDone.isCompleted) {
      onSourceDone.value.get match {
        case Success(_) =>
          val err = "This Source is already completed and cannot be subscribed to."
          logger.error(err)
          subscriber.onError(new IllegalStateException(err))
        case Failure(e) => subscriber.onError(e)
      }
    }
    else {
      val currentSub = subInfo.get
      currentSub.subscriber match {
        case Left(otherSub) =>
          val err = s"This Source already has a subscriber and another cannot be added."
          logger.error(err)
          subscriber.onError(new IllegalStateException(err))
        case Right(promise) =>
          val subscription = new LinkedSubscription(this)
          if (!subInfo.compareAndSet(currentSub, SubInfo(Left(SubscriberInfo(subscriber, subscription))))) {
            // Lost race
            subscribe(subscriber)
          }
          else {
            logger.trace(s"Added subscriber")
            subscriber.onSubscribe(subscription)
            promise.success(())
            started // Force lazy val
          }
      }
    }

  /** Remove this subscription. Equivalent to sub.cancel() */
  @tailrec
  private final def unsubscribe(sub: Subscription): Unit = {
    val currentSub = subInfo.get
    currentSub.subscriber match {
      case Left(SubscriberInfo(_, sub2)) if sub2 eq sub =>
        if (subInfo.compareAndSet(currentSub, SubInfo.none[T])) {
          logger.trace(s"Removed subscriber")
        }
        else {
          // Lost race
          unsubscribe(sub)
        }
      case Left(_) =>
        logger.error(s"Wrong subscription, cannot unsubscribe")
      case _ =>
        logger.error("No subscriber, cannot unsubscribe")
    }
  }

  /** Request more elements for this subscription. Equivalent to sub.requestMore(count) */
  def request(sub: Subscription, count: Int): Unit = {
    // Local copy to prevent races with writer
    val info = subInfo.get
    info.subscriber match {
      case Left(SubscriberInfo(_, sub2)) if sub2 eq sub =>
        logger.trace(s"Subscriber requested $count more")
        info.requestedCount.increment(count)
      case Left(_) =>
        logger.error(s"request() called with wrong Subscription")
      case _ =>
        logger.error(s"request() called but we don't have a subscriber")
    }
  }
}

object SourceImpl {

  /** These are in a class to be replaced atomically together in the SourceImpl instance, when someone subscribes
    * or unsubscribes.
    *
    * @param subscriber either a subscriber, or a promise that will be fulfilled after a subscriber has been
    *                   subscribed and the subInfo field in the class has been changed to a value that includes
    *                   a subscriber.
    * @param requestedCount count of items requested and not yet supplied by the subscriber in this instance.
    */
  private case class SubInfo[T](subscriber: Either[SubscriberInfo[T], Promise[Unit]],
                                requestedCount: AsyncSemaphore)

  private object SubInfo {
    def apply[T](subscriber: Either[SubscriberInfo[T], Promise[Unit]])(implicit ec: ExecutionContext): SubInfo[T] =
      SubInfo(subscriber, new AsyncSemaphore())

    def none[T](implicit ec: ExecutionContext): SubInfo[T] = SubInfo[T](Right(Promise[Unit]()))
  }

  private class LinkedSubscription[T](source: SourceImpl[T]) extends Subscription {
    private val unsubscribedPromise = Promise[Unsubscribed.type]()
    val unsubscribed: Future[Unsubscribed.type] = unsubscribedPromise.future

    def cancel(): Unit = {
      source.unsubscribe(this)
      unsubscribedPromise.success(Unsubscribed)
    }

    def requestMore(elements: Int): Unit = source.request(this, elements)
  }

  private case class SubscriberInfo[T](subscriber: Subscriber[T], subscription: LinkedSubscription[T])

  private object Unsubscribed
}
