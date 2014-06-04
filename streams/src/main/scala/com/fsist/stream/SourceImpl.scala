package com.fsist.stream

import com.fsist.util._
import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Subscriber, Subscription, Publisher}
import scala.async.Async._
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.util.Failure
import scala.Some
import scala.util.Success
import com.fsist.stream.SourceImpl.{SubscriberInfo, SubInfo}
import scala.util.control.NonFatal

/** Base implementation of Source. Concrete implementations need to supply the `produce` method.
  *
  * You can inherit directly and implement produce(), or use one of the construct functions in [[Source]].
  *
  * This is a mutable threadsafe implementation. The produce() function is treated as non-reentrant.
  * It can access and mutate state stored in the concrete class instance using that assumption.
  */
trait SourceImpl[T] extends Source[T] {
  def getPublisher: Publisher[T] = this

  def produceTo(consumer: Consumer[T]): Unit = {
    subscribe(consumer.getSubscriber)
  }

  @volatile private var subInfo: SubInfo[T] = SubInfo[T](Right(Promise[Unit]()))

  /** Must be implemented to produce the next input element to be sent to subscribers.
    *
    * This method will not be called again until the previously returned future has completed.
    * If a future returns None, that signals end of stream; if it fails, it propagates the error.
    * In either case the producer will be terminated and this function will never be called again.
    *
    * If this method blocks or waits for another future, it SHOULD implement cancelation support using this.cancelToken.
    */
  protected def produce(): Future[Option[T]]

  private val done: Promise[Unit] = Promise[Unit]()

  def onSourceDone: Future[Unit] = done.future

  def subscriber: Option[Subscriber[T]] = subInfo.subscriber match {
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
    }(ec) recover { case NonFatal(e) => failSource(e)}
  }

  // Locked to synchronize `fail`, `subscribe` and `unsubscribe` to make sure that if we fail, each subscriber is notified exactly once
  private val failureLock = new AnyRef

  /** Notify all subscribers of this failure and refuse future subscriptions. */
  protected def failSource(e: Throwable): Unit = failureLock.synchronized {
    if (e.isInstanceOf[CanceledException])
      logger.trace(s"Source canceled")
    else
      logger.error(s"Error in source: $e")

    done.tryFailure(e)
    subInfo.subscriber match {
      case Left(SubscriberInfo(sub, _)) => sub.onError(e)
      case _ =>
    }
    subInfo = SubInfo(Right(Promise[Unit]()))
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
      await(fut) match {
        case Some(t) =>
          logger.trace(s"Produced element $t")
          publishToSub(t)
        case None =>
          logger.trace(s"Produced EOF")
          subInfo.subscriber match {
            case Left(SubscriberInfo(sub, _)) => sub.onComplete()
            case _ =>
              logger.trace(s"No subscriber to see our EOF")
            // TODO should wait for a subscriber first and then produce and publish an element
          }
          done.success(())
          done.future
      }
    }
  }

  private def publishToSub(t: T): Future[Unit] = async {
    val info = subInfo // Local copy to avoid races with writers
    info.subscriber match {
      case Left(SubscriberInfo(sub, _)) =>
        val subName = if (logger.underlying.isTraceEnabled) {
          if (sub.isInstanceOf[Sink[T, _]])
            sub.asInstanceOf[Sink[T, _]].name
          else
            sub.toString
        }
        else ""

        logger.trace(s"Waiting for subscriber '$subName' to request more data before publishing")

        await(cancelToken.abandonOnCancel(info.requestedCount.decrement()))

        logger.trace(s"Publishing element to subscriber $subName")
        sub.onNext(t)

        await(nextStep)

      case Right(promise) =>
        logger.trace(s"Waiting for subscribers before publishing")
        await(promise.future)
        if (done.isCompleted) {
          await(done.future)
        }
        else {
          await(publishToSub(t))
        }
    }
  }

  def subscribe(subscriber: Subscriber[T]): Unit =
    failureLock.synchronized {
      if (onSourceDone.isCompleted) {
        onSourceDone.value.get match {
          case Success(_) =>
            val err = "This Source is already completed and cannot be subscribed to."
            logger.error(err)
            subscriber.onError(new IllegalStateException(err))
          case Failure(e) => subscriber.onError(e)
        }
      }
      else subInfo.subscriber match {
        case Left(otherSub) =>
          val err = s"This Source already has a subscriber and another cannot be added."
          logger.error(err)
          subscriber.onError(new IllegalStateException(err))
        case Right(promise) =>
          val subscription = new Subscription {
            def cancel(): Unit = unsubscribe(this)
            def requestMore(elements: Int): Unit = request(this, elements)
          }
          subInfo = SubInfo(Left(SubscriberInfo(subscriber, subscription)))
          logger.trace(s"Added subscriber")
          subscriber.onSubscribe(subscription)
          promise.success(())
          started // Force lazy val
      }
    }

  /** Remove this subscription. Equivalent to sub.cancel() */
  def unsubscribe(sub: Subscription): Unit = failureLock.synchronized {
    subInfo.subscriber match {
      case Left(SubscriberInfo(_, sub2)) if sub2 eq sub =>
        logger.trace(s"Removed subscriber")
        subInfo = SubInfo(Right(Promise[Unit]()))
      case Left(_) =>
        logger.error(s"Wrong subscription, cannot unsubscribe")
      case _ =>
        logger.error("No subscriber, cannot unsubscribe")
    }
  }

  /** Request more elements for this subscription. Equivalent to sub.requestMore(count) */
  def request(sub: Subscription, count: Int): Unit = {
    // Local copy to prevent races with writer
    val info = subInfo
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
    def apply[T](subscriber: Either[SubscriberInfo[T], Promise[Unit]])(implicit ec: ExecutionContext) : SubInfo[T] =
      SubInfo(subscriber, new AsyncSemaphore())
  }

  private case class SubscriberInfo[T](subscriber: Subscriber[T], subscription: Subscription)
}
