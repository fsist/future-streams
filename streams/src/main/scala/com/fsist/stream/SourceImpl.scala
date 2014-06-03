package com.fsist.stream

import com.fsist.util._
import org.reactivestreams.api.Consumer
import org.reactivestreams.spi.{Subscriber, Subscription, Publisher}
import scala.async.Async._
import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.util.Failure
import scala.Some
import com.fsist.stream.SourceImpl.{Subs, SubInfo}
import scala.util.Success

/** Base implementation of Source. Concrete implementations need to supply the `produce` method.
  *
  * You can inherit directly and implement produce(), or use one of the construct functions in [[Source]].
  *
  * This is a mutable threadsafe implementation. The produce() function is treated as non-reentrant.
  * It can access and mutate state stored in the concrete class instance using that assumption.
  */
trait SourceImpl[T] extends Source[T] {
  implicit def ec: ExecutionContext

  /** Expected to always return the same token throughout the lifetime of this instance.
    * Canceling this token will cause the Source to fail with a CanceledException.
    */
  implicit def cancelToken: CancelToken

  def getPublisher: Publisher[T] = this

  def produceTo(consumer: Consumer[T]): Unit = {
    subscribe(consumer.getSubscriber)
  }

  /** All current subscriptions and the amount of elements they're willing to accept.
    * The Promise in the `Subs` is fullfilled when the count of subscribers goes from 0 to 1. */
  private val subscribers : Atomic[Subs[T]] = new Atomic(Subs[T](Promise[Unit](), Map()))

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

  /** This lazy val ensures calling start() is idempotent. */
  private[this] lazy val started: Unit = {
    logger.trace(s"Starting source")

    // Fire and forget
    Future {
      nextStep()
    }(ec) recover { case e: Throwable => failSource(e)}
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
    // We're synchronized, so can access `subscribers` freely without copying/updating
    for (info <- subscribers.get.infos.values) {
      info.subscriber.onError(e)
    }
    clearSubscribers()
  }

  /** Remove all subscriber info */
  private def clearSubscribers() : Unit = subscribers.set(Subs[T](Promise[Unit](), Map()))

  /** Start producing elements. The returned future will complete when the producer stops: either when produce() returns
    * None, or when produce() returns a failed future, in which case that future will be returned. */
  def start(): Future[Unit] = {
    started // Force lazy val
    done.future
  }

  private def nextStep(): Future[Unit] = {
    cancelToken.throwIfCanceled

    val fut = try {
      logger.trace(s"Calling produce()")
      produce()
    } catch {
      case e: Throwable =>
        logger.error(s"Error in produce(): $e")
        Future.failed(e)
    }
    async {
      await(fut) match {
        case Some(t) =>
          logger.trace(s"Produced element $t")
          publishToSubs(t)
        case None =>
          logger.trace(s"Produced EOF")
          for (info <- subscribers.get.infos.values) {
            info.subscriber.onComplete()
          }
          done.success(())
          done.future
      }
    }
  }

  private def publishToSub(t: T, info: SubInfo[T]): Future[Unit] = async {
    val subName = if (logger.underlying.isTraceEnabled) {
      if (info.subscriber.isInstanceOf[Sink[T, _]])
        info.subscriber.asInstanceOf[Sink[T, _]].name
      else
        info.subscriber.toString
    }
    else ""

    logger.trace(s"Waiting for subscriber '$subName' to request more data before publishing")

    await(cancelToken.abandonOnCancel(info.requestedCount.decrement()))

    logger.trace(s"Publishing element to subscriber $subName")
    info.subscriber.onNext(t)
  }

  private def publishToSubs(t: T): Future[Unit] = async {
    val subs = subscribers.get // Get a distinct copy to avoid races with concurrent subscribes
    if (subs.infos.isEmpty) {
      logger.trace(s"Waiting for subscribers before publishing")
      await(subs.hasAny.future)

      if (done.isCompleted) {
        await(done.future)
      }
      else {
        await(publishToSubs(t))
      }
    }
    else {
      await(Future.sequence(subs.infos.values map (publishToSub(t, _))))
      await(nextStep)
    }
  }

  def subscribe(subscriber: Subscriber[T]): Unit =
    failureLock.synchronized {
      if (onSourceDone.isCompleted) {
        onSourceDone.value.get match {
          case Success(_) =>
            logger.error("This Source is already completed and cannot be subscribed to.")
            subscriber.onError(new IllegalStateException("This Source is already completed and cannot be subscribed to."))
          case Failure(e) => subscriber.onError(e)
        }
      }
      else {
        val sub = new Subscription {
          def cancel(): Unit = unsubscribe(this)
          def requestMore(elements: Int): Unit = request(this, elements)
        }
        val info = SubInfo(subscriber, new AsyncSemaphore())

        // Update atomically and return the Promise that should be fulfilled after the update succeeds, if any
        val promise = subscribers.update[Promise[Unit]] { subs =>
          val newSubs =
            if (subs.infos.isEmpty) Subs[T](Promise[Unit](), Map[Subscription, SubInfo[T]](sub -> info))
            else subs.copy(infos = subs.infos + (sub -> info))
          (newSubs, subs.hasAny)
        }
        promise.trySuccess(())

        subscriber.onSubscribe(sub)
      }
    }

  /** Remove this subscription. Equivalent to sub.cancel() */
  def unsubscribe(sub: Subscription): Unit = failureLock.synchronized {
    subscribers.update1{ subs => subs.copy(infos = subs.infos - sub)}
  }

  /** Request more elements for this subscription. Equivalent to sub.requestMore(count) */
  def request(sub: Subscription, count: Int): Unit = {
    val info = subscribers.get.infos.get(sub).get
    info.requestedCount.increment(count)
  }
}

object SourceImpl {
  private case class SubInfo[T](subscriber: Subscriber[T], requestedCount: AsyncSemaphore)
  private case class Subs[T](hasAny: Promise[Unit], infos: Map[Subscription, SubInfo[T]])
}
