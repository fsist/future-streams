package com.fsist.stream

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import com.fsist.FutureTester
import com.fsist.stream.PipeSegment.Passthrough
import com.fsist.util.concurrent.CancelToken
import com.typesafe.scalalogging.slf4j.Logging
import org.reactivestreams.{Subscription, Processor, Subscriber, Publisher}
import org.reactivestreams.tck.{IdentityProcessorVerification, SubscriberBlackboxVerification, TestEnvironment, PublisherVerification}
import org.scalatest.{DoNotDiscover, Ignore}
import org.scalatest.testng.TestNGSuiteLike
import scala.concurrent.blocking

import scala.concurrent.{Future, ExecutionContext}

// NOTE: classes in this file test our Streams implementation against the Reactive Streams TCK.

class SourceTckTest extends PublisherVerification[Long](TckTest.testEnv, 1000) with TckTest {
  override def createPublisher(elements: Long): Publisher[Long] = sourceOf(elements)
}

class SinkBlackboxTckTest extends SubscriberBlackboxVerification[Long](TckTest.testEnv) with TckTest {
  override def createSubscriber(): Subscriber[Long] = Sink.discard[Long]
}

// Two tests still fail: see https://github.com/reactive-streams/reactive-streams/issues/115
/*class IdentityPipeTckTest extends IdentityProcessorVerification[Long](TckTest.testEnv, 1000) with TckTest {
  override def createIdentityProcessor(bufferSize: Int): Processor[Long, Long] = Pipe.buffer[Long](bufferSize)

  override def maxSupportedSubscribers(): Long = 1

  override def mustCancelItsUpstreamSubscriptionIfItsLastDownstreamSubscriptionHasBeenCancelled(): Unit = {
    notVerified("Should not be tested, see #114")
  }

//  override def sourceOf(count: Long): Publisher[Long] = new SimplePublisher(count)
}
*/
trait TckTest extends TestNGSuiteLike with FutureTester {
  implicit val ecc : ExecutionContext = ExecutionContext.global

  /** Returns a Source that publishes exactly `count` Long elements.
    *
    * This isn't as trivial as `Source.from(1L to count)` because a Seq, even a Range, can't contain > Int.MaxValue elements.
    */
  def sourceOf(count: Long): Publisher[Long] = {
    var counted = 0L
    Source.generate {
      if (counted == count && count != Long.MaxValue) None
      else {
        counted += 1
        Some(counted)
      }
    }
  }

  def createErrorStatePublisher(): Publisher[Long] = new SourceImpl[Long] {
    failSource(new IllegalArgumentException)

    override def cancelToken: CancelToken = CancelToken.none
    override implicit def ec: ExecutionContext = ecc
    override protected def produce(): Future[Option[Long]] = Future.failed(new IllegalArgumentException)
  }

  def createHelperPublisher(elements: Long): Publisher[Long] = sourceOf(elements)
}

object TckTest extends TckTest {
  def testEnv: TestEnvironment = new TestEnvironment(1000)
}

// Temporary experiment, please ignore - in the process of working out why some tests are failing
/*
class SimplePublisher(max: Long) extends Publisher[Long] with Logging {
  private implicit val ec: ExecutionContext = ExecutionContext.global
  private val subscriber = new AtomicReference[Subscriber[_ >: Long]] ()
  private val requested = new Semaphore(0)
  private val subscription = new AtomicReference[Subscription]()

  Future(blocking(start()))

  override def subscribe(s: Subscriber[_ >: Long]): Unit = {
    if (! subscriber.compareAndSet(null, s)) {
      s.onError(new IllegalArgumentException("Already subscribed, cannot resubscribe"))
    }
    else {
      logger.info(s"Subscribing")
      val sub = new Subscription {
        override def cancel(): Unit = {
          logger.info(s"Canceling")
          subscriber.compareAndSet(s, null)
          requested.drainPermits()
        }
        override def request(n: Long): Unit = {
          if (n.toInt != n) throw new IllegalArgumentException(s"Doesn't really support request(Long)")
          requested.release(n.toInt)
        }
      }
      subscription.set(sub)
      s.onSubscribe(sub)
    }
  }

  private def start(): Unit = {
    var next = 0L
    while (next < max) {
      logger.info(s"Waiting for demand")
      requested.acquire()
      subscriber.get() match {
        case null => return // canceled
        case s =>
          logger.info(s"Publishing $next")
          s.onNext(next)
      }
      next += 1
    }
    subscriber.get().onComplete()
  }
}

*/