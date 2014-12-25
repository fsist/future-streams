package com.fsist

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent._
import org.scalatest.exceptions.{TestFailedDueToTimeoutException, TestFailedException}
import org.scalatest.time.{Nanoseconds, Span}
import org.scalatest.{Matchers, Assertions}
import scala.concurrent.Future
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.reflect.ClassTag

/** Convenience methods for tests using Futures */
trait FutureTester extends Futures with ScalaFutures with Assertions with Matchers with Eventually with LazyLogging {
  implicit def spanToDuration(span: Span): FiniteDuration = span.totalNanos.nanos

  implicit def durationToSpan(duration: FiniteDuration): Span = Span(duration.toNanos, Nanoseconds)

  /** The correct way to wait for a future to fail with a specific exception type.
    *
    * This method returns once the future has completed.
    * If it completes successfully, this method throws a TestFailedException.
    * If it fails with an exception of type E, this method returns normally.
    * If it fails with any other kind of exception, this method rethrows that exception.
    *
    * If we time out waiting for the future (as defined by the appropriate PatienceConfiguration),
    * we throw a corresponding exception.
    *
    * If an exceptionExpectation is provided, then it is not enough that the future fails on the correct exception
    * type - it must also make the expectation function return true when passed to it.
    */
  def awaitFailure[E <: Throwable](fut: FutureConcept[_], clue: String = "", exceptionExpectation: Option[E => Boolean] = None)(implicit patience: PatienceConfig, mf: Manifest[E]): Unit = {
    try {
      val ret = fut.futureValue(patience)
      fail(clue + s" (instead of failing, future returned: $ret)")
    }
    catch {
      case e: E if exceptionExpectation.isDefined =>
        assert(exceptionExpectation.get(e), clue + s"(exception type is right but content is not as expected: $e)")
      case e: E => return //Success
      case failed: TestFailedException => failed.cause match {
        case Some(cause: E) if exceptionExpectation.isDefined =>
          assert(exceptionExpectation.get(cause), clue + s"(exception type is right but content is not as expected: $cause)")
        case Some(cause: E) => return // Success
        case _ => throw failed
      }
    }
  }

  /** An awaitFailure overload with exception expectations and no textual clue
    */
  def awaitFailure[E <: Throwable](fut: FutureConcept[_], exceptionExpectation: Option[E => Boolean])(implicit patience: PatienceConfig, mf: Manifest[E]): Unit =
    awaitFailure[E](fut, "", exceptionExpectation)(patience, mf)


  /** Await a timeout of this future. Fails (throws a TestFailedException) if the future completes instead of timing out. */
  def awaitTimeout(fut: Future[_], clue: String = "")(implicit patience: PatienceConfig): Unit = {
    try {
      val value = fut.futureValue(patience)
      assert(false, s"Future completed normally but expected timeout in $clue. Future returned $value")
    }
    catch {
      // These are timeouts
      case e: TestFailedDueToTimeoutException => ()
      // Commented out types belong to Akka Actors
      //      case e: AskTimeoutException => ()
      case e: TestFailedException
        if e.message.isDefined && e.message.get.contains("A timeout occurred waiting for a future to complete") => ()
      //      case e: TestFailedException if e.cause.isDefined && e.cause.get.isInstanceOf[AskTimeoutException] => ()

      // This is not a timeout
      //      case NonFatal(e) => throw e
    }
  }

  /** Await either a timeout of this future, or its failure with an exception of type T.
    * Fails (throws a TestFailedException) if the future completes instead of timing out or failing. */
  def awaitTimeoutOrFailure[T <: Throwable](fut: Future[_], clue: String = "")(implicit patience: PatienceConfig, tag: ClassTag[T]): Unit = {
    try {
      val value = fut.futureValue(patience)
      assert(false, s"Future completed normally but expected timeout in $clue. Future returned $value")
    }
    catch {
      // These are timeouts
      case e: TestFailedDueToTimeoutException => ()

      case e: TestFailedException
        if e.message.isDefined && e.message.get.contains("A timeout occurred waiting for a future to complete") => ()

      case e: TestFailedException
        if e.cause.isDefined && tag.runtimeClass.isInstance(e.cause.get) => ()

      case e if tag.runtimeClass.isInstance(e) => ()

      // This is not a timeout
      //      case NonFatal(e) =>
      //        throw e
    }
  }

  /** Do nothing for a duration. This is a very poor implementation and no code outside tests should use it. */
  def await(duration: FiniteDuration): Unit = blocking {
    Thread.sleep(scaled(duration).millisPart)
  }

  /** eventually() overload for convenience. */
  def eventually[T](timeout: FiniteDuration, f: => T): T = eventually(Timeout(scaled(Span.convertDurationToSpan(timeout))))(f)
}
