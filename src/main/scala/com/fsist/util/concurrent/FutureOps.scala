package com.fsist.util.concurrent

import java.util.concurrent.TimeoutException

import com.typesafe.scalalogging.slf4j.Logging

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Extra methods on Future[T], with implicit PML conversion.
  * NOTE: this should be a value type, but that hits a known a bug in the Scala compiler that was only fixed in 2.11.
  */
class FutureOps[T](val fut: Future[T]) extends Logging {

  /** Once this future completes (with success or failure), call `that`, wait for the returned future,
    * ignore its return value or failure, and return the original result of this future once `that` completes.
    */
  def flatAndThen(that: => Future[Unit])(implicit ec: ExecutionContext) : Future[T] =
    fut.flatMap (t => {
      that.map(_ => t).recover { case NonFatal(e) => t }
    }).recoverWith {
      case NonFatal(orig) =>
        that.map(_ => throw orig).recover { case NonFatal(e) => throw orig }
    }

  /** Returns a future that always succeeds and presents the original future's success or failure explicitly.
    * Useful for async/await.
    */
  def toTry()(implicit ec: ExecutionContext): Future[Try[T]] = fut map Success.apply recover { case NonFatal(e) => Failure(e) }
}

object FutureOps extends Logging {
  implicit def apply[T](fut: Future[T]): FutureOps[T] = {
    new FutureOps(fut)
  }

  /** Converts synchronously thrown exceptions into failed Futures. */
  def exceptionToFailure[T](fut: => Future[T]): Future[T] = try {
    fut
  }
  catch {
    case NonFatal(e) => Future.failed(e)
  }
}
