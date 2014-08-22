package com.fsist.util.concurrent

import scala.concurrent._
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.Try

/** Used to cooperatively, asynchronously cancel operations.
  *
  * Cooperative cancellation means a process notices it by explicitly checking the cancel token and reacting to it.
  * An existing black-box process can't be magically made cancellable.
  *
  * The general use pattern is to pass an implicit cancel token to and through library methods to make it available.
  *
  * Create instances using the companion object `apply`.
  */
class CancelToken private(val promise: Promise[Unit], allowCancel: Boolean = true) {
  import com.fsist.util.concurrent.CancelToken._

  /** @return true iff the token is already canceled. */
  def isCanceled: Boolean = promise.isCompleted

  /** Returns a function that passes through values of type T without modification, until this token is canceled,
    * after which the returned function throws a new CanceledException every time it's called.
    *
    * This is useful to implement the following pattern:
    * {{{
    *   someFuture map (cancelToken.throwIfCanceled) map myContinuation
    * }}}
    */
  def throwIfCanceled[T](): T => T = t => if (isCanceled) throw new CanceledException() else t

  /** Cancel this token.
    * @return true if we canceled it; false if it was already canceled.
    * @throws IllegalArgumentException if you try to cancel `CancelToken.none`.
    */
  def cancel(): Boolean = if (allowCancel) promise.trySuccess(()) else throw new IllegalArgumentException("This token doens't allow cancellation")

  /** Returns a future that is completed when the token is canceled (it may be already completed when it's returned). */
  def future: Future[Unit] = if (allowCancel) promise.future else never

  /** Returns a future that is failed with a CanceledException when the token is canceled (it may be already completed when it's returned). */
  def futureFail(implicit ec: ExecutionContext): Future[Unit] =
    if (allowCancel) {
      future flatMap (_ => Future.failed(new CanceledException()))
    } else never

  /** The returned future does one of two things:
    *
    * 1. If this token is canceled before the `other` future completes, it fails with a CanceledException.
    * 2. If the `other` future completes first, it completes with its result value (success or failure).
    *
    * NOTE: the asynchronous task represented by the `other` future is NOT actually canceled or aborted.
    * The only thing that happens when this token is canceled, is that you *stop waiting* for the `other` future.
    * Hence the name of this method, 'abandon' and not 'cancel'.
    *
    * Whenever possible, use or implement an actually cancelable future instead of using this method.
    */
  def abandonOnCancel[T](other: Future[T])(implicit ec: ExecutionContext): Future[T] =
    if (allowCancel) {
      Future.firstCompletedOf(Seq(futureFail.map(_.asInstanceOf[T]), other))
    }
    else other
}

object CancelToken {
  def apply(): CancelToken = new CancelToken(Promise[Unit]())

  /** A singleton token that can never be cancelled. */
  val none: CancelToken = new CancelToken(Promise[Unit](), false)

  private val never : Future[Unit] = new NeverFuture()
}

/** Thrown by methods of CancelToken as documented when an operation has been canceled. */
class CanceledException(msg: String = "Token canceled") extends Exception(msg)

/** Represents a Future that never completes. This allows us to discard continuation functions, which would otherwise
  * stay forever in memory (for CancelToken.none).
  *
  * Credited to Viktor Klang. Cf also https://github.com/viktorklang/scala/commit/0f91e9050e1b5fa7a9aecfd973f85f83d6d6b0d2
  */
class NeverFuture() extends Future[Nothing] {
  type T = Nothing
  override def onSuccess[U](pf: PartialFunction[T, U])(implicit executor: ExecutionContext): Unit = ()
  override def onFailure[U](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Unit = ()
  override def onComplete[U](f: Try[T] => U)(implicit executor: ExecutionContext): Unit = ()
  override def isCompleted: Boolean = false
  override def value: Option[Try[T]] = None
  override val failed: Future[Throwable] = this //new NeverFuture[Throwable]
  override def foreach[U](f: T => U)(implicit executor: ExecutionContext): Unit = ()
  override def transform[S](s: T => S, f: Throwable => Throwable)(implicit executor: ExecutionContext): Future[S] = this //new NeverFuture[S]
  override def map[S](f: T => S)(implicit executor: ExecutionContext): Future[S] = this //new NeverFuture[S]
  override def flatMap[S](f: T => Future[S])(implicit executor: ExecutionContext): Future[S] = this //new NeverFuture[S]
  override def filter(p: T => Boolean)(implicit executor: ExecutionContext): Future[T] = this
  override def collect[S](pf: PartialFunction[T, S])(implicit executor: ExecutionContext): Future[S] = this //new NeverFuture[S]
  override def recover[U >: T](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): Future[U] = this
  override def recoverWith[U >: T](pf: PartialFunction[Throwable, Future[U]])(implicit executor: ExecutionContext): Future[U] = this
  override def zip[U](that: Future[U]): Future[(T, U)] = this //new NeverFuture[(T, U)]
  override def fallbackTo[U >: T](that: Future[U]): Future[U] = this
  override def mapTo[S](implicit tag: ClassTag[S]): Future[S] = this //new NeverFuture[S]
  override def andThen[U](pf: PartialFunction[Try[T], U])(implicit executor: ExecutionContext): Future[T] = this

  override def result(atMost: Duration)(implicit permit: CanAwait): T = {
    atMost match {
      case e if e eq Duration.Undefined => throw new IllegalArgumentException("cannot wait for Undefined period")
      case Duration.Inf =>
        while (true) Thread.sleep(Long.MaxValue)
        throw new TimeoutException // Never reached, but allows the code to compile
      case f: FiniteDuration if f > Duration.Zero =>
        val nanos = (f.toNanos - (f.toMillis * 1000000)).toInt
        Thread.sleep(f.toMillis, nanos)
        throw new TimeoutException
      case _ =>
        // For negative durations, don't wait at all
        throw new TimeoutException
    }
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    ready(atMost)
    throw new TimeoutException
  }
}
