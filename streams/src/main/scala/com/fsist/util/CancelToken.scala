package com.fsist.util

import scala.concurrent.{ExecutionContext, Future, Promise}

/** Used to cooperatively, asynchronously cancel operations.
  *
  * Cooperative cancellation means a process notices it by explicitly checking the cancel token and reacting to it.
  * An existing black-box process can't be magically made cancellable.
  *
  * The general use pattern is to pass an implicit cancel token to and through library methods to make it available.
  */
class CancelToken private (val promise: Promise[Unit], allowCancel: Boolean = true) {
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
  def throwIfCanceled[T](): T=>T = t => if (isCanceled) throw new CanceledException() else t

  /** Cancel this token.
    * @return true if we canceled it; false if it was already canceled.
    */
  def cancel(): Boolean = if (allowCancel) promise.trySuccess(()) else throw new IllegalArgumentException("This token doens't allow cancellation")

  /** Returns a future that is completed when the token is canceled (it may be already completed when it's returned). */
  def future: Future[Unit] = promise.future

  /** Returns a future that is failed with a CanceledException when the token is canceled (it may be already completed when it's returned). */
  def futureFail(implicit ec: ExecutionContext): Future[Unit] =
    promise.future flatMap (_ => Future.failed(new CanceledException()))

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
  def abandonOnCancel[T](other: Future[T])(implicit ec: ExecutionContext) : Future[T] =
    Future.firstCompletedOf(Seq(futureFail.map(_.asInstanceOf[T]), other))
}

object CancelToken {
  def apply(): CancelToken = new CancelToken(Promise[Unit]())

  /** A singleton token that can never be cancelled. */
  val none: CancelToken = new CancelToken(Promise[Unit](), false)
}

/** Thrown by methods of CancelToken as documented when an operation has been canceled. */
class CanceledException(msg: String = "Token canceled") extends Exception(msg)
