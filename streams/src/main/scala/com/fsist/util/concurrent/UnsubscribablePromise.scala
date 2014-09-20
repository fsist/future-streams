package com.fsist.util.concurrent

import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}

import com.fsist.util.concurrent.UnsubscribablePromise.RegHandle
import com.typesafe.scalalogging.slf4j.Logging

import scala.annotation.tailrec
import scala.concurrent.{CanAwait, Future, Promise, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** A Promise whose future's `onComplete` (and `map`, etc) can be unregistered.
  *
  * This helps avoid unbounded memory and OOM when the promise is used like a CancelToken.
  *
  * To be able to actually unregister a callback, you need a token of type `RegHandle` that you will pass to `unregister`.
  * You get these tokens by calling the methods that end with U: `onCompleteU`, `mapU`, `flatMapU`, etc.
  * If you call the regular Future methods, you cannot unregister your callbacks. (These methods are defined on the
  * [[com.fsist.util.concurrent.UnsubscribableFuture]] returned by `future`.)
  *
  * @param internalExecutor Used for some internal scheduling. Never used to execute the user's continuations.
  */
class UnsubscribablePromise[T](internalExecutor: ExecutionContext = ExecutionContext.global) extends Promise[T]  {
  import UnsubscribablePromise._

  private val promise = Promise[T]()

  override lazy val future: UnsubscribableFuture[T] = new UnsubscribableFutureImpl[T](this, internalExecutor)
  override def tryComplete(result: Try[T]): Boolean = promise.tryComplete(result)
  override def isCompleted: Boolean = promise.future.isCompleted
}

object UnsubscribablePromise extends Logging {
  /** The type you need to pass to `unregister`. Returned by various registration methods: `onCompleteU`, `mapU`, etc. */
  type RegHandle[T] = Try[T] => _

  /** Implementation of the Future part of [[UnsubscribablePromise]], see its documentation. */
  class UnsubscribableFutureImpl[T] private[UnsubscribablePromise] (promiser: UnsubscribablePromise[T], internalExecutor: ExecutionContext) extends UnsubscribableFuture[T] {

    // Contains Some map while the promise is uncompleted. Changes atomically to None once the promise is completed
    // and we execute all new registered continuations immediately.
    private val continuations = new AtomicReference[Option[Map[Try[T] => _, ExecutionContext]]](Some(Map.empty))

    promiser.promise.future.onComplete(completeContinuations)(internalExecutor)

    private def completeContinuations(result: Try[T]): Unit = {
      continuations.getAndSet(None) match {
        case Some(map) =>
          for ((continuation, executor) <- map) {
            completeContinuation(result, continuation, executor)
          }
        case None => // Already completed
      }
    }

    private def completeContinuation(result: Try[T], continuation: Try[T] => _, executor: ExecutionContext): Unit = {
      executor.prepare().execute(new Runnable {
        override def run(): Unit =
          try {
            continuation(result)
          }
          catch {
            case NonFatal(e) =>
              executor.reportFailure(e)
          }
      })
    }

    /** As `Future.onComplete`, but returns the `func` passed in instead of Unit. */
    @tailrec
    final def onCompleteU[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): RegHandle[T] = {
      continuations.get() match {
        case some@Some(map) =>
          val updated = map.updated(func, executor)
          if (continuations.compareAndSet(some, Some(updated))) func
          else onCompleteU(func)(executor)
        case None =>
          completeContinuation(value.get, func, executor)
          func
      }
    }

    override def isCompleted: Boolean = promiser.promise.future.isCompleted
    override def onComplete[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): Unit = onCompleteU(func)(executor)
    override def value: Option[Try[T]] = promiser.promise.future.value
    override def result(atMost: Duration)(implicit permit: CanAwait): T = promiser.promise.future.result(atMost)(permit)
    override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
      promiser.promise.future.ready(atMost)(permit)
      this
    }

    // Override the default implementation in the Future trait which creates a new Promise every time, and just make it
    // into a lazy val - there's no reason for Future.failed to create a new promise
    override lazy val failed: Future[Throwable] = promiser.promise.future.failed

    /** Unregister a continuation previously passed to `onComplete` or another similar method. */
    @tailrec
    final def unregister(handle: RegHandle[T]): Boolean = continuations.get() match {
      case some@Some(map) =>
        if (map.contains(handle)) {
          val updated = map - handle
          if (continuations.compareAndSet(some, Some(updated))) true
          else unregister(handle)
        }
        else false
      case None => false // All continuations have already fired
    }


    // ==========================================
    // Wrap other Future methods to create a RegHandle[T]. The implementations here are all copied from the Future trait
    // with the changes being obvious.
    // ==========================================

    def onSuccessU[U](pf: PartialFunction[T, U])(implicit executor: ExecutionContext): RegHandle[T] = onCompleteU {
      case Success(v) =>
        pf.applyOrElse[T, Any](v, Predef.conforms[T]) // Exploiting the cached function to avoid MatchError
      case _ =>
    }

    def onFailureU[U](callback: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): RegHandle[T] = onCompleteU {
      case Failure(t) =>
        callback.applyOrElse[Throwable, Any](t, Predef.conforms[Throwable]) // Exploiting the cached function to avoid MatchError
      case _ =>
    }

    def foreachU[U](f: T => U)(implicit executor: ExecutionContext): RegHandle[T] = onCompleteU {
      _ foreach f
    }

    def transformU[S](s: T => S, f: Throwable => Throwable)(implicit executor: ExecutionContext): (Future[S], RegHandle[T]) = {
      val p = Promise[S]()
      // transform on Try has the wrong shape for us here
      val handle = onCompleteU {
        case Success(r) => p complete Try(s(r))
        case Failure(t) => p complete Try(throw f(t)) // will throw fatal errors!
      }
      (p.future, handle)
    }

    def mapU[S](f: T => S)(implicit executor: ExecutionContext): (Future[S], RegHandle[T]) = {
      val p = Promise[S]()
      val handle = onCompleteU { v => p complete (v map f)}
      (p.future, handle)
    }

    def flatMapU[S](f: T => Future[S])(implicit executor: ExecutionContext): (Future[S], RegHandle[T]) = {
      // TODO Can't use DefaultPromise.linkRootOf here the way Future.flatMap does, because it's private,
      // so we leak memory for now
      val p = Promise[S]()
      val handle = onCompleteU {
        case f: Failure[_] => p complete f.asInstanceOf[Failure[S]]
        case Success(v) => try f(v) match {
          case fut => fut.onComplete(p.complete)(internalExecutor)
        } catch {
          case NonFatal(t) => p failure t
        }
      }
      (p.future, handle)
    }

    def filterU(pred: T => Boolean)(implicit executor: ExecutionContext): (Future[T], RegHandle[T]) =
      mapU {
        r => if (pred(r)) r else throw new NoSuchElementException("Future.filter predicate is not satisfied")
      }

    final def withFilterU(p: T => Boolean)(implicit executor: ExecutionContext): (Future[T], RegHandle[T]) = filterU(p)(executor)

    def collectU[S](pf: PartialFunction[T, S])(implicit executor: ExecutionContext): (Future[S], RegHandle[T]) =
      mapU {
        r => pf.applyOrElse(r, (t: T) => throw new NoSuchElementException("Future.collect partial function is not defined at: " + t))
      }

    def recoverU[U >: T](pf: PartialFunction[Throwable, U])(implicit executor: ExecutionContext): (Future[U], RegHandle[T]) = {
      val p = Promise[U]()
      val handle = onCompleteU { v => p complete (v recover pf)}
      (p.future, handle)
    }

    def recoverWithU[U >: T](pf: PartialFunction[Throwable, Future[U]])(implicit executor: ExecutionContext): (Future[U], RegHandle[T]) = {
      val p = Promise[U]()
      val handle = onCompleteU {
        case Failure(t) => try pf.applyOrElse(t, (_: Throwable) => this).onComplete(p.complete)(internalExecutor) catch {
          case NonFatal(t) => p failure t
        }
        case other => p complete other
      }
      (p.future, handle)
    }

    def zipU[U](that: Future[U]): (Future[(T, U)], RegHandle[T]) = {
      implicit val ec = internalExecutor
      val p = Promise[(T, U)]()
      val handle = onCompleteU {
        case f: Failure[_] => p complete f.asInstanceOf[Failure[(T, U)]]
        case Success(s) => that onComplete { c => p.complete(c map { s2 => (s, s2)})}
      }
      (p.future, handle)
    }

    def fallbackToU[U >: T](that: Future[U]): (Future[U], RegHandle[T]) = {
      implicit val ec = internalExecutor
      val p = Promise[U]()
      val handle = onCompleteU {
        case s@Success(_) => p complete s
        case f@Failure(_) => that onComplete {
          case s2@Success(_) => p complete s2
          case _ => p complete f // Use the first failure as the failure
        }
      }
      (p.future, handle)
    }

    // Copied from Future companion object as implementation detail of `mapToU`
    private val toBoxed = Map[Class[_], Class[_]](
      classOf[Boolean] -> classOf[java.lang.Boolean],
      classOf[Byte] -> classOf[java.lang.Byte],
      classOf[Char] -> classOf[java.lang.Character],
      classOf[Short] -> classOf[java.lang.Short],
      classOf[Int] -> classOf[java.lang.Integer],
      classOf[Long] -> classOf[java.lang.Long],
      classOf[Float] -> classOf[java.lang.Float],
      classOf[Double] -> classOf[java.lang.Double],
      classOf[Unit] -> classOf[scala.runtime.BoxedUnit]
    )

    def mapToU[S](implicit tag: ClassTag[S]): (Future[S], RegHandle[T]) = {
      implicit val ec = internalExecutor
      val boxedClass = {
        val c = tag.runtimeClass
        if (c.isPrimitive) toBoxed(c) else c
      }
      require(boxedClass ne null)
      mapU(s => boxedClass.cast(s).asInstanceOf[S])
    }

    def andThenU[U](pf: PartialFunction[Try[T], U])(implicit executor: ExecutionContext): (Future[T], RegHandle[T]) = {
      val p = Promise[T]()
      val handle = onCompleteU {
        case r => try pf.applyOrElse[Try[T], Any](r, Predef.conforms[Try[T]]) finally p complete r
      }
      (p.future, handle)
    }
  }
}

trait UnsubscribableFuture[T] extends Future[T] {

  /** Unregister a continuation previously passed to `onComplete` or another similar method. */
  def unregister(handle: RegHandle[T]): Boolean

  /** As `Future.onComplete`, but returns the `func` passed in instead of Unit. */
  def onCompleteU[U](func: (Try[T]) => U)(implicit executor: ExecutionContext): RegHandle[T]

  /** As `Future.firstCompletedOf(others :+ this)`, but once the first future completes, unregisters the completion
    * callbacks from all the other futures (including any futures in `others` which are also UnsubscribablePromises.
    *
    * This prevents those callbacks from hanging on forever and becoming memory leaks if some of these futures never
    * complete.
    *
    * Note: the name `firstCompletedOr` is different from `firstCompletedOf` because `this` is one of the awaited futures.
    */
  def firstCompletedOr[U >: T](others: TraversableOnce[Future[U]])(implicit executor: ExecutionContext): Future[U] = {
    val promise = Promise[U]()
    val completeFirst: Try[U] => Unit = promise tryComplete _

    val callbacks = collection.mutable.Map[UnsubscribableFuture[T], RegHandle[T]]()
    others foreach (_ match {
      case unsub: UnsubscribableFuture[T] =>
        val handle = unsub.onCompleteU(completeFirst)
        callbacks.update(unsub, handle)
      case future =>
        future.onComplete(completeFirst)
    })

    val handle = this.onCompleteU(completeFirst)
    callbacks.update(this, handle)

    promise.future.onComplete(_ => {
      callbacks foreach {
        case (unsub, handle) => unsub.unregister(handle)
      }
    })

    promise.future
  }
}
