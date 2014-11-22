package com.fsist.util

import com.fsist.util.FastAsync._

import scala.async.Async._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Success, Failure}
import scala.util.control.NonFatal

sealed trait Func[-A, +B] extends FuncBase[A, B] {
  /** Returns true iff this is a [[SyncFunc]] */
  def isSync: Boolean

  /** Shortcut for a cast to SyncFunc. Fails at runtime with ClassCastException. */
  def asSync: SyncFunc[A, B] = this.asInstanceOf[SyncFunc[A, B]]

  /** Shortcut for a cast to AsyncFunc. Fails at runtime with ClassCastException. */
  def asAsync: AsyncFunc[A, B] = this.asInstanceOf[AsyncFunc[A, B]]

  /** Creates a new function composing these two, which is synchronous iff both inputs were synchronous. */
  def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C]

  /** Adds a synchronous recovery stage to this function. If `this` is a SyncFunc, the result will also be synchronous. */
  def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[A, U]

  /** Adds an asnychronous recovery stage to this function. */
  def recoverWith[U >: B](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): AsyncFunc[A, U]

  /** Adds a recovery stage to this function. If `this` and `handler` are both synchronous, then the result will also
    * be synchronous.
    *
    * However, the handler cannot be a partial function; it must handle all [[NonFatal]] exceptions.
    * Exceptions that don't match the NonFatal extractor will not be passed to the handler.
    */
  def someRecover[U >: B](handler: Func[Throwable, U])(implicit ec: ExecutionContext): Func[A, U] = handler match {
    case syncf: SyncFunc[Throwable, U] => recover {
      case NonFatal(e) => syncf.apply(e)
    }
    case asyncf: AsyncFunc[Throwable, U] => recoverWith {
      case NonFatal(e) => asyncf.apply(e)
    }
  }

  /** Suppress all errors matched by the [[NonFatal]] extractor. If `this` is synchronous, the result is also synchronous. */
  def suppressErrors()(implicit ec: ExecutionContext): Func[A, Unit]

  /** Returns either B or a Future[B] depending on the type of this Func. */
  def someApply(a: A)(implicit ec: ExecutionContext): Any
}

object Func {
  implicit def apply[A, B](f: A => B): SyncFunc[A, B] = SyncFunc(f)

  private[util] val futureSuccess = Future.successful(())

  val nop: SyncFunc[Any, Unit] = new SyncFunc[Any, Unit] {
    override def apply(a: Any): Unit = ()
  }

  val nopAsync: AsyncFunc[Any, Unit] = new AsyncFunc[Any, Unit] {
    override def apply(a: Any)(implicit ec: ExecutionContext): Future[Unit] = futureSuccess
  }

  /** Returns a function that will call all of the `funcs` with the same input, in order, unless one of them fails.
    * The returned function will be synchronous if all of the input functions are synchronous.
    */
  def tee[A](funcs: Func[A, _]*)(implicit ec: ExecutionContext): Func[A, Unit] = {
    if (funcs.forall(_.isSync)) {
      val syncFuncs = funcs.map(_.asSync)
      SyncFunc[A, Unit]((a: A) => for (func <- syncFuncs) func.apply(a))
    }
    else {
      AsyncFunc[A, Unit]((a: A) => async {
        val iter = funcs.iterator
        while (iter.hasNext) iter.next().fastAwait(a)
      })
    }
  }

}

trait SyncFunc[-A, +B] extends Func[A, B] {
  def isSync: Boolean = true

  def apply(a: A): B

  override def someApply(a: A)(implicit ec: ExecutionContext): B = apply(a)

  def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = {
    val self = this
    next match {
      case syncf2: SyncFunc[B, C] => new SyncFunc[A, C] {
        override def apply(a: A): C = syncf2(self(a))
      }
      case asyncf: AsyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = asyncf(self(a))
      }
    }
  }

  /** Like `Func.compose`, but because it composes two SyncFuncs, it doesn't need an ExecutionContext. */
  def compose[C](next: SyncFunc[B, C]): SyncFunc[A, C] = {
    val self = this
    new SyncFunc[A, C] {
      override def apply(a: A): C = next(self(a))
    }
  }

  def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): SyncFunc[A, U] = recoverNoEc(handler)

  // Overload that doesn't require ExecutionContext
  def recoverNoEc[U >: B](handler: PartialFunction[Throwable, U]): SyncFunc[A, U] = SyncFunc[A, U] { a =>
    try {
      apply(a)
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => handler(e)
    }
  }

  def recoverWith[U >: B](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): AsyncFunc[A, U] = AsyncFunc[A, U] { a =>
    try {
      Future.successful(apply(a))
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => handler(e)
    }
  }

  def suppressErrors()(implicit ec: ExecutionContext): SyncFunc[A, Unit] = SyncFunc[A, Unit] { a =>
    try {
      apply(a)
    }
    catch {
      case NonFatal(e) =>
    }
  }
}

object SyncFunc {
  implicit def apply[A, B](f: A => B): SyncFunc[A, B] = new SyncFunc[A, B] {
    override def apply(a: A): B = f(a)
  }
}

trait AsyncFunc[-A, +B] extends Func[A, B] {
  def isSync: Boolean = false

  def apply(a: A)(implicit ec: ExecutionContext): Future[B]

  override def someApply(a: A)(implicit ec: ExecutionContext): Future[B] = apply(a)(ec)

  def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = {
    val self = this
    next match {
      case syncf: SyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = async {
          syncf(FastAsync.fastAwait(self(a)))
        }
      }
      case asyncf2: AsyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = async {
          FastAsync.fastAwait(asyncf2(FastAsync.fastAwait(self(a))))
        }
      }
    }
  }

  def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[A, U] = AsyncFunc[A, U] { a =>
    try {
      val fut = apply(a)
      if (fut.isCompleted) fut.value.get match {
        // Avoid scheduling an extra future
        case Success(x) => fut
        case Failure(e) =>
          if (handler.isDefinedAt(e)) Future.successful(handler(e))
          else Future.failed(e)
      }
      else fut.recover(handler)
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => Future.successful(handler(e))
    }
  }

  def recoverWith[U >: B](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): AsyncFunc[A, U] = AsyncFunc[A, U] { a =>
    try {
      (apply(a).recoverWith(handler))
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => handler(e)
    }
  }

  def suppressErrors()(implicit ec: ExecutionContext): AsyncFunc[A, Unit] = AsyncFunc[A, Unit] { a =>
    try {
      apply(a) map (_ => ()) recover {
        case NonFatal(e) =>
      }
    }
    catch {
      case NonFatal(e) => Func.futureSuccess
    }
  }
}

object AsyncFunc {
  implicit def apply[A, B](f: A => Future[B]): AsyncFunc[A, B] = new AsyncFunc[A, B] {
    override def apply(a: A)(implicit ec: ExecutionContext): Future[B] = try {
      f(a)
    }
    catch {
      case NonFatal(e) => Future.failed(e)
    }
  }
}