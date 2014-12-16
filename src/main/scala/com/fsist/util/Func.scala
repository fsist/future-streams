package com.fsist.util

import akka.http.util.FastFuture

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Success, Failure}
import scala.util.control.NonFatal

sealed trait Func[-A, +B] {
  /** Returns true iff this is a [[SyncFunc]] */
  def isSync: Boolean

  /** Returns true iff this is a function built by `Func.pass` */
  def isPass: Boolean = false

  /** Returns true iff this function is either `Func.nop` or `Func.nopAsync`. */
  def isNop: Boolean = false

  /** Shortcut for a cast to SyncFunc. Fails at runtime with ClassCastException. */
  def asSync: SyncFunc[A, B] = this.asInstanceOf[SyncFunc[A, B]]

  /** Shortcut for a cast to AsyncFunc. Fails at runtime with ClassCastException. */
  def asAsync: AsyncFunc[A, B] = this.asInstanceOf[AsyncFunc[A, B]]

  /** Creates a new function composing these two, which is synchronous iff both inputs were synchronous. */
  def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C]

  /** Alias for `compose`. Creates a new function composing these two, which is synchronous iff both inputs were synchronous. */
  def ~>[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = compose(next)(ec)

  /** Adds a synchronous recovery stage to this function. If `this` is a SyncFunc, the result will also be synchronous. */
  def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[A, U]

  /** Adds an asnychronous recovery stage to this function.
    *
    * This isn't declared to return an AsyncFunc because it can discard the `handler` and return a SyncFunc if the
    * original function is e.g. `nop` or `pass`. */
  def recoverWith[U >: B](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): Func[A, U]

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

  /** Returns a new function that passes any exceptions in the original function to `handler`.
    * The new function still fails with the original exception after the `handler` has run.
    */
  def composeFailure(handler: Throwable => Unit)(implicit ec: ExecutionContext): Func[A, B] = recover {
    case NonFatal(e) =>
      handler(e)
      throw e
  }
}

object Func {
  implicit def apply[A, B](f: A => B): SyncFunc[A, B] = SyncFunc(f)

  implicit def apply[B](f: => B): SyncFunc[Unit, B] = SyncFunc(f)

  private[util] val futureSuccess = Future.successful(())

  def pass[T]: SyncFunc[T, T] = new SyncFunc[T, T] {
    override def isPass: Boolean = true

    override def apply(a: T): T = a

    override def compose[C](next: Func[T, C])(implicit ec: ExecutionContext): Func[T, C] = next

    override def recover[U >: T](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[T, U] = this

    override def recoverWith[U >: T](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): SyncFunc[T, U] = this

    override def suppressErrors()(implicit ec: ExecutionContext): SyncFunc[T, Unit] = nop
  }

  val nop: SyncFunc[Any, Unit] = new SyncFunc[Any, Unit] {
    override def isNop: Boolean = true

    override def apply(a: Any): Unit = ()

    // Not overriding `compose`; we would have to create a new func instance anyway, and there would be no benefit

    override def recover[U >: Unit](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[Any, U] = this

    override def recoverWith[U >: Unit](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): AsyncFunc[Any, U] = nopAsync

    override def suppressErrors()(implicit ec: ExecutionContext): Func[Any, Unit] = this
  }

  val nopAsync: AsyncFunc[Any, Unit] = new AsyncFunc[Any, Unit] {
    override def isNop: Boolean = true

    override def apply(a: Any)(implicit ec: ExecutionContext): Future[Unit] = futureSuccess

    // Can't override compose without defining a new function instance anyway, which saves us nothing

    override def recover[U >: Unit](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[Any, U] = this

    override def recoverWith[U >: Unit](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): AsyncFunc[Any, U] = this

    override def suppressErrors()(implicit ec: ExecutionContext): Func[Any, Unit] = this
  }

  def const[T](t: T): SyncFunc[Unit, T] = new SyncFunc[Unit, T] {
    override def apply(a: Unit): T = t
  }

  /** Returns a function that will call all of the `funcs` with the same input, in order, unless one of them fails.
    * The returned function will be synchronous if all of the input functions are synchronous.
    */
  def tee[A](funcs: Func[A, _]*)(implicit ec: ExecutionContext): Func[A, Unit] = {
    val realFuncs = funcs.filter(f => !f.isNop && !f.isPass)
    if (realFuncs.isEmpty) {
      nop
    }
    else if (funcs.forall(_.isSync)) {
      val syncFuncs = funcs.map(_.asSync)
      SyncFunc[A, Unit]((a: A) => {
        val iter = syncFuncs.iterator
        while (iter.hasNext) {
          iter.next().apply(a)
        }
      })
    }
    else {
      def loop(a: A, iter: Iterator[Func[A, _]]): Future[Unit] = loopStep(a, iter)

      @tailrec
      def loopStep(a: A, iter: Iterator[Func[A, _]]): Future[Unit] =
        if (!iter.hasNext) futureSuccess
        else iter.next() match {
          case syncf: SyncFunc[A, _] =>
            syncf(a)
            loopStep(a, iter)
          case asyncf: AsyncFunc[A, _] =>
            val fut = asyncf(a)
            if (fut.isCompleted) loopStep(a, iter)
            else fut.flatMap(_ => loop(a, iter))
        }

      AsyncFunc[A, Unit]((a: A) => loop(a, funcs.iterator))
    }
  }

  /** A function that applies `target` for each item in the inputs sequentially. The results of `target` are discarded. */
  def foreach[A](target: Func[A, _]): Func[Iterable[A], Unit] = target match {
    case syncf: SyncFunc[A, _] =>
      new SyncFunc[Iterable[A], Unit] {
        override def apply(input: Iterable[A]): Unit = {
          val iter = input.iterator
          while (iter.hasNext) syncf(iter.next())
        }
      }
    case asyncf: AsyncFunc[A, _] =>
      new AsyncFunc[Iterable[A], Unit] {
        private def loopStep(iter: Iterator[A])(implicit ec: ExecutionContext): Future[Unit] = {
          if (!iter.hasNext) futureSuccess
          else asyncf(iter.next()) map (_ => loopStep(iter))
        }

        override def apply(a: Iterable[A])(implicit ec: ExecutionContext): Future[Unit] = loopStep(a.iterator)
      }
  }

  /** Flattens a function by calling the original function, and then calling the returned function with the same input. */
  def flatten[A, B](func: Func[A, Func[A, B]]): Func[A, B] = func match {
    case syncf: SyncFunc[A, Func[A, B]] =>
      new AsyncFunc[A, B] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[B] = syncf(a) match {
          case syncf2: SyncFunc[A, B] => FastFuture.successful(syncf2(a))
          case asyncf: AsyncFunc[A, B] => asyncf(a)
        }
      }
    case asyncf: AsyncFunc[A, Func[A, B]] =>
      new AsyncFunc[A, B] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[B] = {
          new FastFuture(asyncf(a)).flatMap(_ match {
            case syncf2: SyncFunc[A, B] => FastFuture.successful(syncf2(a))
            case asyncf: AsyncFunc[A, B] => asyncf(a)
          })
        }
      }
  }
}

// TODO whereever possible, replace SyncFunc.apply and AsyncFunc.apply with new (A)SyncFunc to replace extra function
// objects (closures) with methods.

trait SyncFunc[-A, +B] extends Func[A, B] {
  override def isSync: Boolean = true

  def apply(a: A): B

  override def someApply(a: A)(implicit ec: ExecutionContext): B = apply(a)

  override def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = {
    val self = this
    next match {
      case func if func.isPass => this.asInstanceOf[Func[A, C]] // B =:= C
      case ComposedAsyncFunc(before, middle, after) => ComposedAsyncFunc(self ~> before, middle, after)
      case syncf2: SyncFunc[B, C] => new SyncFunc[A, C] {
        override def apply(a: A): C = syncf2(self(a))
      }
      case asyncf: AsyncFunc[B, C] => ComposedAsyncFunc[A, C, B, C](self, asyncf, Func.pass)
    }
  }

  /** This overload acts like `Func.compose`, but because it composes two SyncFuncs, it doesn't need an ExecutionContext. */
  def compose[C](next: SyncFunc[B, C]): SyncFunc[A, C] = {
    if (next.isPass) this.asInstanceOf[SyncFunc[A, C]] // B =:= C
    else {
      val self = this
      new SyncFunc[A, C] {
        override def apply(a: A): C = next(self(a))
      }
    }
  }

  /** Alias for `compose`. This overload acts like `Func.compose`, but because it composes two SyncFuncs, it doesn't need an ExecutionContext. */
  def ~>[C](next: SyncFunc[B, C]): SyncFunc[A, C] = compose(next)

  override def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[A, U] = SyncFunc[A, U] { a =>
    try {
      apply(a)
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => handler(e)
    }
  }

  override def recoverWith[U >: B](handler: PartialFunction[Throwable, Future[U]])(implicit ec: ExecutionContext): Func[A, U] = AsyncFunc[A, U] { a =>
    try {
      Future.successful(apply(a))
    }
    catch {
      case NonFatal(e) if handler.isDefinedAt(e) => handler(e)
    }
  }

  override def suppressErrors()(implicit ec: ExecutionContext): Func[A, Unit] = Func[A, Unit] { a =>
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

  implicit def apply[B](f: => B): SyncFunc[Unit, B] = new SyncFunc[Unit, B] {
    override def apply(a: Unit): B = f
  }
}

trait AsyncFunc[-A, +B] extends Func[A, B] {
  override def isSync: Boolean = false

  def apply(a: A)(implicit ec: ExecutionContext): Future[B]

  override def someApply(a: A)(implicit ec: ExecutionContext): Future[B] = apply(a)(ec)

  override def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = {
    val self = this
    next match {
      case func if func.isPass => this.asInstanceOf[Func[A, C]] // B =:= C

      case syncf: SyncFunc[B, C] => ComposedAsyncFunc(Func.pass, self, syncf)

      case asyncf2: AsyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = {
          val fst = new FastFuture(self.apply(a))
          fst.flatMap(asyncf2.apply)
        }
      }
    }
  }

  def recover[U >: B](handler: PartialFunction[Throwable, U])(implicit ec: ExecutionContext): Func[A, U] = AsyncFunc[A, U] { a =>
    try {
      val fut = new FastFuture(apply(a))
      fut.recover(handler)
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

  def suppressErrors()(implicit ec: ExecutionContext): Func[A, Unit] = AsyncFunc[A, Unit] { a =>
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

/** An async func sandwiched between two sync ones. Enables efficient composing of sync funcs around async ones. */
case class ComposedAsyncFunc[-A, +B, InnerA, InnerB](before: SyncFunc[A, InnerA],
                                                     middle: AsyncFunc[InnerA, InnerB],
                                                     after: SyncFunc[InnerB, B]) extends AsyncFunc[A, B] {
  override def apply(a: A)(implicit ec: ExecutionContext): Future[B] = {
    middle(before(a)) map (after.apply)
  }

  override def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = {
    val self = this
    next match {
      case func if func.isPass => this.asInstanceOf[Func[A, C]] // B =:= C

      case ComposedAsyncFunc(nextBefore, nextMiddle, nextAfter) =>
        val composedSyncPart = self.after ~> nextBefore
        val middle = self.middle ~> composedSyncPart ~> nextMiddle
        ComposedAsyncFunc(self.before, middle.asAsync, nextAfter)

      case syncf: SyncFunc[B, C] => ComposedAsyncFunc[A, C, InnerA, InnerB](before, middle, after ~> syncf)

      case asyncf: AsyncFunc[B, C] =>
        val middle = self.middle ~> self.after ~> asyncf
        ComposedAsyncFunc(self.before, middle.asAsync, Func.pass[C])
    }
  }
}
