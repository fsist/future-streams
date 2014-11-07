package com.fsist.stream

import com.fsist.util.FastAsync._

import scala.async.Async._
import scala.concurrent.{Future, ExecutionContext}

sealed trait Func[-A, +B] {
  def compose[C](next: Func[B, C])(implicit ec: ExecutionContext): Func[A, C] = this match {
    case syncf: SyncFunc[A, B] => next match {
      case syncf2: SyncFunc[B, C] => new SyncFunc[A, C] {
        override def apply(a: A): C = syncf2(syncf(a))
      }
      case asyncf: AsyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = asyncf(syncf(a))
      }
    }
    case asyncf: AsyncFunc[A, B] => next match {
      case syncf: SyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = async {
          syncf(fastAwait(asyncf(a)))
        }
      }
      case asyncf2: AsyncFunc[B, C] => new AsyncFunc[A, C] {
        override def apply(a: A)(implicit ec: ExecutionContext): Future[C] = async {
          fastAwait(asyncf2(fastAwait(asyncf(a))))
        }
      }
    }
  }
}

object Func {
  implicit def apply[A, B](f: A => B) : SyncFunc[A, B] = SyncFunc(f)
}

trait SyncFunc[-A, +B] extends Func[A, B] {
  def apply(a: A): B

  /** Like `Func.compose`, but because it composes two SyncFuncs, it doesn't need an ExecutionContext. */
  def compose[C](next: SyncFunc[B, C]): SyncFunc[A, C] = {
    val self = this
    new SyncFunc[A, C] {
      override def apply(a: A): C = next(self(a))
    }
  }
}

object SyncFunc {
  implicit def apply[A, B](f: A => B) : SyncFunc[A, B] = new SyncFunc[A, B] {
    override def apply(a: A): B = f(a)
  }
}

trait AsyncFunc[-A, +B] extends Func[A, B] {
  def apply(a: A)(implicit ec: ExecutionContext): Future[B]
}

object AsyncFunc {
  implicit def apply[A, B](f: A => Future[B]) : AsyncFunc[A, B] = new AsyncFunc[A, B] {
    override def apply(a: A)(implicit ec: ExecutionContext): Future[B] = f(a)
  }
}