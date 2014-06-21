package com.fsist.util

import scala.language.experimental.macros

import scala.concurrent.Future
import scala.reflect.macros.Context

object FastAsync {

  /** A wrapper for `await` from scala-async which returns synchronously if the future is already completed.
    * This gives a big performance boost.
    *
    * See also https://github.com/scala/async/issues/73
    */
  def fastAwait[T](fut: Future[T]): T = macro fastAwaitImpl[T]

  def fastAwaitImpl[T: c.WeakTypeTag](c: Context)(fut: c.Expr[Future[T]]): c.Expr[T] = {
    import c.universe._
    val tree =  q"{ val f = $fut; if (f.isCompleted) f.value.get.get else await(f) }"
    c.Expr[T](tree)
  }
}
