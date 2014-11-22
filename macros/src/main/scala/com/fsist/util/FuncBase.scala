package com.fsist.util

import scala.concurrent.{Future, ExecutionContext}
import scala.language.experimental.macros
import scala.reflect.macros.Context

/** This base trait is separate from `Func` to allow it to include macros. */
trait FuncBase[-A, +B] {
  /** Applies this function. If it is a SyncFunc, returns the result normally. If it is an AsyncFunc, uses [[FastAsync#fastAwait]]. */
  def fastAwait(arg: A): B = macro FuncBase.fastAwaitImpl[A, B]
}

object FuncBase {

  def fastAwaitImpl[A, B](c: Context {type PrefixType = FuncBase[A, B]})
                       (arg: c.Expr[A])
                       (implicit aTag: c.WeakTypeTag[A], bTag: c.WeakTypeTag[B]): c.Expr[B] = {
    import c.universe._

    val aName = aTag.tpe.typeSymbol.name
    val bName = bTag.tpe.typeSymbol.name
    val func = c.prefix

    val tree =
      q"""$func match {
           case sync: com.fsist.util.SyncFunc[$aName, $bName] => sync($arg) : $bTag
           case async: com.fsist.util.AsyncFunc[$aName, $bName] => com.fsist.util.FastAsync.fastAwait(async($arg)) : $bTag
         }"""
    c.Expr[B](tree)
  }
}
