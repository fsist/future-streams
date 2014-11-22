package com.fsist.stream

import com.fsist.util.AsyncFunc

import scala.concurrent.{ExecutionContext, Future}

/** Adds various shortcuts to StreamOutput. Doesn't implement anything new, just provides convenience wrappers
  * for constructors of Source, Transform, Sink and Connect.
  */
trait SourceOps[+Out] {
  self: Source[Out] =>
  // These are just aliases for `connect`
  def to[Super >: Out](sink: Sink[Super]): sink.type = connect(sink)

  def transform[Super >: Out, Next](tr: Transform[Super, Next]): tr.type = connect(tr)

  // Shortcuts for Transform constructors

  def map[Next](mapper: Out => Next): Source[Next] = {
    val tr = Transform.map(mapper)
    transform(tr)
    tr
  }

  def filter(filter: Out => Boolean)(implicit ec: ExecutionContext): Source[Out] = {
    val tr = Transform.filter(filter)
    transform(tr)
    tr
  }

  // Shortcuts for Sink constructors

  def foreach[Super >: Out](func: Super => Unit): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(func)
    to(output)
    output
  }

  def foreachAsync[Super >: Out](func: Super => Future[Unit]): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(AsyncFunc(func))
    to(output)
    output
  }
}
