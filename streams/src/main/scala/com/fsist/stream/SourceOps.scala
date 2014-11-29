package com.fsist.stream

import com.fsist.util.{Func, AsyncFunc}

import scala.collection.immutable.BitSet
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
  }

  def filter(filter: Out => Boolean)(implicit ec: ExecutionContext): Source[Out] = {
    val tr = Transform.filter(filter)
    transform(tr)
  }

  // Shortcuts for Sink constructors

  def foreach[Super >: Out](func: Super => Unit): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(func)
    to(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit, onComplete: Unit => Res): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete)
    to(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit, onComplete: Unit => Res, onError: Throwable => Unit): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete, onError)
    to(output)
  }

  def foreachAsync[Super >: Out](func: Super => Future[Unit]): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(AsyncFunc(func))
    to(output)
  }

  // Shortcuts for Connector constructors

  def split(outputCount: Int, outputChooser: Func[Out, BitSet]): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    to(splitter.inputs(0))
    splitter
  }

  def tee(outputCount: Int = 2): Splitter[_ <: Out] = {
    val splitter = Connector.tee[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }

  def roundRobin(outputCount: Int = 2): Splitter[_ <: Out] = {
    val splitter = Connector.roundRobin[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }
}
