package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder

import scala.language.implicitConversions

/** A part of a stream with a single input Sink and a single output Source.
  *
  * It can represent a single component (a Transform), or a series of components which are already fully connected
  * to one another.
  */
final case class Pipe[-In, +Out](builder: FutureStreamBuilder, sink: Sink[In], source: Source[Out])
  extends SinkBase[In] with SourceBase[Out] {

  /** Irreversibly connects the `next` pipe after this one.
    *
    * Returns a new Pipe composing them.
    */
  def pipe[Next](next: Pipe[Out, Next]): Pipe[In, Next] = {
    source.connect(next.sink)
    Pipe(builder, sink, next.source)
  }

  /** Irreversibly connects the `next` transform after this pipe.
    *
    * Returns a new Pipe composing them.
    */
  def pipe[Next](next: Transform[Out, Next]): Pipe[In, Next] = {
    source.connect(next)
    Pipe(builder, sink, next)
  }
}

object Pipe {
  def apply[In, Out](sink: Sink[In], source: Source[Out])
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): Pipe[In, Out] =
    apply(builder, sink, source)

  implicit def apply[In, Out](transform: Transform[In, Out])
                             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): Pipe[In, Out] =
    apply(builder, transform, transform)

  /** A pipe containing a transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
  def nop[T]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): Pipe[T, T] = Pipe(Transform.nop[T]()(builder))
}
