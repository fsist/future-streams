package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.Func

import scala.concurrent.Future
import scala.language.implicitConversions

/** A part of a stream with a single unconnected input SinkComponent and a single unconnected output SourceComponent.
  *
  * It can represent a single component (a Transform), or a series of components which are already fully connected
  * to one another.
  */
final case class Pipe[-In, +Out](sink: SinkComponent[In], source: SourceComponent[Out])
  extends SinkComponentBase[In] with SourceComponentBase[Out] {

  override implicit def builder: FutureStreamBuilder = sink.builder

  /** Irreversibly connects the `next` pipe after this one.
    *
    * Returns a new Pipe composing them.
    */
  def pipe[Next](next: Pipe[Out, Next]): Pipe[In, Next] = {
    source.connect(next.sink)
    Pipe(sink, next.source)
  }

  /** Irreversibly connects the `next` transform after this pipe.
    *
    * Returns a new Pipe composing them.
    */
  def pipe[Next](next: Transform[Out, Next]): Pipe[In, Next] = {
    source.connect(next)
    Pipe(sink, next)
  }

  /** Irreversibly connects to this `sink`.
    *
    * Returns a new Sink composing this pipe with the original sink.
    */
  def combine[Res](next: Sink[Out, Res]): Sink[In, Res] = {
    source.connect(next.sinkComponent)
    Sink(sink, next.output)
  }
}

object Pipe {
  implicit def apply[In, Out](transform: Transform[In, Out]): Pipe[In, Out] =
    apply(transform, transform)

  /** A pipe containing a transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
  def nop[T]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): Pipe[T, T] = Pipe(Transform.nop[T]()(builder))

  /** The stream will wait for `future` to be completed, and then will materialize and run the provided Pipe. */
  def flatten[In, Out](future:  Future[Pipe[In, Out]],
                       onError: Func[Throwable, Unit] = Func.nop)
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Pipe[In, Out] =
    Pipe(DelayedTransform(builder, future, onError))
}
