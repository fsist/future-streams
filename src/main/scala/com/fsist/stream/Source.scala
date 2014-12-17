package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.{AsyncFunc, SyncFunc, Func}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.ControlThrowable

/** A Source is any stream component that produces elements to a downstream Sink. */
sealed trait Source[+Out] extends StreamComponentBase with SourceOps[Out] {

  /** Irreversibly join this source with that sink. */
  def connect(next: Sink[Out]): next.type = {
    builder.connect(this, next)
    next
  }

  /** Irreversibly join this source with that sink. */
  def to(sink: Sink[Out]): sink.type = connect(sink)

  /** Irreversibly join this source with that transform. */
  def transform[Next](tr: Transform[Out, Next]): tr.type = connect(tr)
}

/** This trait allows extending the sealed Source trait inside this package. */
private[stream] trait SourceBase[+Out] extends Source[Out]

/** Marker trait of all exceptions used by the streams library for flow control.
  *
  * NOTE that extending ControlThrowable means scala.util.control.NonFatal does NOT catch exceptions of this type.
  */
sealed trait StreamControlThrowable extends ControlThrowable

/** Thrown by StreamInput.producer to indicate the stream has completed. */
case class EndOfStreamException() extends Exception("End of stream")

/** A Source that introduces data into the stream from elsewhere, rather than from an upstream component. */
sealed trait StreamInput[+Out] extends SourceBase[Out] {

  /** Called non-concurrently to produce the source elements. To indicate EOF, this function needs to throw a
    * EndOfStreamException.
    */
  def producer: Func[Unit, Out]

  def onError: Func[Throwable, Unit]
}

/** A trait that allows implementing a custom StreamInput that produces items synchronously.
  *
  * This often allows writing more elegant code for complex stateful producers.
  */
trait SyncStreamInput[+Out] extends StreamInput[Out] with SyncFunc[Unit, Out] {
  final override def producer: Func[Unit, Out] = this
  final override def apply(a: Unit): Out = produce()
  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  override lazy val builder: FutureStreamBuilder = new FutureStreamBuilder

  /** Called to produce each successive element in the stream. Should throw a EndOfStreamException to indicate EOF.
    *
    * Equivalent to StreamInput.producer. See the README for concurrency issues.
    */
  def produce(): Out

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}

/** A trait that allows implementing a custom StreamInput that produces items asynchronously.
  *
  * This often allows writing more elegant code for complex stateful producers.
  */
trait AsyncStreamInput[+Out] extends StreamInput[Out] with AsyncFunc[Unit, Out]{
  final override def producer: Func[Unit, Out] = this
  final override def apply(a: Unit)(implicit ec: ExecutionContext): Future[Out] = produce()(ec)
  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  override lazy val builder: FutureStreamBuilder = new FutureStreamBuilder

  /** Called to produce each successive element in the stream. Should throw a EndOfStreamException to indicate EOF.
    *
    * Equivalent to StreamInput.producer. See the README for concurrency issues.
    */
  def produce()(implicit ec: ExecutionContext): Future[Out]

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}

/** A Source producing elements from an Iterator. */
final case class IteratorSource[+Out](builder: FutureStreamBuilder, iter: Iterator[Out]) extends StreamInput[Out] with SyncFunc[Unit, Out] {
  override def producer: Func[Unit, Out] = this

  override def apply(unit: Unit): Out = if (iter.hasNext) iter.next() else throw new EndOfStreamException

  def onError: Func[Throwable, Unit] = Func.nop
}

/** A Source that generates elements by calling a user-supplied `producer` function. */
final case class GeneratorSource[+Out](builder: FutureStreamBuilder, producer: Func[Unit, Out], onError: Func[Throwable, Unit]) extends StreamInput[Out]

object Source {
  def apply[Out](ts: Out*)(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    from(ts)

  def from[Out](iter: Iterable[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    from(iter.iterator)

  def from[Out](iter: Iterator[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    IteratorSource(builder, iter)

  def generateFunc[Out](producer: Func[Unit, Out], onError: Func[Throwable, Unit] = Func.nop)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    GeneratorSource(builder, producer, onError)

  def generate[Out](producer: => Out, onError: Throwable => Unit = Func.nopLiteral)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    generateFunc(SyncFunc(producer), SyncFunc(onError))

  def generateAsync[Out](producer: Unit => Future[Out], onError: Throwable => Unit = Func.nopLiteral)
                        (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    generateFunc(AsyncFunc(producer), SyncFunc(onError))

  /** Creates a Source that produces no elements. This is just an alias for `apply`. */
  def empty[Out]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] = apply()
}
