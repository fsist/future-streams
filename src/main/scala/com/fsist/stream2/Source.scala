package com.fsist.stream2

import com.fsist.stream2.run.FutureStreamBuilder
import com.fsist.util.concurrent.{SyncFunc, Func}

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
private[stream2] trait SourceBase[+Out] extends Source[Out]

/** A Source that introduces data into the stream from elsewhere, rather than from an upstream component. */
sealed trait StreamInput[+Out] extends SourceBase[Out] {

  /** Called non-concurrently to produce the source elements. To indicate EOF, this function needs to throw a
    * NoSuchElementException.
    */
  def producer: Func[Unit, Out]

  def onError: Func[Throwable, Unit]
}

/** A Source producing elements from an Iterator. */
final case class IteratorSource[+Out](builder: FutureStreamBuilder, iter: Iterator[Out]) extends StreamInput[Out] with SyncFunc[Unit, Out] {
  override def producer: Func[Unit, Out] = this

  override def apply(unit: Unit): Out = if (iter.hasNext) iter.next() else throw new NoSuchElementException

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

  def generate[Out](producer: Func[Unit, Out], onError: Func[Throwable, Unit] = Func.nop)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    GeneratorSource(builder, producer, onError)
}
