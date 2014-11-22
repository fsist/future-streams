package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.{SyncFunc, Func}

sealed trait Source[+Out] extends StreamComponent with SourceOps[Out] {
  def connect[Super >: Out](next: Sink[Super]): next.type = {
    builder.connect(this, next)
    next
  }
}

private[stream] trait SourceBase[+Out] extends Source[Out]

/** A Source that introduces data into the stream from elsewhere. */
sealed trait StreamInput[+Out] extends SourceBase[Out] {

  /** Called non-concurrently. Produces the source elements.
    *
    * Note that this is `producer`, not `produce`. IOW, it is called only once; the Func it returns is then called
    * many times. This allows returning a new Func instance every time, whose internal state keeps track of the iteration.
    * IOW, the SourceBase can behave like an Iterable, with each `producer` being a separate Iterator. However, this is
    * not required from every implementation (since we provide IteratorSource as well as IterableSource).
    *
    * @throws NoSuchElementException on EOF.
    */
  def producer: Func[Unit, Out]
}

final case class IteratorSource[+Out](builder: FutureStreamBuilder, iter: Iterator[Out]) extends StreamInput[Out] with SyncFunc[Unit, Out] {
  override def producer: Func[Unit, Out] = this
  override def apply(unit: Unit): Out = if (iter.hasNext) iter.next() else throw new NoSuchElementException
}

final case class IterableSource[+Out](builder: FutureStreamBuilder, iterable: Iterable[Out]) extends StreamInput[Out] {
  override def producer: Func[Unit, Out] = new SyncFunc[Unit, Out] {
    val iter = iterable.iterator
    override def apply(a: Unit): Out = if (iter.hasNext) iter.next() else throw new NoSuchElementException
  }
}

object Source {
  def apply[Out](ts: Out*)(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    from(ts)

  def from[Out](iter: Iterable[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    IterableSource(builder, iter)

  def from[Out](iter: Iterator[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    IteratorSource(builder, iter)
}
