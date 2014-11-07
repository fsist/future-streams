package com.fsist.stream

sealed trait Source[+Out] extends StreamComponent with SourceOps[Out] {
  def to[Super >: Out, Res](sink: Sink[Super, Res]): sink.type = {
    builder.connect(this, sink)
    sink
  }

  def transform[Super >: Out, Next](tr: Transform[Super, Next]): tr.type = {
    builder.connect(this, tr)
    tr
  }
}

/** Package-private trait so we can extend the sealed trait Source in other compilation units */
private[stream] trait SourceBase[+Out] extends Source[Out]

final case class IteratorSource[+Out](builder: StreamBuilder, iter: Iterator[Out]) extends SourceBase[Out]

final case class IterableSource[+Out](builder: StreamBuilder, iter: Iterable[Out]) extends SourceBase[Out]

object Source {
  def apply[Out](ts: Out*)(implicit builder: StreamBuilder = new StreamBuilder()): Source[Out] =
    IterableSource(builder, ts)

  def from[Out](iter: Iterable[Out])(implicit builder: StreamBuilder = new StreamBuilder()): Source[Out] =
    IterableSource(builder, iter)

  def from[Out](iter: Iterator[Out])(implicit builder: StreamBuilder = new StreamBuilder()): Source[Out] =
    IteratorSource(builder, iter)
}
