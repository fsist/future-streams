package com.fsist.stream

sealed trait Transform[-In, +Out] extends SourceBase[Out] with SinkBase[In, Unit]

final case class SingleTransform[-In, +Out](builder: StreamBuilder, mapper: Func[In, Out]) extends Transform[In, Out]

final case class MultiTransform[-In, +Out](builder: StreamBuilder, mapper: Func[In, TraversableOnce[Out]]) extends Transform[In, Out]

object Transform {
  def map[In, Out](mapper: Func[In, Out]): Transform[In, Out] = SingleTransform(null, mapper)
}

