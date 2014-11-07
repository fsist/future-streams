package com.fsist.stream

trait SourceOps[+Out] { self: Source[Out] =>
  def map[Next](mapper: Out => Next): Source[Next] = transform(Transform.map[Out, Next](mapper))

  def foreach[Super >: Out](func: Super => Unit): Sink[Super, Unit] = to(Sink.foreach(func))
}
