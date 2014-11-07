package com.fsist.stream

import scala.concurrent.ExecutionContext

sealed trait Sink[-In, +Res] extends StreamComponent {
  def build()(implicit ec: ExecutionContext): FutureStream = builder.build()

  def run()(implicit ec: ExecutionContext): FutureStream = {
    val stream = builder.build()
    stream.run()
    stream
  }

  def runAndGet()(implicit ec: ExecutionContext): RunningSink[In, Res] = run().apply(this)
}

/** Package-private trait so we can extend the sealed trait Sink in other compilation units */
private[stream] trait SinkBase[-In, +Res] extends Sink[In, Res]

final case class SimpleSink[-In](builder: StreamBuilder, func: Func[In, Unit]) extends SinkBase[In, Unit]

object Sink {
  def foreach[In](func: In => Unit)(implicit builder: StreamBuilder = new StreamBuilder()): Sink[In, Unit] =
    SimpleSink(builder, func)
}
