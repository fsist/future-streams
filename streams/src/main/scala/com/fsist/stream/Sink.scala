package com.fsist.stream

import com.fsist.stream.run.{RunningStreamOutput, FutureStream, FutureStreamBuilder}
import com.fsist.util.Func

import scala.concurrent.ExecutionContext

sealed trait Sink[-In] extends StreamComponent {
}

private[stream] trait SinkBase[-In] extends Sink[In]

/** A Sink that sends data outside the stream, calculates a result, and/or has some other useful side effects.
  *
  * A Sink implementation provides three central functions, `onNext`, `onComplete` and `onError`. These are called
  * non-concurrently wrt. themselves and each other on the series of input elements and, possibly, an input error.
  * If any of them fails, the whole component fails and no more methods are called.
  *
  * These functions are held in a separate class called StreamConsumer. This allows this model (the StreamOutput instance)
  * to be reused in multiple stream instances, as long as the functions inside the StreamConsumer instances it returns
  * every time don't share state.
  */
sealed trait StreamOutput[-In, +Res] extends Sink[In] {
  def build()(implicit ec: ExecutionContext): FutureStream = builder.run()

  def run()(implicit ec: ExecutionContext): FutureStream = builder.run()

  def runAndGet()(implicit ec: ExecutionContext): RunningStreamOutput[In, Res] = run()(ec)(this)

  def consumer(): StreamConsumer[In, Res]
}

private[stream] trait StreamOutputBase[-In, +Res] extends StreamOutput[In, Res]

final case class SimpleOutput[-In, +Res](builder: FutureStreamBuilder, consumer: StreamConsumer[In, Res]) extends StreamOutput[In, Res]

object Sink {
  def foreach[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamOutput[In, Res] =
    SimpleOutput(builder, StreamConsumer(onNext, onComplete, onError))
}
