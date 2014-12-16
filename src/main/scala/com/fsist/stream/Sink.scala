package com.fsist.stream

import com.fsist.stream.run.{RunningOutput, RunningStream, FutureStreamBuilder}
import com.fsist.util.concurrent.{SyncFunc, Func}

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Future, ExecutionContext}

import scala.language.higherKinds

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
  def build()(implicit ec: ExecutionContext): RunningStream = builder.run()

  // TODO rename to something nicer
  def buildAndGet()(implicit ec: ExecutionContext): RunningOutput[In, Res] = build()(ec)(this)
  def buildResult()(implicit ec: ExecutionContext): Future[Res] = buildAndGet()(ec).result

  def consumer(): StreamConsumer[In, Res]
}

private[stream] trait StreamOutputBase[-In, +Res] extends StreamOutput[In, Res]

final case class SimpleOutput[-In, +Res](builder: FutureStreamBuilder, consumer: StreamConsumer[In, Res]) extends StreamOutput[In, Res]

object SimpleOutput {
  def apply[In, Res](consumer: StreamConsumer[In, Res])
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): SimpleOutput[In, Res] =
    apply(builder, consumer)
}

object Sink {
  def foreach[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamOutput[In, Res] =
    SimpleOutput(builder, StreamConsumer(onNext, onComplete, onError))

  def foldLeft[In, Res, State](init: State)(onNext: Func[(In, State), State], onComplete: Func[State, Res],
                                            onError: Func[Throwable, Unit] = Func.nop)
                              (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[In, Res] = {
    val (userOnNext, userOnComplete, userOnError) = (onNext, onComplete, onError)

    SimpleOutput(builder, new StreamConsumer[In, Res] {
      private var state: State = init

      override def onNext: Func[In, Unit] = SyncFunc((in: In) => (in, state)) ~> userOnNext ~> SyncFunc((st: State) => state = st)

      override def onError: Func[Throwable, Unit] = userOnError

      override def onComplete: Func[Unit, Res] = SyncFunc(state) ~> userOnComplete
    })
  }

  def collect[In, M[_]](onError: Func[Throwable, Unit] = Func.nop)
                       (implicit cbf: CanBuildFrom[Nothing, In, M[In]],
                        builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[In, M[In]] = {
    val userOnError = onError

    SimpleOutput(builder, new StreamConsumer[In, M[In]] {
      val m = cbf.apply()

      override def onNext: Func[In, Unit] = SyncFunc(m += _)

      override def onError: Func[Throwable, Unit] = userOnError

      override def onComplete: Func[Unit, M[In]] = m.result()
    })
  }
}
