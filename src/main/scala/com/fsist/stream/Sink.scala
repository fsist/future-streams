package com.fsist.stream

import com.fsist.stream.run.{RunningOutput, RunningStream, FutureStreamBuilder}
import com.fsist.util.concurrent.{SyncFunc, Func}

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Future, ExecutionContext}

import scala.language.higherKinds

/** A Sink is any stream component that receives input elements. */
sealed trait Sink[-In] extends StreamComponentBase {
}

/** This trait allows extending the sealed Sink trait inside this package. */
private[stream] trait SinkBase[-In] extends Sink[In]

/** A Sink that sends data outside the stream, calculates a result, and/or has some other useful side effects.
  *
  * See the README for the semantics of the three onXxx functions.
  */
sealed trait StreamOutput[-In, +Res] extends Sink[In] {
  /** Materializes the stream, including all components linked (transitively) to this one, and starts running it.
    *
    * The same result is produced no matter which stream component is used to call `build`.
    *
    * This method may only be called once per stream (see README on the subject of reusing components).
    */
  def build()(implicit ec: ExecutionContext): RunningStream = builder.run()

  /** A shortcut method that calls `build` and returns the RunningStreamComponent representing `this`. */
  def buildAndGet()(implicit ec: ExecutionContext): RunningOutput[In, Res] = build()(ec)(this)

  /** A shortcut method that calls `build` and returns the future result produced by this component. */
  def buildResult()(implicit ec: ExecutionContext): Future[Res] = buildAndGet()(ec).result

  /** Called on each input element, non-concurrently with itself and onComplete. */
  def onNext: Func[In, Unit]

  /** Called on each input element, non-concurrently with itself and onNext.
    *
    * Can only be called once, and no more calls to onNext are allowed afterwards. */
  def onComplete: Func[Unit, Res]

  /** Called if the stream fails. See the README on the semantics of stream failure. This method is guaranteed to be called
    * exactly once.
    *
    * This is called *concurrently* with onNext and onComplete.
    */
  def onError: Func[Throwable, Unit]
}

/** This trait allows extending the sealed StreamOutput trait inside this package. */
private[stream] trait StreamOutputBase[-In, +Res] extends StreamOutput[In, Res]

/** A StreamOutput represented as a triplet of onXxx functions.
  *
  * @see [[com.fsist.stream.StreamOutput]]
  */
final case class SimpleOutput[-In, +Res](builder: FutureStreamBuilder,
                                         onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit]) extends StreamOutput[In, Res]

object SimpleOutput {
  def apply[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit])
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): SimpleOutput[In, Res] =
    apply(builder, onNext, onComplete, onError)
}

object Sink {
  def foreach[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamOutput[In, Res] =
    SimpleOutput(builder, onNext, onComplete, onError)

  def foldLeft[In, Res](init: Res)(onNext: Func[(In, Res), Res],
                                   onError: Func[Throwable, Unit] = Func.nop)
                       (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[In, Res] = {
    val (userOnNext, userOnError) = (onNext, onError)
    val b = builder

    new StreamOutput[In, Res] {
      override def builder: FutureStreamBuilder = b

      private var state: Res = init

      override def onNext: Func[In, Unit] = SyncFunc((in: In) => (in, state)) ~> userOnNext ~> SyncFunc((st: Res) => state = st)

      override def onError: Func[Throwable, Unit] = userOnError

      override def onComplete: Func[Unit, Res] = SyncFunc(state)

    }
  }

  /** Collect all input elements in a collection of type `M`, and produce it as the result. */
  def collect[In, M[_]]()(implicit cbf: CanBuildFrom[Nothing, In, M[In]],
                          builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[In, M[In]] = {
    val b = builder

    new StreamOutput[In, M[In]] {
      override def builder: FutureStreamBuilder = b

      private val m = cbf.apply()

      override def onNext: Func[In, Unit] = SyncFunc(m += _)

      override def onError: Func[Throwable, Unit] = Func.nop

      override def onComplete: Func[Unit, M[In]] = m.result()
    }
  }
}

