package com.fsist.stream

import com.fsist.util.Func

/** NOTE: if onNext or onComplete fails, the component is considered to be in a failed state and calling onError with
  * the same error again is unnecessary. */
trait StreamConsumer[-In, +Res] {
  def onNext: Func[In, Unit]

  def onComplete: Func[Unit, Res]

  def onError: Func[Throwable, Unit]
}

object StreamConsumer {

  def apply[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit]): StreamConsumer[In, Res] =
    Simple(onNext, onComplete, onError)

  /** The three methods of an individual consumer must be called non-concurrently with regard to themselves and each other. */
  case class Simple[-In, +Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit]) extends StreamConsumer[In, Res]
}