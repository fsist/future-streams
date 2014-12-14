package com.fsist.stream

import com.fsist.util.Func

/** onNext and onComplete are called non-concurrently, i.e. each call must complete before another call starts.
  *
  * onError is called *concurrently* wrt. onNext and onComplete.
  *
  * If onComplete has finished, onError will not be called even if the stream fails.
  * If onComplete is in progress when the stream fails, onError will be called if onComplete doesn't finish before the
  * call to onError is attempted.
  * The consumer will never observe a call to onNext or onComplete after (the start of) a call to onError.
  */
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