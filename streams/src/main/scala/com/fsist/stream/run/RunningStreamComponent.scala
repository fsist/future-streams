package com.fsist.stream.run

import com.fsist.stream.{Connector, Transform, StreamOutput, StreamInput}

import scala.concurrent.{ExecutionContext, Future}

sealed trait RunningStreamComponent {
  /** The completion (or failure) of this individual component.
    *
    * A StreamInput, Transform or Connector completes after it generates an EOF or an error and passes it to the connected
    * Sink, or to all chosen Sinks in the case of a Connector.
    *
    * A StreamOutput completes when its onComplete or onError function completes.
    */
  def completion: Future[Unit]
}

case class RunningStreamInput[+Out](completion: Future[Unit],
                                    input: StreamInput[Out]) extends RunningStreamComponent

/** @param result succeeds or fails together with `completion`, but contains the calculated result if it succeeds. */
case class RunningStreamOutput[-In, +Res](result: Future[Res])
                                         (implicit ec: ExecutionContext) extends RunningStreamComponent {
  override def completion: Future[Unit] = result map (_ => ())
}

case class RunningTransform[-In, +Out](completion: Future[Unit],
                                       transform: Transform[In, Out]) extends RunningStreamComponent

case class RunningConnector[-In, +Out](completion: Future[Unit],
                                       connector: Connector[In, Out]) extends RunningStreamComponent
