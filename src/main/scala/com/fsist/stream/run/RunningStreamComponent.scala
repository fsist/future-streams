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
    *
    * Note that in the case of successful completion, all graph components complete this future before the graph's
    * `FutureStream.completion` is completed. However, in case of failure, this will typically complete after the graph,
    * because it will wait for the component's onError to complete. It may even never be completed, in case the graph fails,
    * if some call to one of the component's onXxx functions never returns at all.
    */
  def completion: Future[Unit]
}

case class RunningInput[+Out](completion: Future[Unit],
                                    input: StreamInput[Out]) extends RunningStreamComponent

/** @param result succeeds or fails together with `completion`, but contains the calculated result if it succeeds. */
case class RunningOutput[-In, +Res](result: Future[Res])
                                         (implicit ec: ExecutionContext) extends RunningStreamComponent {
  override def completion: Future[Unit] = result map (_ => ())
}

case class RunningTransform[-In, +Out](completion: Future[Unit],
                                       transform: Transform[In, Out]) extends RunningStreamComponent

case class RunningConnector[-In, +Out](completion: Future[Unit],
                                       connector: Connector[In, Out]) extends RunningStreamComponent
