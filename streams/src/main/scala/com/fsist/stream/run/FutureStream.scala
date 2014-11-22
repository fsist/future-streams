package com.fsist.stream.run

import com.fsist.stream._

import scala.concurrent.{ExecutionContext, Future}

/** An actual stream (or general graph). Once an instance exists, it is already running. */
class FutureStream(val builder: FutureStreamBuilder,
                   val components: Map[StreamComponent, RunningStreamComponent],
                   val connectors: Map[Connector[_, _], RunningConnector[_, _]])
                  (implicit ec: ExecutionContext) {
  /** Completes when all stream components have completed (as exposed by their `completion` future). */
  def completion: Future[Unit] = streamCompletion
  private lazy val streamCompletion: Future[Unit] =
    Future.sequence(components.values.map(_.completion) ++ connectors.values.map(_.completion)) map (_ => ())

  // Convenience accessors follow

  def get[In, Res](output: StreamOutput[In, Res]): Option[RunningStreamOutput[In, Res]] =
    components.get(output).map(_.asInstanceOf[RunningStreamOutput[In, Res]])

  def apply[In, Res](output: StreamOutput[In, Res]): RunningStreamOutput[In, Res] =
    components(output).asInstanceOf[RunningStreamOutput[In, Res]]

  def get[Out](input: StreamInput[Out]): Option[RunningStreamInput[Out]] =
    components.get(input).map(_.asInstanceOf[RunningStreamInput[Out]])

  def apply[Out](input: StreamInput[Out]): RunningStreamInput[Out] =
    components(input).asInstanceOf[RunningStreamInput[Out]]

  def get[In, Out](transform: Transform[In, Out]): Option[RunningTransform[In, Out]] =
    components.get(transform).map(_.asInstanceOf[RunningTransform[In, Out]])

  def apply[In, Out](transform: Transform[In, Out]): RunningTransform[In, Out] =
    components(transform).asInstanceOf[RunningTransform[In, Out]]

  def get[In, Out](connector: Connector[In, Out]): Option[RunningConnector[In, Out]] =
    connectors.get(connector).map(_.asInstanceOf[RunningConnector[In, Out]])

  def apply[In, Out](connector: Connector[In, Out]): RunningConnector[In, Out] =
    connectors(connector).asInstanceOf[RunningConnector[In, Out]]
}

