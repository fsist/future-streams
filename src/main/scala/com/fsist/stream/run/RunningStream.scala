package com.fsist.stream.run

import com.fsist.stream._

import scala.concurrent.{ExecutionContext, Future}

/** An actual stream (or general graph). Once an instance exists, it is already running. */
class RunningStream(val builder: FutureStreamBuilder,
                   val components: Map[StreamComponent, RunningStreamComponent],
                   val connectors: Map[Connector[_, _], RunningConnector[_, _]],
                   graphOps: GraphOps)
                  (implicit ec: ExecutionContext) {

  /** Completes when all stream components have completed (as exposed by their `completion` future).
    *
    * NOTE: if the graph fails, this future may complete (with a failure) before all component onError methods have
    * been called or competed. It is guaranteed to complete even if some components' onNext never return and so
    * their onError is never called.
    */
  def completion: Future[Unit] = streamCompletion

  // Once all Outputs have completed, all other components must have completed as well.
  private lazy val streamCompletion: Future[Unit] =
    Future.sequence(components.values.filter(_.isInstanceOf[RunningOutput[_, _]]).map(_.completion)) map (_ => ())

  /** Fails the running stream by injecting an exception from outside.
    *
    * All stream components that have not yet completed will get a call to onError.
    * All `completion` and `result` Futures that have not yet completed will be failed with this throwable.
    *
    * Due to race conditions, some onNext methods may be called in parallel or slightly after onError for the same
    * components, and after the `completion` future of the entire stream has been failed.
    */
  def fail(throwable: Throwable): Unit = graphOps.failGraph(throwable)

  // Convenience accessors follow

  def get[In, Res](output: StreamOutput[In, Res]): Option[RunningOutput[In, Res]] =
    components.get(output).map(_.asInstanceOf[RunningOutput[In, Res]])

  def apply[In, Res](output: StreamOutput[In, Res]): RunningOutput[In, Res] =
    components(output).asInstanceOf[RunningOutput[In, Res]]

  def get[Out](input: StreamInput[Out]): Option[RunningInput[Out]] =
    components.get(input).map(_.asInstanceOf[RunningInput[Out]])

  def apply[Out](input: StreamInput[Out]): RunningInput[Out] =
    components(input).asInstanceOf[RunningInput[Out]]

  def get[In, Out](transform: Transform[In, Out]): Option[RunningTransform[In, Out]] =
    components.get(transform).map(_.asInstanceOf[RunningTransform[In, Out]])

  def apply[In, Out](transform: Transform[In, Out]): RunningTransform[In, Out] =
    components(transform).asInstanceOf[RunningTransform[In, Out]]

  def get[In, Out](connector: Connector[In, Out]): Option[RunningConnector[In, Out]] =
    connectors.get(connector).map(_.asInstanceOf[RunningConnector[In, Out]])

  def apply[In, Out](connector: Connector[In, Out]): RunningConnector[In, Out] =
    connectors(connector).asInstanceOf[RunningConnector[In, Out]]
}

