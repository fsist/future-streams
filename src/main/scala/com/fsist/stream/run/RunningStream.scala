package com.fsist.stream.run

import java.util.concurrent.atomic.AtomicLong

import com.fsist.stream._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** An actual stream (or general graph). Once an instance exists, it is already running. */
class RunningStream(val builder: FutureStreamBuilder,
                    val components: Map[ComponentId, RunningStreamComponent],
                    val connectors: Map[ConnectorId[_], RunningConnector[_]],
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

  def get[T](connector: Connector[T]): Option[RunningConnector[T]] =
    connectors.get(connector).map(_.asInstanceOf[RunningConnector[T]])

  def apply[T](connector: Connector[T]): RunningConnector[T] =
    connectors(connector).asInstanceOf[RunningConnector[T]]
}


/** Wraps each stream component for placement in the graph, and makes sure to compare nodes by identity.
  * Otherwise StreamComponent case classes that compare equal (e.g. two Merger(3) nodes) would be confused.
  */
case class ComponentId(value: StreamComponent) {
  override def equals(other: Any): Boolean =
    (other != null) && other.isInstanceOf[ComponentId] && (value eq other.asInstanceOf[ComponentId].value)

  override def hashCode(): Int = System.identityHashCode(value)

  override lazy val toString: String = ComponentId.counter.incrementAndGet().toString
}

object ComponentId {
  implicit def make(value: StreamComponent): ComponentId = ComponentId(value)

  private val counter = new AtomicLong()
}

/** Wraps each Connector when used as a key in a Set or Map.
  *
  * Can't use `Node` here because Connector isn't a StreamComponent.
  */
case class ConnectorId[T](value: Connector[T]) {
  override def equals(other: Any): Boolean =
    (other != null) && other.isInstanceOf[ConnectorId[_]] && (value eq other.asInstanceOf[ConnectorId[_]].value)

  override def hashCode(): Int = System.identityHashCode(value)
}

object ConnectorId {
  implicit def make[T](value: Connector[T]): ConnectorId[T] = ConnectorId(value)
}

