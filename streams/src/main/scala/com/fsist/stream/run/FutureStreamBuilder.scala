package com.fsist.stream.run

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream._
import com.fsist.stream.run.StateMachine.{MergerMachine, TransformMachine, OutputMachine, InputMachine}
import com.fsist.util.SyncFunc
import com.typesafe.scalalogging.slf4j.Logging

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder
import scala.concurrent.{Future, Promise, ExecutionContext}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.immutable.Graph

import scala.async.Async._

import com.fsist.util.concurrent.FutureOps._

import SyncFunc._

/** Builds the mutable state describing the stream graph being built, and allows building it into a runnable [[FutureStream]].
  *
  * All operations are concurrent-safe, implemented using compare-and-swap on a single AtomicReference to an immutable State.
  * Since building a graph model usually has no performance issues, this errs in favor of correctness and catching problems
  * early, instead of relying on the user to operate a mutable model.
  *
  * If you use this explicitly, you need to:
  * - `register` all stream components (Sources, Sinks, Connectors and Transforms)
  * - `connect` all components together, so that no Source or Sink remains unconnected
  * - `build` the runnable FutureStream.
  *
  * Normally, however, this class remains implicit in the background. Each StreamComponent constructor creates a new
  * instance of this class if an existing implicit value is not provided; every shortcut method (like `Source.map`)
  * passes its own Builder instance as the implicit parameter to the new component it creates. All components and links
  * are registered with the builder.
  *
  * Whenever two different builder instances meet (by connecting two stream components that were created using separate
  * builders), they become bidirectionally linked. Calling `build` on any of them takes all of them into account, and so
  * it doesn't matter how the state produced by `register` and `connect` is initially distributed between them.
  *
  * After calling `build` once, you can keep modifying the graph (this will not affect the previously built stream),
  * and/or call `build` again, producing a separate stream. However, if any components are not reusable
  * (e.g. any custom Func which is a closure over external mutable state), the behavior of the second and future streams
  * will be undefined.
  */
class FutureStreamBuilder {

  import FutureStreamBuilder._

  private val state = new AtomicReference[State](State())

  @tailrec
  private def alterState(func: State => State): Unit = {
    val old = state.get()
    val altered = func(old)
    if (!state.compareAndSet(old, altered)) alterState(func)
  }

  // == PUBLIC API ==

  /** Adds this component to the graph. If it's already in the graph, it's a no-op.
    *
    * This doesn't link it to any other components; it's a safety measure that lets us detect any unconnected components
    * when `build` is called.
    */
  def register(component: StreamComponent): Unit = alterState(_.map(_ + component))

  /** Irreversibly connects two components together. */
  def connect[In >: Out, Out](source: Source[Out], sink: Sink[In]): Unit = alterState(_.map(_ + DiEdge(source, sink)))

  /** Builds and starts a runnable FutureStream from the current graph. */
  def run()(implicit ec: ExecutionContext): FutureStream = {
    val st = state.get()
    validateBeforeBuilding(st)

    val model = st.graph

    // Create the StateMachine instances

    val allStateMachines = Vector.newBuilder[StateMachine]
    val graphOps = new GraphOps with Logging {
      private lazy val stateMachines = allStateMachines.result()
      private val failure = Promise[Throwable]()
      override def failGraph(th: Throwable): Unit = {
        if (failure.trySuccess(th))
          stateMachines foreach (_.fail(th))
        else {
          val existing = failure.future.value.get.get
          if (th ne existing) {
            logger.trace(s"Discarding additional error $th, already failed with $existing")
          }
        }
      }
    }

    val allConnectors: Set[Connector[_, _]] =
      model.nodes.filter(_.isInstanceOf[ConnectorEdge[_, _]]).map(_.asInstanceOf[ConnectorEdge[_, _]].connector).toSet

    val connectorMachines: Map[Connector[_, _], ConnectorMachine[_]] =
      allConnectors.map({
        case merger: Merger[_] => (merger, new MergerMachine(merger, graphOps))
        case splitter: Splitter[_] => ???
      }).toMap
    allStateMachines ++= connectorMachines.values

    val componentMachines: Map[StreamComponent, StateMachine] =
      (for (node <- model.nodes.toOuter if !node.isInstanceOf[ConnectorEdge[_, _]]) yield {
        node match {
          case input: StreamInput[_] => (input: StreamComponent, new InputMachine(input, graphOps))
          case output: StreamOutput[_, _] => (output: StreamComponent, new OutputMachine(output, graphOps))
          case transform: Transform[_, _] => (transform: StreamComponent, new TransformMachine(transform, graphOps))
        }
      }).toMap
    allStateMachines ++= componentMachines.values

    val sinkMachines: Map[StreamComponent, StateMachineWithInput[_]] =
      componentMachines.filter {
        case (component, machine) => machine.isInstanceOf[StateMachineWithInput[_]]
      }.mapValues(_.asInstanceOf[StateMachineWithInput[_]])

    // Fill in the state machines' .next fields

    for (DiEdge(from: Source[_], to: Sink[_]) <- model.edges.toOuter) {
      componentMachines(from) match {
        case machine: StateMachineWithOneOutput[_] =>
          machine.next = Some(sinkMachines(to).asInstanceOf[StateMachineWithInput[machine.TOut]])
        case other => ???
      }
    }

    // Start the initial machines
    for (machine <- componentMachines.values if machine.isInstanceOf[InputMachine[_]]) {
      Future {
        machine.asInstanceOf[InputMachine[_]].run()
      }
    }

    new FutureStream(this, componentMachines.mapValues(_.running), connectorMachines.mapValues(_.running))
  }

  private def validateBeforeBuilding(state: State): Unit = {
    val model = state.graph

    require(model.isConnected, "Stream graph must be connected")
    require(model.isAcyclic, "Cycles are not yet supported")

    model.degreeNodeSeq(model.OutDegree).map {
      case (degree, innerNode) => (degree, innerNode.value)
    }.foreach {
      _ match {
        case (1, node) => require(!node.isInstanceOf[StreamOutput[_, _]], s"Node $node is a StreamOutput and cannot be connected to another Sink")
        case (0, node) => require(node.isInstanceOf[StreamOutput[_, _]], s"Node $node must be connected to a Sink")
        case (degree, node) if degree > 1 => throw new IllegalArgumentException(s"Node $node cannot be connected to $degree (>1) Sinks at once")
        case _ =>
      }
    }

    model.degreeNodeSeq(model.InDegree).map {
      case (degree, innerNode) => (degree, innerNode.value)
    }.foreach {
      _ match {
        case (1, node) => require(!node.isInstanceOf[StreamInput[_]], s"Node $node is a StreamInput and cannot be connected to another Source")
        case (0, node) => require(node.isInstanceOf[StreamInput[_]], s"Node $node must be connected to a Source")
        case (degree, node) if degree > 1 => throw new IllegalArgumentException(s"Node $node cannot be connected to $degree (>1) Sources at once")
        case _ =>
      }
    }

  }
}

object FutureStreamBuilder {
  /** Type of the edges in the model graph. */
  private type ModelEdge = DiEdge[StreamComponent]

  /** Type of the model graph (as opposed to the built, runnable graph). */
  private type ModelGraph = Graph[StreamComponent, DiEdge]

  /** Complete state of FutureStreamBuilder before `build` is called, describing the model graph. */
  private case class State(graph: ModelGraph = Graph.empty[StreamComponent, DiEdge]) {
    def map(func: ModelGraph => ModelGraph) = copy(graph = func(this.graph))
  }

}
