package com.fsist.stream.run

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream._
import com.fsist.stream.run.StateMachine._
import com.fsist.util.concurrent.SyncFunc
import com.typesafe.scalalogging.slf4j.Logging

import scala.annotation.tailrec
import scala.collection.immutable.VectorBuilder
import scala.concurrent.{Future, Promise, ExecutionContext}
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.immutable.Graph

/** Builds the mutable state describing the stream graph being built, and allows building it into a runnable [[RunningStream]].
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
class FutureStreamBuilder extends Logging {

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
  def register(component: StreamComponent): Unit = alterState(_.mapGraph(_ + component))

  private def link(builder: FutureStreamBuilder): Unit = {
    if (!state.get.linked.contains(builder)) {
      alterState(_.mapLinked(_ + builder))
      builder.link(this)
    }
  }

  /** Irreversibly connects two components together, and possibly links their builders. */
  def connect[In >: Out, Out](source: Source[Out], sink: Sink[In]): Unit = {
    alterState(_.mapGraph(_ + DiEdge[StreamComponent](source, sink)))
    if (source.builder ne this) link(source.builder)
    if (sink.builder ne this) link(sink.builder)
  }

  private def collectLinkedBuilders(seen: Set[FutureStreamBuilder] = Set.empty,
                                    next: FutureStreamBuilder = this): Set[FutureStreamBuilder] = {
    if (seen.contains(next)) seen
    else {
      next.state.get().linked.foldLeft(seen + next)(collectLinkedBuilders)
    }
  }

  private def mergeLinkedStates(): State =
    collectLinkedBuilders().foldLeft(State()) {
      case (state, builder) =>
        val st = builder.state.get
        state.merge(st)
    }

  /** Builds and starts a runnable FutureStream from the current graph. */
  def run()(implicit ec: ExecutionContext): RunningStream = {
    val st = mergeLinkedStates()
    validateBeforeBuilding(st)
    val model = st.graph

    // Declare here, set later, and graphOps will access it later from its lazy val
    var stateMachinesVector : Vector[StateMachine] = Vector.empty

    val graphOps = new GraphOps with Logging {
      private lazy val stateMachines = stateMachinesVector
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

    val allConnectors: Set[Connector[_]] =
      model.nodes.toOuter.filter(_.isInstanceOf[ConnectorEdge[_]]).map(_.asInstanceOf[ConnectorEdge[_]].connector).toSet

    // Create the StateMachine instances

    val connectorMachines: Map[Connector[_], ConnectorMachine[_]] =
      allConnectors.map({
        case merger: Merger[_] => (merger, new MergerMachine(merger, graphOps))
        case splitter: Splitter[_] => (splitter, new SplitterMachine(splitter, graphOps))
        case scatterer: Scatterer[_] => (scatterer, new ScattererMachine(scatterer, graphOps))
      }).toMap

    // All component types other than connectors
    val componentMachines: Map[StreamComponent, StateMachine] =
      (for (node <- model.nodes.toOuter if !node.isInstanceOf[ConnectorEdge[_]]) yield {
        node match {
          case input: StreamInput[_] => (input: StreamComponent, new InputMachine(input, graphOps))
          case output: StreamOutput[_, _] => (output: StreamComponent, new OutputMachine(output, graphOps))
          case transform: Transform[_, _] => (transform: StreamComponent, new TransformMachine(transform, graphOps))
          case other => ??? // Can't really happen, this is to silence the error due to StreamComponentBase not being sealed
        }
      }).toMap

    // All machines including connectors
    val allMachines = componentMachines ++ {
      val builder = Map.newBuilder[StreamComponent, StateMachine]
      // Try to write it using connectorMachines.flatMap - you'll get some delicious type errors
      connectorMachines.foreach {
        case (k, v) => k.edges.foreach {
          case e => builder += ((e, v))
        }
      }
      builder.result()
    }

    stateMachinesVector = allMachines.values.toVector

    // Only the sink-like machines
    val sinkMachines: Map[StreamComponent, StateMachineWithInput[_]] =
      allMachines.filter {
        case (component, machine) => machine.isInstanceOf[StateMachineWithInput[_]]
      }.mapValues(_.asInstanceOf[StateMachineWithInput[_]])

    // Connect the state machines to one another

    for (DiEdge(from: Source[_], to: Sink[_]) <- model.edges.toOuter) {
      from match {
        // If output.connector.outputs.size == 1, it will be handled as a StateMachineWithOneOutput below
        case output: ConnectorOutput[_] if output.connector.outputs.size > 1 =>
          allMachines(from) match {
            case machine: ConnectorMachineWithOutputs[_] =>
              val outputIndex = output.connector.outputs.indexOf(output)
              val outputMachine = sinkMachines(to).asInstanceOf[StateMachineWithInput[machine.TT]]
              machine.consumers(outputIndex) = Some(outputMachine)
            case other => throw new IllegalArgumentException(s"No others allowed")
          }
        case _ =>
          allMachines(from) match {
            case machine: StateMachineWithOneOutput[_] =>
              machine.next = Some(sinkMachines(to).asInstanceOf[StateMachineWithInput[machine.TOut]])
            case other => throw new IllegalArgumentException(s"No others allowed")
          }
      }
    }

    // Start the initial machines
    for (machine <- allMachines.values if machine.isInstanceOf[RunnableMachine]) {
      Future {
        machine.asInstanceOf[RunnableMachine].run()
      }
    }

    new RunningStream(this, componentMachines.mapValues(_.running), connectorMachines.mapValues(_.running), graphOps)
  }

  private def validateBeforeBuilding(state: State): Unit = {
    val model = state.graph

    // TODO this fails because it notices, correctly, that the input- and output-side of Connectors are not connected,
    // because the Connector itself isn't in the model
    //    require(model.isConnected, "Stream graph must be connected")

    require(model.isAcyclic, "Cycles are not yet supported")

    for (DiEdge(from, to) <- model.edges.toOuter) {
      require(model.contains(from), s"Graph must contain all linked nodes, missing $from (linked to $to)")
      require(model.contains(to), s"Graph must contain all linked nodes, missing $to (linked from $from)")
    }

    model.degreeNodeSeq(model.OutDegree).map {
      case (degree, innerNode) => (degree, innerNode.value)
    }.foreach {
      _ match {
        case (1, node) => require(!node.isInstanceOf[StreamOutput[_, _]], s"Node $node is a StreamOutput and cannot be connected to another Sink")
        case (0, node) => require(node.isInstanceOf[StreamOutput[_, _]] || node.isInstanceOf[ConnectorInput[_]], s"Node $node must be connected to a Sink")
        case (degree, node) if degree > 1 => throw new IllegalArgumentException(s"Node $node cannot be connected to $degree (>1) Sinks at once")
        case _ =>
      }
    }

    model.degreeNodeSeq(model.InDegree).map {
      case (degree, innerNode) => (degree, innerNode.value)
    }.foreach {
      _ match {
        case (1, node) => require(!node.isInstanceOf[StreamInput[_]], s"Node $node is a StreamInput and cannot be connected to another Source")
        case (0, node) => require(node.isInstanceOf[StreamInput[_]] || node.isInstanceOf[ConnectorOutput[_]], s"Node $node must be connected to a Source")
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
  private case class State(graph: ModelGraph = Graph.empty, linked: Set[FutureStreamBuilder] = Set.empty) {
    def mapGraph(func: ModelGraph => ModelGraph): State = copy(graph = func(this.graph))

    def mapLinked(func: Set[FutureStreamBuilder] => Set[FutureStreamBuilder]): State = copy(linked = func(this.linked))

    def merge(other: State): State = State(graph ++ other.graph, linked ++ other.linked)
  }

}
