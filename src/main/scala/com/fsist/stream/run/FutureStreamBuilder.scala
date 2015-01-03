package com.fsist.stream.run

import com.fsist.stream._
import com.fsist.stream.run.StateMachine._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.util.control.NonFatal

import scala.language.implicitConversions

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
class FutureStreamBuilder extends LazyLogging {

  // When two builders are linked, their graphs are modified and their `graph` fields both point to the same StreamGraph
  // instance. Needless to say, this is not concurrent-safe.
  @volatile private var graph = new StreamGraph

  // == PUBLIC API ==

  /** Adds this component to the graph. If it's already in the graph, it's a no-op.
    *
    * This doesn't link it to any other components; it's a safety measure that lets us detect any unconnected components
    * when `build` is called.
    */
  def register(component: StreamComponent): Unit = component match {
    case Pipe(sink, source) =>
      register(sink)
      register(source)
    case other => graph.register(component)
  }

  private def link(other: FutureStreamBuilder): Unit = {
    if (graph ne other.graph) {
      graph.mergeFrom(other.graph)
      other.graph = graph
    }
  }

  /** Irreversibly connects two components together, and links their builders if they are not yet linked. */
  def connect[In >: Out, Out](source: SourceComponent[Out], sink: SinkComponent[In]): Unit = {
    // Replaces Pipes with their contents
    source match {
      case Pipe(_, source) => connect(source, sink)
      case _ => sink match {
        case Pipe(sink, _) => connect(source, sink)
        case _ =>
          graph.connect(source, sink)
          if (source.builder ne this) link(source.builder)
          if (sink.builder ne this) link(sink.builder)
      }
    }
  }

  /** Builds and starts a runnable FutureStream from the current graph. */
  def run()(implicit ec: ExecutionContext): RunningStream = {
    validateBeforeBuilding()

    logger.trace(s"Running stream:\n$graph")

    // Declare here, set later, and graphOps will access it later from its lazy val
    var stateMachinesVector: Vector[StateMachine] = Vector.empty

    val graphOps = new GraphOps with LazyLogging {
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

    val allConnectors = graph.connectors()

    // Create the StateMachine instances

    val connectorMachines: Map[ConnectorId[_], ConnectorMachine[_]] =
      allConnectors.map({
        case node@ConnectorId(merger: Merger[_]) => (node, new MergerMachine(merger, graphOps))
        case node@ConnectorId(splitter: Splitter[_]) => (node, new SplitterMachine(splitter, graphOps))
        case node@ConnectorId(scatterer: Scatterer[_]) => (node, new ScattererMachine(scatterer, graphOps))
      }).toMap

    // All component types other than connectors
    val componentMachines: Map[ComponentId, StateMachine] =
      (for (node@ComponentId(component) <- graph.components.keys if !component.isInstanceOf[ConnectorEdge[_]]) yield {
        component match {
          case input: StreamProducer[_] => (node, new ProducerMachine(input, graphOps))
          case input: DelayedSource[_] => (node, new DelayedSourceMachine(input, graphOps))
          case input: DrivenSource[_] => (node, new DrivenSourceMachine(input, graphOps))
          case output: StreamConsumer[_, _] => (node, new ConsumerMachine(output, graphOps))
          case output: DelayedSink[_, _] => (node, new DelayedSinkMachine(output, graphOps))
          case nop: NopTransform[_] => (node, new NopMachine(nop, graphOps))
          case pipe: DelayedPipe[_, _] => (node, new DelayedPipeMachine(pipe, graphOps))
          case transform: Transform[_, _] => (node, new TransformMachine(transform, graphOps))
          case other => throw new NotImplementedError(other.toString) // Can't really happen, this is to silence the error due to StreamComponentBase not being sealed
        }
      }).toMap

    // All machines including connectors
    val allMachines = componentMachines ++ {
      val builder = Map.newBuilder[ComponentId, StateMachine]
      // Try to write it using connectorMachines.flatMap - you'll get some delicious type errors
      connectorMachines.foreach {
        case (k, v) => k.value.edges.foreach {
          case e => builder += ((e, v))
        }
      }
      builder.result()
    }

    stateMachinesVector = allMachines.values.toVector

    // Only the sink-like machines
    val sinkMachines: Map[ComponentId, StateMachineWithInput[_]] =
      allMachines.filter {
        case (component, machine) => machine.isInstanceOf[StateMachineWithInput[_]]
      }.mapValues(_.asInstanceOf[StateMachineWithInput[_]])

    // Connect the state machines to one another
    for ((source, Some(sink)) <- graph.components) {
      source.value match {
        // If output.connector.outputs.size == 1, it will be handled as a StateMachineWithOneOutput below
        case output: ConnectorOutput[_] if output.connector.outputs.size > 1 =>
          allMachines(source) match {
            case machine: ConnectorMachineWithOutputs[_] =>
              val outputIndex = output.connector.outputs.indexOf(output)
              val outputMachine = sinkMachines(sink).asInstanceOf[StateMachineWithInput[machine.TT]]
              machine.consumers(outputIndex) = Some(outputMachine)
            case other => throw new IllegalArgumentException(s"No others allowed")
          }
        case _ =>
          allMachines(source) match {
            case machine: StateMachineWithOneOutput[_] =>
              machine.next = Some(sinkMachines(sink).asInstanceOf[StateMachineWithInput[machine.TOut]])
            case other => throw new IllegalArgumentException(s"No others allowed")
          }
      }
    }

    // Start the initial machines. Note that `allMachines` can contain the same value many times (for Connectors mapped
    // from multiple StreamComponents which are their edges) so we use .toSet to only `run` each machine once.
    for (machine <- allMachines.values.toSet if machine.isInstanceOf[RunnableMachine]) {
      Future {
        machine.asInstanceOf[RunnableMachine].run()
      } recover {
        case NonFatal(e) =>
          graphOps.failGraph(e)
      }
    }

    new RunningStream(this, componentMachines.mapValues(_.running), connectorMachines.mapValues(_.running), graphOps)
  }

  /** Validate the current state of the `graph` to make sure it's fully connected, etc. before materializing it. */
  private def validateBeforeBuilding(): Unit = {
    try {
      // If the graph isn't connected, some StateMachine will fail because its `next` field will be unset, so we don't
      // need to check that here

      require(graph.isAcyclic, "Cycles are not supported")
    }
    catch {
      case e: IllegalArgumentException =>
        throw new IllegalArgumentException(s"Bad model: $graph", e)
    }
  }

  /** Returns a multiline description of the current stream graph structure, useful for debugging. */
  def describeGraph(): String = graph.toString
}

object FutureStreamBuilder {
  /** Implicitly creates a new Builder whenever one is needed */
  implicit def makeNew: FutureStreamBuilder = new FutureStreamBuilder
}
