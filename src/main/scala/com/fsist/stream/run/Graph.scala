package com.fsist.stream.run

import collection.mutable
import java.util.concurrent.atomic.AtomicLong

import com.fsist.stream._

import scala.annotation.tailrec
import scala.language.implicitConversions

// This file contains a small implementation of a mutable graph. Existing libraries like scala-graph are not performant
// enough (at least by default) because they are too general. We need a very fast implementation of mutable, small digraphs
// to make stream building fast.

/** Wraps each stream component for placement in the graph, and makes sure to compare nodes by identity.
  * Otherwise StreamComponent case classes that compare equal would be confused.
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

/** Mutable graph containing the components and connectors in a stream and the connections between them.
  * This serves as the main part of a FutureStreamBuilder's internal state.
  */
private[run] class StreamGraph {
  /** Component nodes mapped to the sink components to which they are connected */
  val components = new mutable.HashMap[ComponentId, Option[ComponentId]]()

  /** Registers a new component */
  def register(component: ComponentId): Unit = {
    components.getOrElseUpdate(component, None)
  }

  /** Connect two components together (forms an edge) */
  def connect(component: ComponentId, target: ComponentId): Unit = {
    components.put(component, Some(target))
    register(target)
  }

  /** Update this graph to also contain all the data from that other graph. */
  def mergeFrom(graph: StreamGraph): Unit = {
    for ((component, sink) <- graph.components) {
      components.get(component) match {
        case None => components.put(component, sink)
        case Some(None) if sink.isDefined => components.update(component, sink)
        case _ =>
      }
    }
  }

  def connectors(): Set[ConnectorId[_]] =
    (for (component <- components.keys if component.value.isInstanceOf[ConnectorEdge[_]]) yield
      ConnectorId(component.value.asInstanceOf[ConnectorEdge[_]].connector)).toSet

  def isAcyclic(): Boolean = {
    type Node = Either[ComponentId, ConnectorId[_]]

    def nextNodes(node: Node): Seq[Node] = node match {
      case Left(ComponentId(ConnectorInput(connector, _))) => List(Right(connector))
      case Left(comp @ ComponentId(other)) if ! other.isInstanceOf[StreamOutput[_, _]] => List(Left(components(comp).get))
      case Left(_) => List.empty // StreamOutput node
      case Right(ConnectorId(connector)) => connector.outputs map (output => Left[ComponentId, ConnectorId[_]](output))
    }

    // See eg https://en.wikipedia.org/wiki/Topological_sorting#Algorithms

    val visited = new mutable.HashSet[Node]()
    val marked = new mutable.HashSet[Node]()

    def visit(node: Node): Boolean = {
      if (marked.contains(node)) false
      else {
        marked.add(node)
        for (next <- nextNodes(node)) visit(next)
        visited.add(node)
        marked.remove(node)
        true
      }
    }

    components.keys.map(Left.apply).forall {
      node => visited.contains(node) || visit(node)
    }
  }

  override def toString(): String = {
    val sb = new mutable.StringBuilder(100)

    sb ++= s"Nodes: ${components.keys.mkString(", ")}\n"

    sb ++= s"Edges: ${
      ((for ((source, Some(sink)) <- components) yield {
        s"$source ~> $sink"
      }) ++
        (for (node <- components.keys if node.value.isInstanceOf[ConnectorInput[_]];
              input = node.value.asInstanceOf[ConnectorInput[_]];
              output <- input.connector.outputs) yield {
          val inputComponent = components.keySet.find(_.value eq input).get
          val outputComponent = components.keySet.find(_.value eq output).get

          s"$inputComponent ~> $outputComponent"
        })).mkString(", ")
    }\n"

    sb ++= s"Where:\n"

    for (node <- components.keys) {
      sb ++= s"$node\t= ${node.value}\n"
    }

    sb.result()
  }
}
