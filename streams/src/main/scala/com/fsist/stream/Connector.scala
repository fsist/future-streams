package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder

import scala.collection.immutable.{IndexedSeq, BitSet}

final case class ConnectorReadiness(readyInputs: BitSet, readyOutputs: BitSet)
final case class ConnectionChoice(incomingIndex: Int, outgoingIndex: Int)

sealed trait ConnectorEdge[-In, +Out] {
  def connector: Connector[In, Out]
  def index: Int
  def isInput: Boolean
}

final case class ConnectorInput[-In, +Out](connector: Connector[In, Out], index: Int)
                                          (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder)
  extends SinkBase[In] with ConnectorEdge[In, Out] {

  override def isInput: Boolean = true
}

final case class ConnectorOutput[-In, +Out](connector: Connector[In, Out], index: Int)
                                          (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder)
  extends SourceBase[Out] with ConnectorEdge[In, Out] {

  override def isInput: Boolean = false
}

/** Note that a Connector is not a StreamComponent. */
sealed trait Connector[-In, +Out] {
  def inputs: IndexedSeq[ConnectorInput[In, Out]]
  def outputs: IndexedSeq[ConnectorOutput[In, Out]]
  def edges: IndexedSeq[ConnectorEdge[In, Out]] = inputs ++ outputs
}
