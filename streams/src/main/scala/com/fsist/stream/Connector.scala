package com.fsist.stream

import scala.collection.immutable.{IndexedSeq, BitSet}

final case class ConnectorReadiness(readyInputs: BitSet, readyOutputs: BitSet)
final case class ConnectionChoice(incomingIndex: Int, outgoingIndex: Int)

sealed trait Connector[A, B] {
  def inputs: IndexedSeq[Sink[A, Unit]]
  def outputs: IndexedSeq[Source[B]]
}
