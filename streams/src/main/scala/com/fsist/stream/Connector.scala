package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.{SyncFunc, Func}
import com.fsist.util.SyncFunc._

import scala.collection.immutable.{IndexedSeq, BitSet}

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

/** Note that a Connector is not a StreamComponent.
  *
  * TODO if we only have Splitters and Mergers, and if we're sure we won't have anything else in the future, why not
  * simplify this to Connector[T]?
  */
sealed trait Connector[-In, +Out] {
  def inputs: IndexedSeq[ConnectorInput[In, Out]]
  def outputs: IndexedSeq[ConnectorOutput[In, Out]]
  def edges: IndexedSeq[ConnectorEdge[In, Out]] = inputs ++ outputs

  def isSingleInput: Boolean = inputs.length == 1
}

/** Distributes data from one input to several outputs.
  *
  * @param outputChooser called for each input element. Should return the outputs to which this element is copied.
  *                      If an empty BitSet is returned, the element is dropped.
  */
final case class Splitter[T](outputCount: Int, outputChooser: Func[T, BitSet]) extends Connector[T, T] {
  val inputs = Vector(ConnectorInput(this, 0))
  val outputs = for (index <- 0 until outputCount) yield ConnectorOutput(this, index)
}

/** Merges data from several inputs to one output. Ordering is not strictly guaranteed, but the connector will not
  * wait for an input if another input has data available. */
final case class Merger[T](inputCount: Int) extends Connector[T, T] {
  val inputs = for (index <- 0 until inputCount) yield ConnectorInput(this, index)
  val outputs = Vector(ConnectorOutput(this, 0))
}

// NOTES: for a many-to-many Connector, just use a Merger followed by a Splitter.
// A parallelizing connector (where multiple outputs are exercised in parallel) is not implemented yet.

object Connector {
  def split[T](outputCount: Int, outputChooser: Func[T, BitSet]): Splitter[T] = Splitter(outputCount, outputChooser)

  /** Duplicates the input to each output. */
  def tee[T](outputCount: Int = 2): Splitter[T] = {
    val fullBitset = BitSet(0 until outputCount : _*)
    Splitter(outputCount, (t: T) => fullBitset)
  }

  /** Distribute the input among outputs in a round-robin fashion */
  def roundRobin[T](outputCount: Int = 2): Splitter[T] = Splitter(outputCount, new SyncFunc[T, BitSet]{
    private var next = 0
    override def apply(a: T): BitSet = {
      val ret = BitSet(next)
      next += 1
      if (next == outputCount) next = 0
      ret
    }
  })

  def merge[T](inputCount: Int): Merger[T] = Merger(inputCount)
}

