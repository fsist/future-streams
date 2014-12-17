package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.{Func, SyncFunc}
import SyncFunc._

import scala.collection.immutable
import scala.collection.immutable.{IndexedSeq, BitSet}

/** Common trait for the inputs and outputs of a Connector. */
sealed trait ConnectorEdge[T] extends StreamComponent {
  def connector: Connector[T]

  /** Index in the connector's list of inputs or outputs (not the list of all edges). */
  def index: Int

  /** True iff this is a ConnectorInput. */
  def isInput: Boolean
}

/** A Sink which inputs data into a connector.
  *
  * This type allows connecting Sources to a Connector, which is not itself a StreamComponent of any kind.
  */
final case class ConnectorInput[T](connector: Connector[T], index: Int)
                                          (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder)
  extends SinkBase[T] with ConnectorEdge[T] {

  override def isInput: Boolean = true
}

/** A Source which outputs data from a connector.
  *
  * This type allows connecting Sinks to a Connector, which is not itself a StreamComponent of any kind.
  */
final case class ConnectorOutput[T](connector: Connector[T], index: Int)
                                           (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder)
  extends SourceBase[T] with ConnectorEdge[T] {

  override def isInput: Boolean = false
}

/** Connectors represent the only ways to connect multiple inputs to one output, or multiple outputs to one input.
  *
  * They never change the elements going through them, which is also why there is only one type parameter `T` for both
  * input and output.
  */
sealed trait Connector[T] {
  /** The inputs to this Connector. They are fixed; this method will always return the exact same sequence. */
  def inputs: IndexedSeq[ConnectorInput[T]]

  /** Connects these sources to our inputs, in order.
    *
    * This is a convenience method only; you can call connect() on each ConnectorInput individually.
    *
    * @throws IllegalArgumentException if the size of `sources` isn't the same as the size of `this.inputs`.
    */
  def connectInputs(sources: immutable.Seq[Source[T]]): this.type = {
    require(sources.size == inputs.size, s"Must pass the same number of sources as we have inputs, was ${sources.size} vs ${inputs.size}")

    for ((source, input) <- sources zip inputs) source.connect(input)

    this
  }

  /** The outputs from this Connector. They are fixed; this method will always return the exact same sequence. */
  def outputs: IndexedSeq[ConnectorOutput[T]]

  /** Connects these sinks to our outputs, in order.
    *
    * This is a convenience method only; you can call connect() on each ConnectorOutput individually.
    *
    * @throws IllegalArgumentException if the size of `sinks` isn't the same as the size of `this.outputs`.
    */
  def connectOutputs(sinks: immutable.Seq[Sink[T]]): this.type = {
    require(sinks.size == outputs.size, s"Must pass the same number of sinks as we have outputs, was ${sinks.size} vs ${outputs.size}")

    for ((sink, output) <- sinks zip outputs) output.connect(sink)

    this
  }

  /** The inputs and outputs of this Connector. */
  def edges: IndexedSeq[ConnectorEdge[T]] = inputs ++ outputs
}

/** Distributes data from one input to several outputs. Each chosen output is called sequentially and must complete
  * handling the element before the next output is called.
  *
  * @param outputChooser called for each input element. Should return the outputs to which this element is copied.
  *                      If an empty BitSet is returned, the element is dropped.
  */
final case class Splitter[T](outputCount: Int, outputChooser: Func[T, BitSet])
                            (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder) extends Connector[T] {
  require(outputCount > 0, "Must have at least one output")

  val inputs = Vector(ConnectorInput(this, 0))
  def input = inputs(0)

  val outputs = for (index <- 0 until outputCount) yield ConnectorOutput(this, index)
}

/** Distributes data from one input to several outputs in parallel.
  *
  * Each output is driven asynchronously. For each input element, the first available output is picked. If all outputs
  * are busy when an input element arrives, we wait for any output to become available.
  */
final case class Scatterer[T](outputCount: Int)
                             (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder) extends Connector[T] {
  require(outputCount > 0, "Must have at least one output")

  val inputs = Vector(ConnectorInput(this, 0))
  def input = inputs(0)
  val outputs = for (index <- 0 until outputCount) yield ConnectorOutput(this, index)
}

/** Merges data from several inputs to one output. Ordering is not strictly guaranteed, but the connector will not
  * wait for an input if another input has data available.
  */
final case class Merger[T](inputCount: Int)
                          (implicit val builder: FutureStreamBuilder = new FutureStreamBuilder) extends Connector[T] {
  require(inputCount > 0, "Must have at least one input")

  val inputs = for (index <- 0 until inputCount) yield ConnectorInput(this, index)
  val outputs = Vector(ConnectorOutput(this, 0))
  def output = outputs(0)
}

object Connector {
  /** @see [[com.fsist.stream.Splitter]] */
  def split[T](outputCount: Int, outputChooser: Func[T, BitSet])
              (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[T] = Splitter(outputCount, outputChooser)

  /** Duplicates all input to several output streams. */
  def tee[T](outputCount: Int)
            (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[T] = {
    val fullBitset = BitSet(0 until outputCount: _*)
    Splitter(outputCount, (t: T) => fullBitset)
  }

  /** Distributes the input among outputs in a round-robin fashion. */
  def roundRobin[T](outputCount: Int)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[T] = Splitter(outputCount, new SyncFunc[T, BitSet] {
    private var next = 0

    override def apply(a: T): BitSet = {
      val ret = BitSet(next)
      next += 1
      if (next == outputCount) next = 0
      ret
    }
  })

  /** @see [[com.fsist.stream.Merger]] */
  def merge[T](inputCount: Int)
              (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Merger[T] = Merger(inputCount)

  /** Distributes the input among outputs in parallel, picking the first free output every time.
    * @see [[com.fsist.stream.Scatterer]]
    */
  def scatter[T](outputCount: Int)
                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Scatterer[T] = Scatterer(outputCount)
}

