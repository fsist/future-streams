package com.fsist.stream

import com.fsist.util.concurrent.AsyncFunc
import org.scalatest.FunSuite

import scala.collection.immutable
import scala.collection.immutable.BitSet
import scala.concurrent.Promise
import scala.util.Random
import scala.concurrent.duration._

class ConnectorTest extends FunSuite with StreamTester {
  test("split, constant choice of single output") {
    val range = 1 to 100
    val count = 3

    val splitter = Source.from(range).split(count, (i: Int) => BitSet(0))
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    splitter.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    assert(stream(sinks(0)).result.futureValue == range, "All data went to sink 0")
    assert(stream(sinks(1)).result.futureValue == List(), "All data went to sink 0")
    assert(stream(sinks(2)).result.futureValue == List(), "All data went to sink 0")
  }

  test("split, constant choice of two outputs") {
    val range = 1 to 100
    val count = 3

    val splitter = Source.from(range).split(count, (i: Int) => BitSet(0, 2))
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    splitter.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    assert(stream(sinks(0)).result.futureValue == range, "All data went to sinks 0 and 2")
    assert(stream(sinks(1)).result.futureValue == List(), "All data went to sinks 0 and 2")
    assert(stream(sinks(2)).result.futureValue == range, "All data went to sinks 0 and 2")
  }

  test("split, random choice of single output") {
    val range = 1 to 100
    val count = 3
    val random = new Random()

    val splitter = Source.from(range).split(count, (i: Int) => BitSet(random.nextInt(3)))
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    splitter.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    val allResults =
      (for (sink <- sinks)
      yield stream(sink).result.futureValue).flatten

    assert(allResults.sorted == range, "Each data element went to a single sink")
  }

  test("round robin") {
    val range = 1 to 100
    val count = 3

    val splitter = Source.from(range).roundRobin(count)
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    splitter.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    assert(stream(sinks(0)).result.futureValue == range.by(3))
    assert(stream(sinks(1)).result.futureValue == range.drop(1).by(3))
    assert(stream(sinks(2)).result.futureValue == range.drop(2).by(3))
  }

  test("tee") {
    val range = 1 to 100
    val count = 3

    val splitter = Source.from(range).tee(3)
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    splitter.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    assert(stream(sinks(0)).result.futureValue == range, "All items were passed to sink 0")
    assert(stream(sinks(1)).result.futureValue == range, "All items were passed to sink 1")
    assert(stream(sinks(2)).result.futureValue == range, "All items were passed to sink 2")
  }

  test("merge") {
    val range = 1 to 100
    val count = 3

    val sources = Vector.fill(count)(Source.from(range))
    val merger = Connector.merge[Int](count)
    merger.connectInputs(sources)
    val result = merger.output.toList().buildResult().futureValue

    val expected = Vector.fill(count)(range).flatten
    assert(result.sorted == expected.sorted , "All elements were merged")
  }

  test("scatter") {
    val range = 1 to 100
    val count = 3

    val scatterer = Source.from(range).scatter(3)
    val sinks = Vector.fill(count)(Sink.collect[Int, List]())
    scatterer.connectOutputs(sinks)

    val stream = sinks(0).build()
    stream.completion.futureValue

    val allResults =
      (for (sink <- sinks)
      yield stream(sink).result.futureValue).flatten

    assert(allResults.sorted == range, "Each data element went to a single sink")
  }

  test("scatter: ensure parallelism") {
    // Need quick timeouts
    implicit def patienceConfig = PatienceConfig(250.millis)

    val range = 1 to 100
    val count = 3

    val scatterer = Source.from(range).scatter(3)

    val promises = Vector.fill(count)(Promise[Unit]())

    val sinks = for (promise <- promises) yield Sink.foreach[Int, Unit](AsyncFunc(x => promise.future))
    scatterer.connectOutputs(sinks)

    val stream = sinks(0).build()

    awaitTimeout(stream.completion, "All sinks are blocked; stream should not complete")

    promises(0).success(())

    for (sink <- sinks) {
      awaitTimeout(stream(sink).completion, "Sink will not complete as long as the stream doesn't")
    }
    awaitTimeout(stream.completion, "All sinks are blocked; stream should not complete")

    promises(1).success(())
    promises(2).success(())

    assert(stream.completion.futureValue === (), "Entire stream should now complete")
  }

  test("Scatter-gather") {
    val range = 1 to 100
    val count = 3

    val scatterer = Source.from(range).scatter(3)
    val gatherer = Connector.merge[Int](count)
    gatherer.connectInputs(scatterer.outputs)

    val result = gatherer.output.toList().buildResult().futureValue

    assert(result.sorted == range, "Scatter-gather")
  }

  test("Split-gather") {
    val range = 1 to 100
    val count = 3

    val splitter = Source.from(range).roundRobin(3)
    val gatherer = Connector.merge[Int](count)
    gatherer.connectInputs(splitter.outputs)

    val result = gatherer.output.toList().buildResult().futureValue

    assert(result.sorted == range, "Scatter-gather")
  }
}