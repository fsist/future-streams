package com.fsist.stream

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream.run.RunningStreamOutput
import com.fsist.util.Func
import org.scalatest.FunSuite

import scala.util.Failure
import scala.concurrent.duration._

class StreamErorrHandlingTest extends FunSuite with StreamTester {

  class OnErrorRecorder {
    val captured: AtomicReference[Option[Throwable]] = new AtomicReference[Option[Throwable]](None)
    val onError = Func((th: Throwable) => captured.set(Some(th)))
  }

  test("When a source fails, the whole graph fails with the same exception and all onError handlers are called") {
    val err = new Exception("test")

    val sourceOnError = new OnErrorRecorder
    val sinkOnError = new OnErrorRecorder

    val source = Source.generate[Int]((x: Unit) => throw err, sourceOnError.onError)
    val sink = Sink.foreach(Func.nop, onError = sinkOnError.onError)

    val stream = source.to(sink).build()

    awaitFailure(stream.completion, "Stream should fail")

    for (fut <- stream.components.values.map(_.completion).toList :+ stream.completion :+
      stream.components(sink).asInstanceOf[RunningStreamOutput[Int, Unit]].result) {

      awaitFailure(fut, "Each future should fail")
      assert(fut.value == Some(Failure(err)), "Each future failed with the original exception")
    }

    assert(sourceOnError.captured.get == Some(err), "Source onError handler was called with the original exception")
    assert(sinkOnError.captured.get == Some(err), "Sink onError handler was called with the original exception")
  }

  test("When a sink fails, the whole graph fails with the same exception") {
    val err = new Exception("test")

    val sourceOnError = new OnErrorRecorder
    val sinkOnError = new OnErrorRecorder

    val source = Source.generate[Int](1, sourceOnError.onError)
    val sink = Sink.foreach((x: Int) => throw err, onError = sinkOnError.onError)

    val stream = source.to(sink).build()

    awaitFailure(stream.completion, "Stream should fail")

    for (fut <- stream.components.values.map(_.completion).toList :+ stream.completion :+
      stream.components(sink).asInstanceOf[RunningStreamOutput[Int, Unit]].result) {

      awaitFailure(fut, "Each future should fail")
      assert(fut.value == Some(Failure(err)), "Each future failed with the original exception")
    }

    assert(sourceOnError.captured.get == Some(err), "Source onError handler was called with the original exception")
    assert(sinkOnError.captured.get == Some(err), "Sink onError handler was called with the original exception")
  }

  test("In a non-linear graph, when a component fails, the whole graph fails with the same exception") {
    val err = new Exception("test")

    val sourceOnError1 = new OnErrorRecorder
    val sourceOnError2 = new OnErrorRecorder
    val sinkOnError1 = new OnErrorRecorder
    val sinkOnError2 = new OnErrorRecorder

    val source1 = Source.generate[Int](1, sourceOnError1.onError)
    val source2 = Source.generate[Int](1, sourceOnError2.onError)
    val sink1 = Sink.foreach(Func.nop, onError = sinkOnError1.onError)
    val sink2 = Sink.foreach((x: Int) => throw err, onError = sinkOnError2.onError)

    val merger = Merger[Int](2)
    val splitter = Connector.tee[Int](2)

    merger.connectInputs(List(source1, source2))
    merger.output.connect(splitter.input)
    splitter.connectOutputs(List(sink1, sink2))

    val stream = sink2.build()

    awaitFailure(stream.completion, "Stream should fail")

    for (fut <- stream.components.values.map(_.completion).toList :+ stream.completion :+
      stream.components(sink1).asInstanceOf[RunningStreamOutput[Int, Unit]].result :+
      stream.components(sink2).asInstanceOf[RunningStreamOutput[Int, Unit]].result) {

      awaitFailure(fut, "Each future should fail")
      assert(fut.value == Some(Failure(err)), "Each future failed with the original exception")
    }

    assert(sourceOnError1.captured.get == Some(err), "Source1 onError handler was called with the original exception")
    assert(sinkOnError1.captured.get == Some(err), "Sink1 onError handler was called with the original exception")
    assert(sourceOnError2.captured.get == Some(err), "Source2 onError handler was called with the original exception")
    assert(sinkOnError2.captured.get == Some(err), "Sink2 onError handler was called with the original exception")
  }

  test("Failing a running graph from outside") {
    val sourceOnError = new OnErrorRecorder
    val sinkOnError = new OnErrorRecorder

    val source = Source.generate(1, sourceOnError.onError)
    val sink = Sink.foreach(Func.nop, onError = sinkOnError.onError)

    val stream = source.to(sink).build()

    val err = new Exception("test")
    stream.fail(err)

    for (fut <- stream.components.values.map(_.completion).toList :+ stream.completion :+
      stream.components(sink).asInstanceOf[RunningStreamOutput[Int, Unit]].result) {

      awaitFailure(fut, "Each future should fail")
      assert(fut.value == Some(Failure(err)), "Each future failed with the original exception")
    }

    assert(sourceOnError.captured.get == Some(err), "Source onError handler was called with the original exception")
    assert(sinkOnError.captured.get == Some(err), "Sink onError handler was called with the original exception")
  }
}
