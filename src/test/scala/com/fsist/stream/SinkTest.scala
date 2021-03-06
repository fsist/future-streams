package com.fsist.stream

import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class SinkTest extends FunSuite with StreamTester {
  test("SimpleOutput") {
    val data = 1 to 10
    val source = Source.from(data)

    var total = 0
    val sink = SimpleOutput[Int, Int](
      (i: Int) => total += i,
      total,
      Func.nop
    )

    val result = source.to(sink).buildResult().futureValue
    val expected = data.sum
    assert(result == expected, "Data arrived correctly")
  }

  test("foreach") {
    var sum = 0
    val data = 1 to 10
    val result = Source.from(data).foreach((i: Int) => sum += i).buildResult().futureValue
    assert(sum == data.sum, "Data processed correctly")
  }

  test("StreamOutput completion and futureResult promises are fulfilled") {
    val sink = Sink.foreach[Int, Unit](Func.nopLiteral)
    val stream = Source.of(1, 2, 3).to(sink).build()
    stream(sink).completion.futureValue(impatience)
    sink.futureResult().futureValue(impatience)
  }

  test("StreamOutput futureResult fails if the stream fails") {
    val err = new IllegalArgumentException
    val src = Source.generate[Int](throw err)
    val sink = Sink.foreach[Int, Unit](println(_))
    val result = src.to(sink).buildResult
    assert(result.failed.futureValue == err)
    assert(sink.futureResult.failed.futureValue == err)
  }

  test("single") {
    val result = Source.of(1).singleResult().futureValue
    assert(result == 1)

    val stream = Source.empty.singleResult()
    awaitFailure[NoSuchElementException](stream, ".single operator on empty stream")

    val stream2 = Source.of(1, 2, 3).singleResult()
    awaitFailure[IllegalArgumentException](stream2, ".single operator on stream with more than one elements")
  }

  test("DelayedSink") {
    val data = 1 to 10
    val promise = Promise[Sink[Int, List[Int]]]()
    val flat = Sink.flatten(promise.future)
    val result = Source.from(data).to(flat).buildResult()

    awaitTimeout(result, "Stream doesn't complete while waiting for delayed sink")(impatience)

    val tr = Transform.collect[Int, List]()
    val sink = Sink(tr, tr.single())
    promise.success(sink)

    assert(result.futureValue == data)
  }

  test("DelayedSink with empty input") {
    val data = Seq.empty
    val promise = Promise[Sink[Int, List[Int]]]()
    val flat = Sink.flatten(promise.future)
    val result = Source.from(data).to(flat).buildResult()

    awaitTimeout(result, "Stream doesn't complete while waiting for delayed sink")(impatience)

    val tr = Transform.collect[Int, List]()
    val sink = Sink(tr, tr.single())
    promise.success(sink)

    assert(result.futureValue == data)
  }

  test("DelayedSink (when the Future fails)") {
    val data = 1 to 10
    val promise = Promise[Sink[Int, List[Int]]]()
    val flat = Sink.flatten(promise.future)
    val result = Source.from(data).to(flat).buildResult()

    val error = new IllegalArgumentException("test")
    promise.failure(error)

    assert(result.failed.futureValue == error)
  }

  test("Completion: ConsumerMachine") {
    val sink = Sink.foreach[Int, Unit](x => ())
    val stream = Source.from(1 to 10).to(sink).build()
    stream.completion.futureValue(impatience)
    assert(stream.components(sink).completion.isCompleted)
  }

  test("Completion: DelayedSinkMachine") {
    val promise = Promise[Sink[Int, Unit]]()
    val sink = Sink.flatten(promise.future)
    val stream = Source.from(1 to 10).to(sink).build()
    promise.success(Sink.foreach[Int, Unit](x => ()))
    stream.completion.futureValue(impatience)
    assert(stream.components(sink).completion.isCompleted)
  }
}
