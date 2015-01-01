package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite

import scala.concurrent.Promise

class PipeTest extends FunSuite with StreamTester {
  test("Using a pipe manually with a single component") {
    val data = 1 to 10
    val source = Source.from(data)

    val pipe: Pipe[Int, Int] = Transform.map((i: Int) => i + 1)

    val result = source.to(pipe).collect[List].singleResult().futureValue
    val expected = data.map(_ + 1)
    assert(result == expected)
  }

  test("Using a pipe manually with several components") {
    val data = 1 to 10
    val source = Source.from(data)

    val tr1 = Transform.map((i: Int) => i + 1)
    val tr2 = Transform.map((i: Int) => i + 1)
    val tr3 = Transform.map((i: Int) => i + 1)

    tr1.connect(tr2)
    tr2.connect(tr3)

    val pipe = Pipe(tr1, tr3)

    val result = source.to(pipe).collect[List].singleResult().futureValue
    val expected = data.map(_ + 3)
    assert(result == expected)
  }

  test("Building on a pipe") {
    val data = 1 to 10
    def tr = Transform.map((i: Int) => i + 1)

    def pipe = tr.pipe(tr).pipe(tr)

    val result = Source.from(data).through(pipe).collect[List]().singleResult().futureValue

    assert(result == data.map(_ + 3))
  }

  test("DelayedPipe") {
    val data = 1 to 10
    val promise = Promise[Pipe[Int, Int]]()
    val stream = Source.from(data).through(Pipe.flatten(promise.future)).toList.singleResult()

    awaitTimeout(stream, "Stream doesn't complete while waiting for delayed pipe")(impatience)

    val pipe = Transform.map(Func[Int, Int](_ + 1)).pipe(Transform.map(Func[Int, Int](_ - 2)))

    promise.success(pipe)

    assert(stream.futureValue == data.map(_ - 1))
  }

  test("DelayedPipe (when the Future is already completed)") {
    val data = 1 to 10
    val promise = Promise[Pipe[Int, Int]]()

    val pipe = Transform.map(Func[Int, Int](_ + 1)).pipe(Transform.map(Func[Int, Int](_ - 2)))

    promise.success(pipe)

    val stream = Source.from(data).through(Pipe.flatten(promise.future)).toList.singleResult()

    assert(stream.futureValue == data.map(_ - 1))
  }

  test("DelayedPipe (when the Future fails)") {
    val data = 1 to 10
    val promise = Promise[Pipe[Int, Int]]()
    val stream = Source.from(data).through(Pipe.flatten(promise.future)).toList.singleResult()

    val error = new IllegalArgumentException("test")
    promise.failure(error)

    assert(stream.failed.futureValue == error)
  }

  test("Double-delayed pipe") {
    val data = 1 to 10
    val promise = Promise[Pipe[Int, Int]]()
    val result = Source.from(data).through(Pipe.flatten(promise.future)).toList.singleResult()

    val promise2 = Promise[Pipe[Int, Int]]()
    promise.success(Pipe.flatten(promise2.future))

    promise2.success(Pipe.nop[Int])

    assert(result.futureValue == data)
  }

  test("Pipe of pipe") {
    // Regression test for bug in FutureStreamBuilder
    val data = 1 to 10

    implicit val builder = new FutureStreamBuilder

    val src = Source.from(data)

    val tr1 = Transform.map[Int, Int](Func.pass[Int])
    val pipe1 = Pipe(tr1, tr1.map(_ + 1))

    val tr2 = pipe1.source.map(_ + 1)
    val pipe2 = Pipe(pipe1, tr2) // This is legal

    val result = src.to(pipe2).toList.singleResult

    assert(result.futureValue == data.map(_ + 2))
  }
}
