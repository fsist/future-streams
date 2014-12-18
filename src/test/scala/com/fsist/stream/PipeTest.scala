package com.fsist.stream

import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite

class PipeTest extends FunSuite with StreamTester {
  test("Using a pipe manually with a single component") {
    val data = 1 to 10
    val source = Source.from(data)

    val pipe : Pipe[Int, Int] = Transform.map((i: Int) => i + 1)

    val result = source.to(pipe).collect[List].buildResult().futureValue
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

    val result = source.to(pipe).collect[List].buildResult().futureValue
    val expected = data.map(_ + 3)
    assert(result == expected)
  }

  test("Building on a pipe") {
    val data = 1 to 10
    def tr = Transform.map((i: Int) => i + 1)

    def pipe = tr.pipe(tr).pipe(tr)

    val result = Source.from(data).through(pipe).collect[List]().buildResult().futureValue

    assert(result == data.map(_ + 3))
  }
}
