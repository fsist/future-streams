package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.collection.generic.CanBuildFrom
import scala.concurrent.duration._

class StreamOutputTest extends FunSuite with StreamTester {
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

  test("foldLeft") {
    val data = 1 to 10

    val result = Source.from(data).foldLeft(0){
      case (item, sum) => item + sum
    }.buildResult().futureValue

    assert(result == data.sum, "Data processed correctly")
  }

  test("collect (vector)") {
    val data = 1 to 10
    val result = Source.from(data).collect[Vector]().buildResult().futureValue
    assert(result.isInstanceOf[Vector[Int]] && result == data.to[Vector], "Collected in a Vector")
  }

  test("StreamOutput completion promise is fulfilled") {
    val sink = Sink.foreach[Int, Unit](Func.nop)
    val stream = Source(1, 2, 3).to(sink).build()
    stream(sink).completion.futureValue(Timeout(1.second))
  }

  test("concat") {
    val data = Seq.tabulate(10)(List(_))

    val result = Source.from(data).concat().buildResult().futureValue
    assert(result == data.toList.flatten, "Concatenated all input lists")
  }
}
