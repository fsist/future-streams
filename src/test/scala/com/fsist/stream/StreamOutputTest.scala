package com.fsist.stream

import com.fsist.util.Func
import org.scalatest.FunSuite

class StreamOutputTest extends FunSuite with StreamTester {
  test("SimpleOutput") {
    val data = 1 to 10
    val source = Source.from(data)
    val sink = SimpleOutput(new StreamConsumer[Int, Int] {
      var total = 0

      override def onNext: Func[Int, Unit] = (i: Int) => total += i

      override def onError: Func[Throwable, Unit] = Func.nop

      override def onComplete: Func[Unit, Int] = total
    })

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
    val result = Source.from(data).collect[Int, Vector]().buildResult().futureValue
    assert(result.isInstanceOf[Vector[Int]] && result == data.to[Vector], "Collected in a Vector")
  }
}
