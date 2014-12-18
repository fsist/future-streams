package com.fsist.stream

import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.duration._

class TransformTest extends FunSuite with StreamTester {
  test("SingleTransform") {
    val range = 1 to 5

    val result = Source.from(range).transform(SingleTransform((i: Int) => i + 10)).toList().buildResult().futureValue
    assert(result == (11 to 15), "Transformed correctly")
  }

  test("MultiTransform") {
    val range = 1 to 5

    val tr = MultiTransform.apply(Func((i: Int) => 1 to i), Seq.empty[Int])
    val result = Source.from(range).transform(tr).toList().buildResult().futureValue
    val expected = range.flatMap(i => 1 to i)
    assert(result == expected, "Transformed correctly")
  }

  test("map") {
    val range = 1 to 5

    val result = Source.from(range).map(_ + 1).toList().buildResult().futureValue
    val expected = range.map(_ + 1)
    assert(result == expected, "Mapped correctly")
  }

  test("flatMap") {
    val range = 1 to 5

    val result = Source.from(range).flatMap(1 to _).toList().buildResult().futureValue
    val expected = range.flatMap(1 to _)
    assert(result == expected, "Mapped correctly")
  }

  test("filter") {
    val range = 1 to 20

    val result = Source.from(range).filter(_ % 2 == 0).toList().buildResult().futureValue
    val expected = range.filter(_ % 2 == 0)
    assert(result == expected, "Filtered correctly")
  }

  test("take") {
    val range = 1 to 10

    val result = Source.from(range).take(5).toList().buildResult().futureValue
    val expected = range.take(5)
    assert(result == expected)
  }

  test("drop") {
    val range = 1 to 10

    val result = Source.from(range).drop(5).toList().buildResult().futureValue
    val expected = range.drop(5)
    assert(result == expected)
  }

  test("SingleTransform completion promise is fulfilled") {
    val tr = Transform.map[Int, Int](Func.pass)
    val stream = Source(1, 2, 3).to(tr).foreach(Func.nop).build()
    stream(tr).completion.futureValue(Timeout(1.second))
  }

  test("MultiTransform completion promise is fulfilled") {
    val tr = Transform.flatMap((i: Int) => Seq(i))
    val stream = Source(1, 2, 3).to(tr).foreach(Func.nop).build()
    stream(tr).completion.futureValue(Timeout(1.second))
  }

  test("flatten") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).flatten().collect[List]().buildResult().futureValue
    assert(result == range, "Flattened")
  }

  test("takeElements") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).takeElements(5).collect[List]().buildResult().futureValue
    assert(result == List(List(1, 2, 3), List(4, 5)))
  }

  test("dropElements") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).dropElements(5).collect[List]().buildResult().futureValue
    assert(result == List(List(6), List(7, 8, 9), List(10)))
  }

  test("nop") {
    val range = 1 to 10
    val result = Source.from(range).transform(Transform.nop[Int]).collect[List].buildResult().futureValue
    assert(result == range)
  }
}
