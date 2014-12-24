package com.fsist.stream

import com.fsist.util.concurrent.{AsyncFunc, Func}
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._

class TransformTest extends FunSuite with StreamTester {
  test("SingleTransform") {
    val range = 1 to 5

    val result = Source.from(range).transform(SingleTransform((i: Int) => i + 10)).toList().singleResult().futureValue
    assert(result == (11 to 15), "Transformed correctly")
  }

  test("MultiTransform") {
    val range = 1 to 5

    val tr = MultiTransform.apply(Func((i: Int) => 1 to i), Seq.empty[Int])
    val result = Source.from(range).transform(tr).toList().singleResult().futureValue
    val expected = range.flatMap(i => 1 to i)
    assert(result == expected, "Transformed correctly")
  }

  test("map") {
    val range = 1 to 5

    val result = Source.from(range).map(_ + 1).toList().singleResult().futureValue
    val expected = range.map(_ + 1)
    assert(result == expected, "Mapped correctly")
  }

  test("flatMap") {
    val range = 1 to 5

    val result = Source.from(range).flatMap(1 to _).toList().singleResult().futureValue
    val expected = range.flatMap(1 to _)
    assert(result == expected, "Mapped correctly")
  }

  test("filter") {
    val range = 1 to 20

    val result = Source.from(range).filter(_ % 2 == 0).toList().singleResult().futureValue
    val expected = range.filter(_ % 2 == 0)
    assert(result == expected, "Filtered correctly")
  }

  test("fold") {
    val data = 1 to 10

    val result = Source.from(data).fold(0) {
      case (item, sum) => item + sum
    }.singleResult().futureValue

    assert(result == data.sum, "Data processed correctly")
  }

  test("take") {
    val range = 1 to 10

    val result = Source.from(range).take(5).toList().singleResult().futureValue
    val expected = range.take(5)
    assert(result == expected)
  }

  test("drop") {
    val range = 1 to 10

    val result = Source.from(range).drop(5).toList().singleResult().futureValue
    val expected = range.drop(5)
    assert(result == expected)
  }

  test("SingleTransform completion promise is fulfilled") {
    val tr = Transform.map(Func.pass[Int])
    val stream = Source.of(1, 2, 3).to(tr).discard().build()
    stream(tr).completion.futureValue(Timeout(1.second))
  }

  test("MultiTransform completion promise is fulfilled") {
    val tr = Transform.flatMap[Int, Int]((i: Int) => Seq(i))
    val stream = Source.of(1, 2, 3).to(tr).discard().build()
    stream(tr).completion.futureValue(Timeout(1.second))
  }

  test("flatten") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).flatten().collect[List]().singleResult().futureValue
    assert(result == range, "Flattened")
  }

  test("takeElements") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).takeElements(5).collect[List]().singleResult().futureValue
    assert(result == List(List(1, 2, 3), List(4, 5)))
  }

  test("dropElements") {
    val range = 1 to 10
    val input = range.grouped(3)
    val result = Source.from(input).dropElements(5).collect[List]().singleResult().futureValue
    assert(result == List(List(6), List(7, 8, 9), List(10)))
  }

  test("nop") {
    val range = 1 to 10
    val result = Source.from(range).transform(Transform.nop[Int]).collect[List].singleResult().futureValue
    assert(result == range)
  }

  test("concat") {
    val range = 1 to 10
    val result = Source.from(range.grouped(3)).concat().collect[List]().singleResult().futureValue
    assert(result == List(range))
  }

  test("head") {
    val data = 1 to 10
    val result = Source.from(data).head.singleResult().futureValue
    assert(result == data.head)

    val result2 = Source.empty.head.singleResult()
    awaitFailure[NoSuchElementException](result2, "head should fail on an empty stream")
  }

  test("headOpt") {
    val data = 1 to 10
    val result = Source.from(data).headOption().singleResult().futureValue
    assert(result == data.headOption)

    val result2 = Source.empty.headOption.singleResult().futureValue
    assert(result2 == None, "headOption of empty stream")
  }

  test("collect (vector)") {
    val data = 1 to 10
    val result = Source.from(data).collect[Vector]().singleResult().futureValue
    assert(result.isInstanceOf[Vector[Int]] && result == data.to[Vector], "Collected in a Vector")
  }

  test("tapHead") {
    val data = 1 to 10
    val tap = Source.from(data).tapHead()
    tap.discard().buildResult().futureValue
    assert(tap.aside.futureValue == Some(1))
  }

  test("tapHead of empty stream") {
    val tap = Source.empty.tapHead()
    tap.discard().buildResult().futureValue
    assert(tap.aside.futureValue == None)
  }

  test("tapHead aside completes once an element has passed it") {
    var emittedFirst = false
    val source = Source.generateAsync{
      if (emittedFirst) Promise[Int]().future
      else {
        emittedFirst = true
        Future.successful(1)
      }
    }

    val tap = source.tapHead()
    val stream = tap.discard().buildResult()

    awaitTimeout(stream)(impatience)

    assert(tap.aside.futureValue == Some(1))
  }

  test("append") {
    val data = 1 to 10
    val extra = 11 to 20
    val result = Source.from(data).append(extra).collect[List].singleResult.futureValue
    assert(result == data ++ extra)
  }

  test("prepend") {
    val data = 1 to 10
    val extra = 11 to 20
    val result = Source.from(data).prepend(extra).collect[List].singleResult.futureValue
    assert(result == extra ++ data)
  }
}
