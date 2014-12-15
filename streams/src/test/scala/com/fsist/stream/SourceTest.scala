package com.fsist.stream

import com.fsist.FutureTester
import com.fsist.util.Func
import org.scalatest.FunSuite
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class SourceTest extends FunSuite with FutureTester {
  override implicit def patienceConfig = PatienceConfig(1.minute)

  implicit def ec: ExecutionContext = ExecutionContext.global

  test("IteratorSource") {
    val range = 1 to 10
    val iter = range.iterator
    val result = Source.from(iter).toList().buildResult().futureValue

    assert(result == range, "All items were passed")
  }

  test("IterableSource") {
    val range = 1 to 10
    val result = Source.from(range).toList().buildResult().futureValue

    assert(result == range, "All items were passed")
  }

  test("GeneratorSource") {
    val range = 1 to 10
    val iter = range.iterator

    val source = Source.generate(Func(iter.next()))
    val result = source.toList().buildResult().futureValue
    assert(result == range, "All items were generated")
  }
}
