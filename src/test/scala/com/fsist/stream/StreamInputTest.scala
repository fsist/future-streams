package com.fsist.stream

import com.fsist.util.concurrent.Func
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.duration._

class StreamInputTest extends FunSuite with StreamTester {
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

    val source = Source.generate(if (iter.hasNext) iter.next() else throw new EndOfStreamException)
    val result = source.toList().buildResult().futureValue
    assert(result == range, "All items were generated")
  }

  test("StreamInput completion promise is fulfilled") {
    val source = Source(1, 2, 3)
    val stream = source.foreach(Func.nop).build()
    stream(source).completion.futureValue(Timeout(1.second))
  }
}
