package com.fsist.stream

import org.scalatest.FunSuite
import com.fsist.FutureTester
import com.fsist.util.CancelToken
import scala.concurrent.ExecutionContext

class SourceTest extends FunSuite with FutureTester {
  implicit val cancelToken: CancelToken = CancelToken.none
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("concat") {
    logger.trace("starting")
    val source1 = Source(1,2,3) named "source1"
    val source2 = Source(4,5,6) named "source2"
    val both = Source.concat(Seq(source1, source2))

    val sink = Sink.collect[Int]()
    val result = (both >>| sink)

    assert(result.futureValue == List(1,2,3,4,5,6))
  }
}
