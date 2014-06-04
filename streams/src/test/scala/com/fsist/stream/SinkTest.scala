package com.fsist.stream

import com.fsist.FutureTester
import scala.collection.mutable.ListBuffer
import com.fsist.util.CancelToken
import scala.concurrent.{ExecutionContext, Future}
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.duration._

class SinkTest extends FunSuite with FutureTester {
  implicit val cancelToken: CancelToken = CancelToken.none
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("foreach") {
    val source = Source(1, 2, 3)
    val received = ListBuffer[Int]()
    val sink = Sink.foreach[Int] {
      i => received.append(i)
    }
    source >>| sink
    sink.onSinkDone.futureValue
    assert(received === List(1, 2, 3))
  }

  test("foreachM") {
    val source = Source(1, 2, 3)
    val received = ListBuffer[Int]()
    val sink = Sink.foreachM[Int] {
      i => Future { received += i }
    }
    source >>| sink
    sink.onSinkDone.futureValue
    assert(received === List(1, 2, 3))
  }

  test("collect") {
    // This also ensures it completes reasonably fast
    val list = (1 to 100000).toList
    val source = Source(list: _*)
    val sink = Sink.collect[Int]
    val result = (source >>| sink).futureValue(Timeout(1.second))
    assert(result == list)
  }
}
