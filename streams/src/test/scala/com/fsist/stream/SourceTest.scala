package com.fsist.stream

import org.reactivestreams.api.Producer
import org.scalatest.FunSuite
import com.fsist.FutureTester
import com.fsist.util.{CanceledException, CancelToken}
import scala.concurrent.{Future, ExecutionContext}

class SourceTest extends FunSuite with FutureTester {
  implicit val cancelToken: CancelToken = CancelToken.none
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("from(Iterator)") {
    val data = 1 to 1000
    assert((Source.from(data.iterator) >>| Sink.collect()).futureValue == data)
  }

  test("empty") {
    assert((Source.empty >>| Sink.collect()).futureValue == Seq())
  }

  test("generateM") {
    var count = 0
    val max = 100
    assert((Source.generateM {
      Future {
        count += 1
        if (count == max) None else Some(count)
      }
    } >>| Sink.collect()).futureValue == (1 until max))
  }

  test("generate") {
    var count = 0
    val max = 100
    assert((Source.generate {
      count += 1
      if (count == max) None else Some(count)
    } >>| Sink.collect()).futureValue == (1 until max))
  }

  test("from(Producer)") {
    val producer : Producer[Int] = Source.from(1 to 100)
    val source = Source.from(producer)
    assert((source >>| Sink.collect()).futureValue == (1 to 100))
  }

  test("concat") {
    val source1 = Source(1, 2, 3) named "source1"
    val source2 = Source(4, 5, 6) named "source2"
    val source3 = Source(7, 8, 9) named "source3"
    val all = Source.concat(Seq(source1, source2, source3))

    val sink = Sink.collect[Int]()
    val result = (all >>| sink)

    assert(result.futureValue == (1 to 9))
  }

  test("flatten") {
    val source = Source.flatten(Future { Source(1, 2, 3) })
    assert((source >>| Sink.collect()).futureValue == Seq(1,2,3))
  }

  test("pusher") {
    val pusher = Source.pusher[Int]()
    val sink = Sink.collect[Int]()
    val future = pusher >>| sink
    for (x <- 1 to 10) pusher.push(Some(x)).futureValue
    awaitTimeout(future)
    pusher.push(None).futureValue
    assert(future.futureValue == (1 to 10))
  }

  test("Cancel Source waiting for demand") {
    val cancel = CancelToken()
    val source = Source.from(1 to 10)(ec, cancel)
    val sink = Sink.puller[Int]()
    val fut = source >>| sink
    awaitTimeout(fut)

    cancel.cancel()
    awaitFailure[CanceledException](fut)
    awaitFailure[CanceledException](source.onSourceDone)
    awaitFailure[CanceledException](sink.onSinkDone)
  }

  test("Cancel Source.pusher") {
    val cancel = CancelToken()
    val source = Source.pusher[Int](1)(ec, cancel)
    val sink = Sink.collect[Int]()
    val fut = source >>| sink
    awaitTimeout(fut)

    cancel.cancel()
    awaitFailure[CanceledException](fut)
    awaitFailure[CanceledException](source.onSourceDone)
    awaitFailure[CanceledException](sink.onSinkDone)
  }
}
