package com.fsist.stream

import com.fsist.FutureTester
import com.fsist.util.concurrent.CanceledException
import com.fsist.util.concurrent.{CanceledException, CancelToken}
import org.reactivestreams.Publisher
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Future}

class SourceTest extends FunSuite with FutureTester {
  implicit val cancelToken: CancelToken = CancelToken.none
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("from(Iterator)") {
    val data = 1 to 10
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

  test("from(Publisher)") {
    val publisher : Publisher[Int] = Source.from(1 to 100)
    val source = Source.from(publisher)
    assert((source >>| Sink.collect()).futureValue == (1 to 100))
  }

  test("concat") {
    val source1 = Source(1, 2, 3) named "source1"
    val source2 = Source(4, 5, 6) named "source2"
    val source3 = Source(7, 8, 9) named "source3"
    val all = Source.concat(Seq(source1, source2, source3))

    val result = all >>| Sink.collect[Int]()

    assert(result.futureValue == (1 to 9))
  }

  test("concat Source[Source[T]]") {
    val source1 = Source(1, 2, 3) named "source1"
    val source2 = Source(4, 5, 6) named "source2"
    val source3 = Source(7, 8, 9) named "source3"
    val sources = Source(source1, source2, source3)

    val all = Source.concat(sources)
    val result = all >>| Sink.collect[Int]()

    assert(result.futureValue == (1 to 9))
  }

  test("concat (infinite sequence)") {
    import scala.collection.immutable.{Stream => FunctionalStream}
    lazy val naturals: FunctionalStream[Int] = 1 #:: naturals.map(_ + 1)
    val sources = naturals.map(x => Source(x, x*x, x*x*x))
    val all = Source.concat(sources).take(9)
    val prefix = List(1, 1, 1, 2, 4, 8, 3, 9, 27)

    val result = all >>| Sink.collect()
    assert(result.futureValue == prefix)
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

  test("Replace subscriber") {
    // Regression test for #16: when the Source is waiting for the old subscriber to requestMore, and we unsubscribe
    // it and a different subscriber eventually calls requestMore, make sure the Source isn't stuck
    val source = Source.from(1 to 10)
    val sink1 = Sink.puller[Int]()
    val fut = source >>| sink1
    assert(sink1.pull().futureValue == Some(1))

    sink1.cancelSubscription()
    val sink2 = Sink.collect[Int]()
    assert((source >>| sink2).futureValue.size > 0) // Exact size is hard to predict due to buffering in various places
  }

  test("take") {
    val source = Source.from(1 to 10)
    val take = source.take(5)
    val result = (take >>| Sink.collect()).futureValue

    assert(result == (1 to 5))
  }

  test("take and cancel") {
    val cancel = CancelToken()
    val source = Source.from(1 to 10)(ec, cancel)
    val take = source.take(5, true)
    val result = (take >>| Sink.collect()).futureValue

    assert(result == (1 to 5))
    assert(cancel.isCanceled)
  }

  test("skip") {
    val source = Source.from(1 to 10)
    val take = source.skip(5)
    val result = (take >>| Sink.collect()).futureValue

    assert(result == (6 to 10))
  }

  test("mergeEither") {
    val srca = Source.from(1 to 10)
    val srcb = Source("foo", "bar")

    val merged = Source.mergeEither(srca, srcb).named("merged")
    val collected = (merged >>| Sink.collect()).futureValue
    val expected = (1 to 10).map(Left.apply) ++ Seq("foo", "bar").map(Right.apply)
    assert(collected.size == expected.size, "Correct number of items collected")
    assert(collected.toSet == expected.toSet, "Correct items collected")
  }
}
