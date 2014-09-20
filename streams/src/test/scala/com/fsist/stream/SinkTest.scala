package com.fsist.stream

import java.util.concurrent.ConcurrentLinkedQueue

import com.fsist.FutureTester
import com.fsist.util.concurrent.CancelToken
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.collection.immutable.VectorBuilder
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent._
import JavaConversions._
import scala.util.Success

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
      i => Future {
        received += i
      }
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

  test("fold") {
    val source = Source.from(1 to 10)
    val sink = Sink.fold[Int, Int, Int](0)(_ + _)(x => x)
    assert((source >>| sink).futureValue == (1 to 10).sum)
  }

  test("foldM") {
    val source = Source.from(1 to 10)
    val sink = Sink.foldM[Int, Int, Int](0)((a, b) => Future {
      a + b
    })(x => Future {
      x
    })
    assert((source >>| sink).futureValue == (1 to 10).sum)
  }

  test("flatten") {
    val sink = Sink.flatten(Future {
      Sink.collect[Int]()
    })
    val source = Source.from(1 to 10)
    assert((source >>| sink).futureValue == (1 to 10))
  }

  test("puller") {
    val puller = Sink.puller[Int]()
    val fut = Source.from(1 to 10) >>| puller
    awaitTimeout(fut)

    for (x <- (1 to 10)) {
      assert(puller.pull().futureValue == Some(x))
    }
    assert(puller.pull().futureValue == None)
    fut.futureValue
  }

  test("scatter") {
    val collected = new ConcurrentLinkedQueue[Int]()

    def sink: Sink[Int, Unit] = Sink.foreach { i => collected.add(i)}

    val sinks = for (x <- 1 to 10) yield sink

    val count = 150
    (Source.from(1 to count) >>| Sink.scatter(sinks)).futureValue
    Future.sequence(sinks map (_.onSinkDone)).futureValue

    assert(collected.size == count, "Correct amount of entries was emitted")
    assert(collected.toArray().toSet == (1 to count).toSet, "All entries were collected")
  }

  test("scatter distributes EOF") {
    val sinks = for (x <- 1 to 10) yield Sink.collect[Int]()

    (Source.from(1 to 100) >>| Sink.scatter(sinks)).futureValue

    for (sink <- sinks) {
      sink.onSinkDone.futureValue
    }
  }

  test("scatter doesn't block") {
    val queue = new ConcurrentLinkedQueue[Int]()
    val collector = Sink.foreach[Int](queue.add(_))

    val sinks = new VectorBuilder[Sink[Int, Unit]]()
    for (x <- 1 to 2) sinks += Sink.block[Int]()
    sinks += collector
    for (x <- 1 to 2) sinks += Sink.block[Int]()

    // Fire and forget
    Source.from(1 to 100) >>| Sink.scatter(sinks.result())

    eventually {
      assert(queue.size > 80, s"Most items made it through (current queue size is ${queue.size})")
    }
  }

  test("duplicate") {
    val sinks = for (x <- 1 to 5) yield Sink.collect[Int]()

    val duplicated = Sink.duplicate(sinks)

    val ints = 1 to 100
    (Source.from(ints) >>| duplicated).futureValue

    for (sink <- sinks) {
      assert(sink.onSinkDone.isCompleted, "Component sink has completed")
      assert(sink.result.value == Some(Success(ints)), "Component sink has received correct elements")
    }
  }
}
