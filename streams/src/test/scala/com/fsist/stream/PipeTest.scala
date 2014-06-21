package com.fsist.stream

import com.fsist.FutureTester
import java.util.concurrent.ConcurrentLinkedQueue
import org.scalatest.FunSuite
import scala.collection.JavaConversions._
import scala.concurrent.{Future, ExecutionContext}

class PipeTest extends FunSuite with FutureTester {
  implicit val ec : ExecutionContext = ExecutionContext.global

  test("flatMapInput") {
    val source = Source.from(1 to 10)
    val sink = Sink.collect[Int]()

    val pipe = Pipe.flatMapInput[Int, Int] {
      case Some(x) if x <= 5 => Future { Some(x * 2) }
      case _ => Future.successful(None)
    }

    val result = (source >> pipe >>| sink).futureValue
    assert(result == Seq(2, 4, 6, 8, 10))
  }

  test("blocker") {
    val source = Source(1 to 10: _*)
    val blocker = Pipe.blocker[Int]()

    val queue = new ConcurrentLinkedQueue[Int]()
    val sink = Sink.foreach[Int] {
      i => queue.add(i)
    }

    val done = source >> blocker >>| sink

    awaitTimeout(done, "Pipe is blocked")
    assert(queue.isEmpty, "No elemnts were passed")

    blocker.unblock()
    done.futureValue
    assert(queue.toIndexedSeq == (1 to 10), "Unblocked pipe passed all elements")
  }

  test("tapOne") {
    val source = Source(1 to 10: _*)
    val tapper = Pipe.tapOne[Int]()
    val sink = Sink.collect[Int]()

    val result = (source >> tapper >>| sink).futureValue

    sink.result.futureValue // Run to completion

    assert(result == (1 to 10).toList, "Elements were passed through the tapper unmodified")
    assert(tapper.result.futureValue == Some(1), "Tapper captured first element")
  }
}
