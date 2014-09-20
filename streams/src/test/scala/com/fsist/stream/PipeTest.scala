package com.fsist.stream

import java.util.concurrent.ConcurrentLinkedQueue

import com.fsist.FutureTester
import com.fsist.util.concurrent.{CanceledException, CancelToken}
import org.scalatest.FunSuite

import scala.collection.JavaConversions._
import scala.concurrent._

class PipeTest extends FunSuite with FutureTester {
  implicit val ec: ExecutionContext = ExecutionContext.global

  test("flatMapInput") {
    val source = Source.from(1 to 10)
    val sink = Sink.collect[Int]()

    val pipe = Pipe.flatMapInput[Int, Int] {
      case Some(x) if x <= 5 => Future {
        Some(x * 2)
      }
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

  test("tap") {
    val source = Source.from(1 to 10)
    val (pipe, source2) = Pipe.tap[Int]()
    source >>| pipe
    val result1 = pipe >>| Sink.collect()
    val result2 = source2 >>| Sink.collect()
    assert(result1.futureValue == (1 to 10))
    assert(result2.futureValue == (1 to 10))
  }

  test("flatten") {
    val source = Source.from(1 to 10)
    val sink = Sink.collect[Int]()
    val pipe = Pipe.flatten(Future {
      Pipe.map[Int, Int](a => a)
    })
    assert((source >> pipe >>| sink).futureValue == (1 to 10))
  }

  test("cancel pipe") {
    val cancel = CancelToken()
    val source = Source.from(1 to 10)(ec, cancel)
    val sink = Sink.puller[Int]()

    val cancelPipe = CancelToken()
    val pipe = Pipe.map[Int, Int](x => x)(ec, cancelPipe)

    val fut = source >> pipe >>| sink
    awaitTimeout(fut)

    cancelPipe.cancel()
    awaitFailure[CanceledException](fut)
    awaitFailure[CanceledException](sink.onSinkDone)
    awaitFailure[CanceledException](pipe.onSourceDone)
    awaitFailure[CanceledException](pipe.onSinkDone)
  }

  test("flattenTraversable") {
    val input = for (x <- 1 to 10) yield Seq((1 to 10) : _*)

    val result = (Source.from(input) >> Pipe.flattenTraversable[Int, Seq[Int]]() >>| Sink.collect()).futureValue
    val expected = input.flatten.toSeq
    assert(result == expected)
  }

  test("mergeEither") {
    val srca = Source.from(1 to 10)
    val srcb = Source("foo", "bar")

    val pipe = Pipe.mergeEither[String, Int](srcb)
    val collected = (srca >> pipe >>| Sink.collect()).futureValue
    val expected = (1 to 10).map(Left.apply) ++ Seq("foo", "bar").map(Right.apply)

    assert(collected.size == expected.size, "Correct number of items collected")
    assert(collected.toSet == expected.toSet, "Correct items collected")
  }

  test("inject") {
    val srca = Source.generate(Some(1)) // Never terminates
    val srcb = Source("foo", "bar")

    val pipe = Pipe.inject[Int, String](srca)
    val collected = (srcb >> pipe >>| Sink.collect()).futureValue

    val expected = Set(Right(1), Left("foo"), Left("bar"))
    assert(collected.toSet == expected, "Correct items collected")
  }

  test("stopOn") {
    // Never terminates
    val source = Source.generateM[Int]{
      Future {
        blocking {
          Thread.sleep(10)
          Some(1)
        }
      }
    }
    val sink = Sink.discard[Int]
    val stop = Promise[Unit]()
    val pipe = Pipe.stopOn[Int](stop.future)

    val done = source >> pipe >>| sink

    awaitTimeout(done, "Pipeline is running")
    stop.trySuccess(())
    done.futureValue // Await completion
  }
}
