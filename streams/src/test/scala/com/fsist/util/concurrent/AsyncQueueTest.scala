package com.fsist.util.concurrent

import com.fsist.FutureTester
import com.fsist.stream.Sink
import com.typesafe.scalalogging.slf4j.Logging
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext

class AsyncQueueTest extends FunSuite with FutureTester with Logging {
  implicit val ec = ExecutionContext.global

  test("Sanity") {
    val aq = new AsyncQueue[Int]()

    val dequeue = aq.dequeue()
    awaitTimeout(dequeue, "Can't dequeue from empty queue")

    aq.enqueue(1)
    assert(dequeue.futureValue === 1, "Dequeue")

    val dequeue2 = aq.dequeue()
    awaitTimeout(dequeue2, "Can't dequeue from empty queue (2)")

    aq.enqueue(2)
    assert(dequeue2.futureValue === 2, "Second dequeue")
  }

  test("Load") {
    val aq = new AsyncQueue[Int]()

    val count = 10000
    val waiters = for (x <- 0 to count) yield aq.dequeue()

    for (x <- 0 to count) aq.enqueue(x)

    for ((w, x) <- waiters.zipWithIndex) assert(w.futureValue === x)
  }

  test("Source") {
    logger.trace(s"Starting")

    val aq = new AsyncQueue[Int]()
    for (x <- 1 to 10) aq.enqueue(x)

    val source = aq.source(_ == 500)
    val result = source >>| Sink.collect[Int]()

    for (x <- 11 to 20) aq.enqueue(x)
    aq.enqueue(500)

    assert(result.futureValue == (1 to 20).toList :+ 500)
  }
}