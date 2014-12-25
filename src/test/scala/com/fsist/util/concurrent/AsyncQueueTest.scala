package com.fsist.util.concurrent

import com.fsist.FutureTester
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext

class AsyncQueueTest extends FunSuite with FutureTester with LazyLogging {
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
}
