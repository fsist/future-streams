package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import akka.http.util.FastFuture
import com.fsist.FutureTester
import com.fsist.util.AsyncFunc
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, ExecutionContext}

class Sandbox extends FunSuite with FutureTester {
  //    System.setProperty("scala.concurrent.context.maxThreads", "1")
  //    System.setProperty("scala.concurrent.context.minThreads", "1")
  //    System.setProperty("scala.concurrent.context.numThreads", "1")

  implicit val defaultEc = ExecutionContext.global
  //        implicit val ec = new ExecutionContext {
  //          override def reportFailure(t: Throwable): Unit = logger.error(s"Failure in future", t)
  //          override def execute(runnable: Runnable): Unit = {
  //            logger.info(s"EC execute from: ${(new Exception).getStackTraceString}")
  //            ExecutionContext.global.execute(runnable)
  //          }
  //        }

  test("sandbox") {

    val counter = new AtomicLong()
    val total = 1000000L

    def fut: Future[Unit] = {
      val src = Source.from(1L to total)
      val mapped = (1 to 5).foldLeft(src: Source[Long]) {
        case (src, i) =>
          src.transform(Transform.map[Long, Long](AsyncFunc(x => FastFuture.successful(x + 1))))
            .map(_ + 1)
      }
      val stream = mapped.foreach(_ => counter.incrementAndGet())

      val start = System.currentTimeMillis()
      stream.buildResult().map {
        case res =>
          val end = System.currentTimeMillis()
          val elapsed = end - start
          logger.info(s"$total in $elapsed ms = ${total / elapsed} items/ms")
      }.flatMap(_ => fut)
    }

    fut.futureValue(Timeout(10.minutes))
  }

  test("split and merge") {
    val splitter = Source.from(1 to 10).tee(3) //.roundRobin(3)
    val merger = Merger[Int](3)
    for ((output, input) <- splitter.outputs.zip(merger.inputs)) {
      output.connect(input)
    }
    val sink = merger.outputs(0).foreach(println(_))

    sink.buildResult().futureValue(Timeout(10.minutes))
  }
}
