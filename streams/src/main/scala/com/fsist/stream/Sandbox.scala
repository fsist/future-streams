package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import com.fsist.util.{AsyncFunc, Func, SyncFunc}

import scala.annotation.tailrec
import scala.concurrent.{Future, Await, ExecutionContext}

import scala.concurrent.duration._

import scala.async.Async._
import com.fsist.util.FastAsync._

import SyncFunc._

object Sandbox {
  def main(args: Array[String]): Unit = {

    implicit val ec = ExecutionContext.global

    val counter = new AtomicLong()
    val total = 10000000L

    def fut: Future[Unit] = {
      val stream = Source.from(1L to total).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).map(_ + 1).foreach(_ => counter.incrementAndGet())

      val start = System.currentTimeMillis()
      stream.runAndGet().result.map {
        case res =>
          val end = System.currentTimeMillis()
          val elapsed = end - start
          println(s"$total in $elapsed ms = ${total/elapsed} items/ms")
      }.flatMap (_ => fut)
    }

    Await.result(fut, 1.minute)
  }
}
