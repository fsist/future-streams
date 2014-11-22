package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import com.fsist.util.{AsyncFunc, Func, SyncFunc}

import scala.annotation.tailrec
import scala.concurrent.{Future, Await, ExecutionContext}

import scala.concurrent.duration._

import scala.async.Async._
import com.fsist.util.FastAsync._

object Sandbox {
  def main(args: Array[String]): Unit = {

    implicit val ec = ExecutionContext.global

    @volatile var counter = 0L
    val fut2 = Source.from(1 to 1000000).map(_ + 1).foreach(x => counter += x).runAndGet().result

    Await.result(fut2, 1.minute)
    println(counter)
  }
}
