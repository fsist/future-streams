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

  {
    class C1[T] {
      type M = T
      def foo(c2: C2[T]): Unit = {}
      def fooM(c2: C2[M]): Unit = foo(c2)
    }

    class C2[T]

    val c1: C1[_] = new C1[String]()
    val c2: C2[_] = new C2[String]()

    c1.fooM(c2.asInstanceOf[C2[c1.M]])

    println("ok")
  }


  def main(args: Array[String]): Unit = {

    implicit val ec = ExecutionContext.global

    val counter = new AtomicLong()
    val source = Source.from(1L to 100000000L).map(_ + 1) //.foreach((x: Int) => counter += x)
//    val sink = Sink.foreach(AsyncFunc((x: Long) => Future {counter.incrementAndGet()}), AsyncFunc(_ => Future.successful(())))(source.builder)
    val sink = Sink.foreach(SyncFunc((x: Long) => counter.incrementAndGet()), AsyncFunc(_ => Future.successful(())))(source.builder)
    val stream = source.to(sink)

    val fut2 = stream.runAndGet().result

    Await.result(fut2, 1.minute)
    println(counter.get)
  }
}
