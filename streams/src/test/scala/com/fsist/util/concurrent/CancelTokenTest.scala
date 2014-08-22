package com.fsist.util.concurrent

import com.fsist.FutureTester
import org.scalatest.FunSuite

import scala.concurrent.{ExecutionContext, Promise}

class CancelTokenTest extends FunSuite with FutureTester {
  implicit val ec = ExecutionContext.global

  test("abandonOnCancel - complete the original future") {
    val promise = Promise[String]()
    val cancel = CancelToken()
    val aoc = cancel.abandonOnCancel(promise.future)

    awaitTimeout(aoc, "abandonOnCancel does not complete spuriously")
    promise.success("foo")
    assert(aoc.futureValue === "foo", "abandonOnCancel completed with original value")
  }

  test("abandonOnCancel - fail the original future") {
    val promise = Promise[String]()
    val cancel = CancelToken()
    val aoc = cancel.abandonOnCancel(promise.future)

    awaitTimeout(aoc, "abandonOnCancel does not complete spuriously")
    val e = new Exception("foo")
    promise.failure(e)
    assert(aoc.failed.futureValue === e, "abandonOnCancel completed with original future's exception")
  }

  test("abandonOnCancel - cancel") {
    val promise = Promise[String]()
    val cancel = CancelToken()
    val aoc = cancel.abandonOnCancel(promise.future)

    awaitTimeout(aoc, "abandonOnCancel does not complete spuriously")
    cancel.cancel()
    awaitFailure[CanceledException](aoc, "abandonOnCancel fails when canceled")
  }
}
