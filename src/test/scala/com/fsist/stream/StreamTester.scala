package com.fsist.stream

import com.fsist.FutureTester
import org.scalatest.FunSuiteLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

/** Common definitions for stream test code */
trait StreamTester extends FunSuiteLike with FutureTester {
  override implicit def patienceConfig = PatienceConfig(1.minute)

  implicit def ec: ExecutionContext = ExecutionContext.global
}
