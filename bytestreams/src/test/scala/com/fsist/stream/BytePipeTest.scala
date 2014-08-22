package com.fsist.stream

import akka.util.ByteString
import com.fsist.FutureTester
import com.fsist.util.concurrent.CancelToken
import org.scalatest.FunSuite

import scala.concurrent.ExecutionContext
import scala.util.Random

class BytePipeTest extends FunSuite with FutureTester {
  implicit val ec = ExecutionContext.global
  implicit val cancelToken = CancelToken.none

  test("take") {
    val dataArr = new Array[Byte](10000)
    // Deliberately don't use Misc.random to make sync with github future-streams easier
    val random = new Random()
    random.nextBytes(dataArr)

    val data = ByteString(dataArr)
    val dataChunks = data.sliding(112, 112).toIndexedSeq

    val taken = (Source.from(dataChunks) >> BytePipe.take(1234) >>| ByteSink.concat()).futureValue
    assert(taken.length == 1234, "Prefix of correct length was taken")
    assert(taken == data.take(1234), "Prefix was taken")
  }
}
