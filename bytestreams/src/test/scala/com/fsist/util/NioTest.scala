package com.fsist.util

import org.scalatest.FunSuite
import akka.util.ByteString
import com.fsist.FutureTester
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import java.nio.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel}
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import com.fsist.stream.{ByteSource, ByteSink, Sink, Source}
import scala.util.Random

class NioTest extends FunSuite with FutureTester {
  import ExecutionContext.Implicits.global
  import NioTest._

  def generateData(count : Int = 1024*8): ByteString = {
    val array = new Array[Byte](count)
    new Random().nextBytes(array)
    ByteString(array)
  }

  test("Socket read/write") {
    val (chan1, chan2) = socketPair(12345)

    val data1 = generateData()
    val data2 = generateData()

    Nio.write(chan1, data1).futureValue
    Nio.write(chan2, data2).futureValue

    assert(Nio.read(chan1, data2.size).futureValue === data2, "Data was transferred")
    assert(Nio.read(chan2, data1.size).futureValue === data1, "Data was transferred (2)")

    chan1.close()
    chan2.close()
  }

  test("Socket reader/writer") {
    val (chan1, chan2) = socketPair(12345)

    val data1 = generateData()
    val data2 = generateData()

    val writer1 = ByteSink(chan1)
    val reader1 = ByteSource(chan1)
    val writer2 = ByteSink(chan2)
    val reader2 = ByteSource(chan2)

    (Source(data1) >>| writer1).futureValue
    (Source(data2) >>| writer2).futureValue

    val read1 = reader1 >>| Sink.collect[ByteString]
    val read2 = reader2 >>| Sink.collect[ByteString]

    // Having finished writing, close the sockets' writing side so the Sources can complete
    await(100.millis)
    chan1.close()
    chan2.close()

    assert(readAll(read1) === data2, "Data was transferred")
    assert(readAll(read2) === data1, "Data was transferred (2)")

    chan1.close()
    chan2.close()
  }

  def readAll(chunks: Future[List[ByteString]]) = chunks.futureValue.fold(ByteString()){case (a,b) => a++b}

}

object NioTest {
  /** Opens and returns two local sockets connected to one another. This method is mainly useful for tests.
    * Warning: this method blocks for a short period of time.
    *
    * On Linux, we could use the native socketpair...
    *
    * @param tempListeningPort Needs to temporarily succeed at binding to this port. Closes the port before returning.
    */
  def socketPair(tempListeningPort: Int) : (AsynchronousSocketChannel, AsynchronousSocketChannel) = {
    // Note: would use java.nio.channels.Pipe, but it doesn't provide an AsynchronousByteBufferChannel!
    val serverChannel = AsynchronousServerSocketChannel.open()
    try {
      val address = new InetSocketAddress("127.0.0.1", tempListeningPort)
      serverChannel.bind(address)
      val channel1 = AsynchronousSocketChannel.open()
      val connectFuture = channel1.connect(address) // Note, this is a horrible j.u.c.Future, it's not async!
      val channel2 = serverChannel.accept().get(50, TimeUnit.MILLISECONDS)
      connectFuture.get(50, TimeUnit.MILLISECONDS)

      (channel1, channel2)
    }
    finally {
      serverChannel.close()
    }
  }
}
