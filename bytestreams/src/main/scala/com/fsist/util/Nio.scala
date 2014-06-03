package com.fsist.util

import java.nio.channels._
import scala.concurrent.{ExecutionContext, Future, Promise}
import akka.util.ByteString
import java.io.{IOException, EOFException}
import java.nio.ByteBuffer
import com.typesafe.scalalogging.slf4j.Logging
import scala.Some
import com.fsist.stream.{Source, Sink}

/** Utility code to work with Java NIO async operations and Scala futures. */
object Nio extends Logging {

  /** Translates Java NIO completion events to Scala Futures.
    *
    * Make sure to use a new instance for each completion callback. See example use in [[writeSome]].
    */
  class JavaCompletionHandler[T] extends CompletionHandler[T, Object] {
    private val promise = Promise[T]()

    val future: Future[T] = promise.future

    override def failed(exc: Throwable, attachment: Object): Unit = promise.failure(exc)
    override def completed(result: T, attachment: Object): Unit = promise.success(result)
  }

  /** Write some of this data to the channel, in a single async write() channel operation. Returns number of bytes written.
    *
    * The future fails with EOFException if the channel is closed.
    */
  private def writeSome(channel: AsynchronousByteChannel, bytes: ByteBuffer)(implicit ec: ExecutionContext): Future[Int] = {
    val callback = new JavaCompletionHandler[Integer]()
    channel.write(bytes, null, callback)
    callback.future.map(count => {
      if (count == -1) throw new EOFException()
      count.intValue
    }).recover {
      // Unfortunately, async writes sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      //      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      //      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: IOException =>
        logger.trace("Closing after IOException")
        throw new EOFException(e.getMessage)
      case e: Throwable =>
        logger.error(s"Unexpected error type: $e")
        throw new EOFException(e.getMessage)
    }
  }

  /** Write this buffer fully to a channel in a series of asynchronous calls.
    *
    * The future fails with EOFException if the channel is closed.
    */
  def write(channel: AsynchronousByteChannel, bytes: ByteString)(implicit ec: ExecutionContext): Future[Unit] = {
    val buf = bytes.asByteBuffer
    def next(): Future[Int] = writeSome(channel, buf)
    (next() flatMap (_ =>
      if (buf.hasRemaining) next() else Future.successful(0))
      ).map(_ => ())
  }

  /** @return a Sink that writes to this channel. */
  def sink(channel: AsynchronousByteChannel)(implicit ec: ExecutionContext): Sink[ByteString, Unit] =
    Sink.foreachM(write(channel, _))

  /** Execute a single async read operation on the channel, appending to this buffer. Returns the number of bytes read.
    *
    * The future fails with EOFException if not enough data can be read.
    */
  private def readSome(channel: AsynchronousByteChannel, bytes: ByteBuffer)(implicit ec: ExecutionContext): Future[Int] = {
    if (!bytes.hasRemaining) throw new IllegalArgumentException("No space left in buffer")

    val callback = new JavaCompletionHandler[Integer]()
    channel.read(bytes, null, callback)
    callback.future.map(count => {
      if (count == -1) throw new EOFException()
      count.intValue
    }).recover {
      // Unfortunately, async reads sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      //      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      //      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: IOException => throw new EOFException(e.getMessage)
      case e: Throwable =>
        logger.error(s"Unexpected error type: $e")
        throw new EOFException(e.getMessage)
    }
  }

  /** Read this amount of data from this channel.
    *
    * The future fails with EOFException if not enough data can be read.
    */
  def read(channel: AsynchronousByteChannel, count: Int)(implicit ec: ExecutionContext): Future[ByteString] = {
    val buf = ByteBuffer.allocate(count)
    def next(): Future[Int] = readSome(channel, buf)
    (next() flatMap (_ => if (buf.hasRemaining) next() else Future.successful(0))).map(_ => {
      buf.position(0)
      ByteString(buf)
    })
  }

  /** An async reader from this channel represented as a Source. */
  def source(channel: AsynchronousByteChannel, readBufferSize: Int = 1024 * 16)
            (implicit ec: ExecutionContext): Source[ByteString] = {
    val buf = ByteBuffer.allocate(readBufferSize)
    Source.generateM[ByteString] {
      readSome(channel, buf) map { count =>
        if (count == 0) None
        else {
          buf.position(0)
          val data = ByteString(buf).take(count)
          buf.position(0)
          Some(data)
        }
      } recover { case e: EOFException => None}
    }
  }

}
