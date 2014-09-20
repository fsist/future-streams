package com.fsist.util

import java.nio.channels._
import scala.concurrent.{ExecutionContext, Future, Promise}
import akka.util.ByteString
import java.io.{IOException, EOFException}
import java.nio.ByteBuffer
import com.typesafe.scalalogging.slf4j.Logging
import scala.Some
import com.fsist.stream.{Source, Sink}
import scala.util.control.NonFatal

/** Bridges between `java.nio` async operations and Scala futures. */
object Nio extends Logging {

  /** Translates Java NIO completion events to Scala Futures.
    *
    * Make sure to use a new instance for each completion callback. See example use in `writeSome`.
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
  def writeSome(channel: AsynchronousByteChannel, bytes: ByteBuffer)(implicit ec: ExecutionContext): Future[Int] = {
    val callback = new JavaCompletionHandler[Integer]()
    channel.write(bytes, null, callback)
    callback.future.map(_.intValue).recover {
      // Unfortunately, async writes sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      //      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      //      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: IOException =>
        logger.trace("Closing after IOException")
        throw new EOFException(e.getMessage)
      case NonFatal(e) =>
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

  /** Write some of this data to the channel, in a single async write() channel operation, at file position `filePos`.
    *
    * @return number of bytes written. The future fails with EOFException if the channel is closed.
    */
  def writeSome(channel: AsynchronousFileChannel, bytes: ByteBuffer, filePos: Long)(implicit ec: ExecutionContext): Future[Int] = {
    val callback = new JavaCompletionHandler[Integer]()
    channel.write(bytes, filePos, null, callback)
    callback.future.map(_.intValue).recover {
      // Unfortunately, async writes sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      //      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      //      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: IOException =>
        logger.trace("Closing after IOException")
        throw new EOFException(e.getMessage)
      case NonFatal(e) =>
        logger.error(s"Unexpected error type: $e")
        throw new EOFException(e.getMessage)
    }
  }

  /** Write this buffer fully to a channel in a series of asynchronous calls, starting at file position `filePos`.
    *
    * The future fails with EOFException if the channel is closed.
    */
  def write(channel: AsynchronousFileChannel, bytes: ByteString, filePos: Long)(implicit ec: ExecutionContext): Future[Unit] = {
    val buf = bytes.asByteBuffer
    var currentPos = filePos
    def next(): Future[Int] = writeSome(channel, buf, currentPos)
    (next() flatMap { case count =>
      currentPos += count
      if (buf.hasRemaining) next() else Future.successful(0)
    }).map(_ => ())
  }

  /** Execute a single async read operation on the channel, appending to this buffer. Returns the number of bytes read.
    *
    * The future fails with EOFException if not enough data can be read.
    */
  def readSome(channel: AsynchronousByteChannel, bytes: ByteBuffer)(implicit ec: ExecutionContext): Future[Int] = {
    if (!bytes.hasRemaining) throw new IllegalArgumentException("No space left in buffer")

    val callback = new JavaCompletionHandler[Integer]()
    channel.read(bytes, null, callback)
    callback.future.map(count => {
      logger.trace(s"Read $count bytes from byte channel")
      if (count == -1) throw new EOFException()
      count.intValue
    }).recover {
      // Unfortunately, async reads sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: EOFException => throw e
      case e: IOException =>
        logger.warn(s"Unexpected IOException $e, assuming EOF")
        throw new EOFException(e.getMessage)
      case NonFatal(e) =>
        logger.error(s"Unexpected error type: $e")
        throw new EOFException(e.getMessage)
    }
  }

  /** Execute a single async read operation on the channel at the specified `filePos`, appending to this buffer.
    *
    * @return the number of bytes read. The future fails with EOFException if not enough data can be read.
    */
  def readSome(channel: AsynchronousFileChannel, bytes: ByteBuffer, filePos: Long)(implicit ec: ExecutionContext): Future[Int] = {
    if (!bytes.hasRemaining) throw new IllegalArgumentException("No space left in buffer")

    val callback = new JavaCompletionHandler[Integer]()
    channel.read(bytes, filePos, null, callback)
    callback.future.map(count => {
      if (count == -1) throw new EOFException()
      count.intValue
    }).recover {
      // Unfortunately, async reads sometime complete with simple IOExceptions and not more specific types like these.
      // Simplest to treat any IO error as fatal (not retriable) and close the channel.
      case e: AsynchronousCloseException => throw new EOFException(e.getMessage)
      case e: ClosedChannelException => throw new EOFException(e.getMessage)
      case e: EOFException => throw e
      case e: IOException =>
        logger.warn(s"Unexpected IOException $e, assuming EOF")
        throw new EOFException(e.getMessage)
      case NonFatal(e) =>
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

  /** Read this amount of data from this channel at the given file position `filePos`.
    *
    * The future fails with EOFException if not enough data can be read.
    */
  def read(channel: AsynchronousFileChannel, filePos: Long, count: Int)(implicit ec: ExecutionContext): Future[ByteString] = {
    val buf = ByteBuffer.allocate(count)
    var currentPos = filePos
    def next(): Future[Int] = readSome(channel, buf, currentPos)
    (next() flatMap {
      case count =>
        currentPos += count
        if (buf.hasRemaining) next() else Future.successful(0)
    }).map(_ => {
      buf.position(0)
      ByteString(buf)
    })
  }
}
