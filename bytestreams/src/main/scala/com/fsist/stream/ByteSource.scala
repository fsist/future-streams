package com.fsist.stream

import java.io.{EOFException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousByteChannel, AsynchronousFileChannel}

import akka.util.ByteString
import com.fsist.util.Nio
import com.fsist.util.concurrent.CancelToken
import com.typesafe.scalalogging.slf4j.Logging

import scala.concurrent._
import scala.util.control.NonFatal

/** Functions to create and work with types extending Source[ByteString]. */
object ByteSource extends Logging {

  /** Wraps an InputStream in a Source.
    *
    * WARNING: an InputStream is inherently blocking, and this consumes a thread for each stream wrapped in this way,
    * which is inefficient and does NOT scale. Avoid this method unless you absolutely have to wrap a legacy InputStream.
    */
  def fromStreamAvoidThis(stream: InputStream, bufferSize: Int = 1024 * 16)
                         (implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[ByteString] = new SourceImpl[ByteString] {
    override def cancelToken: CancelToken = cancel
    override def ec: ExecutionContext = ecc

    private val buffer = new Array[Byte](bufferSize)

    cancel.future.onSuccess {
      case _ =>
        stream.close()
    }

    override protected def produce(): Future[Option[ByteString]] = Future {
      blocking {
        stream.read(buffer) match {
          case -1 =>
            stream.close()
            None
          case count => Some(ByteString(buffer).slice(0, count))
        }
      }
    }
  }

  /** An async reader from this channel represented as a Source. */
  def fromByteChannel(channel: AsynchronousByteChannel, readBufferSize: Int = 1024 * 16, closeOnDone: Boolean = true)
                     (implicit ec: ExecutionContext): Source[ByteString] = {
    val buf = ByteBuffer.allocate(readBufferSize)
    Source.generateM[ByteString] {
      Nio.readSome(channel, buf) map { count =>
        logger.trace(s"Read $count bytes from channel")
        if (count == 0) {
          if (closeOnDone) channel.close()
          None
        }
        else {
          buf.position(0)
          val data = ByteString(buf).take(count)
          buf.position(0)
          Some(data)
        }
      } recover { case e: EOFException => None}
    } named "ByteSource.fromByteChannel"
  }

  /** An async reader from this channel represented as a Source, which starts reading at file position `filePos`
    * (an AsynchronousFileChannel doesn't have a stateful file position). */
  def fromFileChannel(channel: AsynchronousFileChannel, filePos: Long, readBufferSize: Int = 1024 * 16, closeOnDone: Boolean = true)
                     (implicit ec: ExecutionContext): Source[ByteString] = {
    val buf = ByteBuffer.allocate(readBufferSize)
    var currentPos = filePos
    Source.generateM[ByteString] {
      Nio.readSome(channel, buf, currentPos) map { count =>
        currentPos += count

        if (count == 0) {
          if (closeOnDone) channel.close()
          None
        }
        else {
          buf.position(0)
          val data = ByteString(buf).take(count)
          buf.position(0)
          Some(data)
        }
      } recover {
        case e: EOFException =>
          if (closeOnDone) channel.close()
          None
        case NonFatal(e) if closeOnDone =>
          channel.close()
          throw e
      }
    }
  } named "ByteSource.fromFileChannel"
}

