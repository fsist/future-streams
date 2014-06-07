package com.fsist.stream

import akka.util.ByteString
import com.fsist.util.{CanceledException, Nio, CancelToken}
import java.io.{IOException, EOFException, InputStream}
import scala.Some
import scala.concurrent._
import java.nio.channels.AsynchronousByteChannel
import java.nio.ByteBuffer
import java.net.Socket

/** Functions to create and work with types extending Source[ByteString]. */
object ByteSource {

  /** Wraps an InputStream in a Source.
    *
    * WARNING: an InputStream is inherently blocking, and this consumes a thread for each stream wrapped in this way,
    * which is inefficient and does NOT scale. Avoid this method unless you absolutely have to wrap a legacy InputStream.
    *
    * @param closeOnDone If true, the stream's `close` method is called when the stream returns EOF or reading returns
    *                    an error. More importantly, it will also be called if this Source is cancelled.
    */
  def fromStreamAvoidThis(stream: InputStream, bufferSize: Int = 1024 * 16, closeOnDone: Boolean = true)
                         (implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[ByteString] = new SourceImpl[ByteString] {
    override def cancelToken: CancelToken = cancel

    override def ec: ExecutionContext = ecc

    if (closeOnDone) {
      try {
        cancelToken.future.map(_ => stream.close())
      }
      catch {
        case e: IOException =>
      }
    }

    private val buffer = new Array[Byte](bufferSize)

    override protected def produce(): Future[Option[ByteString]] = Future {
      blocking {
        try {
          stream.read(buffer) match {
            case -1 =>
              if (closeOnDone) stream.close()
              None
            case count => Some(ByteString(buffer).slice(0, count))
          }
        }
        catch {
          case e: IOException if cancelToken.isCanceled => throw new CanceledException()
        }
      }
    }
  }

  /** An async reader from this channel represented as a Source.
    *
    * @param closeOnDone If true, the channel's `close` method is called when the stream returns EOF or reading returns
    *                    an error. More importantly, it will also be called if this Source is cancelled.
    */
  def apply(channel: AsynchronousByteChannel, readBufferSize: Int = 1024 * 16, closeOnDone: Boolean = true)
           (implicit ec: ExecutionContext, cancelToken: CancelToken = CancelToken.none): Source[ByteString] = {
    if (closeOnDone) {
      try {
        cancelToken.future.map(_ => channel.close())
      }
      catch {
        case e: IOException =>
      }
    }

    val buf = ByteBuffer.allocate(readBufferSize)
    Source.generateM[ByteString] {
      Nio.readSome(channel, buf) map { count =>
        if (count == 0) None
        else {
          buf.position(0)
          val data = ByteString(buf).take(count)
          buf.position(0)
          Some(data)
        }
      } recover {
        case e: EOFException => None
        case e: IOException if cancelToken.isCanceled => throw new CanceledException()
      }
    }
  }
}
