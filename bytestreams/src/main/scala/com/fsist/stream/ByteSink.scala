package com.fsist.stream

import akka.util.ByteString
import com.fsist.util.Nio
import java.io.OutputStream
import com.fsist.util.concurrent.CancelToken
import scala.async.Async._
import com.fsist.util.FastAsync._
import scala.concurrent.blocking
import scala.concurrent.{Future, ExecutionContext}
import java.nio.channels.{AsynchronousFileChannel, AsynchronousByteChannel}

/** Functions to create and work with types extending Sink[ByteString, _]. */
object ByteSink extends NamedLogger {

  /** A Sink which collects and concatenates all its input and produces that as the result. */
  def concat()(implicit ecc: ExecutionContext): Sink[ByteString, ByteString] =
    new SinkImpl[ByteString, ByteString] {
      override def ec: ExecutionContext = ecc

      private var buffer: ByteString = ByteString.empty

      override protected def process(t: Option[ByteString]): Future[Boolean] = t match {
        case Some(bytes) =>
          buffer = buffer ++ bytes
          falseFuture
        case None =>
          resultPromise.trySuccess(buffer)
          trueFuture
      }
    } named "ByteSink.concat"

  /** Wraps an OutputStream in a Sink.
    *
    * WARNING: an OutputStream is inherently blocking, and this consumes a thread for each stream wrapped in this way,
    * which is inefficient and does NOT scale. Avoid this method unless you absolutely have to wrap a legacy OutputStream.
    */
  def toStreamAvoidThis(stream: OutputStream)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Sink[ByteString, Unit] =
    new SinkImpl.WithoutResult[ByteString] {
      override def ec: ExecutionContext = ecc
      override protected def process(input: Option[ByteString]): Future[Boolean] = {
        cancel.throwIfCanceled

        Future {
          blocking {
            input match {
              case Some(bytes) =>
                stream.write(bytes.toArray)
                false
              case None =>
                stream.close()
                true
            }
          }
        }
      }
    } named "ByteSink.toStream"

  /** Returns a Sink that writes to this channel.
    *
    * @param closeOnDone if true, will close the channel after EOF is received by the sink.
    */
  def toByteChannel(channel: AsynchronousByteChannel, closeOnDone: Boolean = true)
                   (implicit ec: ExecutionContext): Sink[ByteString, Unit] =
    Sink.foreachInputM{
      case Some(bytes) => Nio.write(channel, bytes)
      case None =>
        if (closeOnDone) channel.close()
        success
    }

  /** Returns a Sink that writes to this channel starting at the given file position.
    *
    * @param closeOnDone if true, will close the channel after EOF is received by the sink.
    */
  def toFileChannel(channel: AsynchronousFileChannel, filePos: Long, closeOnDone: Boolean = true)
                   (implicit ec: ExecutionContext): Sink[ByteString, Unit] = {
    var currentPos = filePos
    var count : Long = 0
    Sink.foreachInputM[ByteString] {
      case Some(bytes) =>
        async {
          await(Nio.write(channel, bytes, currentPos))
          currentPos += bytes.size
          count += bytes.size
        }
      case None =>
        if (closeOnDone) channel.close()
        success
    }
  }
}
