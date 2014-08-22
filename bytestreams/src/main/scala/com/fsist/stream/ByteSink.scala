package com.fsist.stream

import akka.util.ByteString
import com.fsist.util.Nio
import java.io.OutputStream
import com.fsist.util.concurrent.CancelToken

import scala.concurrent.blocking
import scala.concurrent.{Future, ExecutionContext}
import java.nio.channels.AsynchronousByteChannel

/** Functions to create and work with types extending Sink[ByteString, _]. */
object ByteSink {

  /** A Sink which collects and concatenates all its input and produces that as the result. */
  def concat()(implicit ecc: ExecutionContext) : Sink[ByteString, ByteString] =
    new SinkImpl[ByteString, ByteString] {
      override def ec: ExecutionContext = ecc

      private var buffer : ByteString = ByteString.empty

      override protected def process(t: Option[ByteString]): Future[Boolean] = t match {
        case Some(bytes) =>
          buffer = buffer ++ bytes
          falseFuture
        case None =>
          resultPromise.success(buffer)
          trueFuture
      }
    } named "ByteSink.concat"

  /** Wraps an OutputStream in a Sink.
    *
    * WARNING: an OutputStream is inherently blocking, and this consumes a thread for each stream wrapped in this way,
    * which is inefficient and does NOT scale. Avoid this method unless you absolutely have to wrap a legacy OutputStream.
    */
  def toStreamAvoidThis(stream: OutputStream)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Sink[ByteString, Unit] =
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

  /** Returns a Sink that writes to this channel. */
  def apply(channel: AsynchronousByteChannel)(implicit ec: ExecutionContext): Sink[ByteString, Unit] =
    Sink.foreachM(Nio.write(channel, _))
}
