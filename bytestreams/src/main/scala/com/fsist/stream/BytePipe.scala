package com.fsist.stream

import scala.concurrent.{Promise, Future, ExecutionContext}
import com.fsist.util.CancelToken
import akka.util.ByteString
import org.reactivestreams.spi.{Publisher, Subscriber, Subscription}
import scala.async.Async._

/** Functions to create and work with types extending Pipe[ByteString, ByteString, _]. */
object BytePipe {

  /** Take these many bytes, produce them as the result, and forward all other bytes unmodified.
    *
    * If fewer than `count` bytes are present in the input stream, the `result` fails with a [[NotEnoughInputException]]. */
  def take(count: Int)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Pipe[ByteString, ByteString, ByteString] =
    new PipeSegment[ByteString, ByteString, ByteString] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private var buffer : ByteString = ByteString.empty
      private var done: Boolean = false

      override protected def process(t: Option[ByteString]): Future[Boolean] = async {
        if (done) {
          val _ = await(emit(t)) // The val _ disables a warning
          t.isEmpty
        }
        else t match {
          case Some(bytes) =>
            buffer ++= bytes

            if (buffer.size >= count) {
              resultPromise.success(buffer.slice(0, count))
              if (buffer.size > count) {
                val _ = await(emit(Some(buffer.drop(count))))
                done = true
              }
            }

            false
          case None =>
            resultPromise.failure(new NotEnoughInputException(count, buffer))
            emit(None)
            true
        }
      }
    } named "BytePipe.take"

  /** The failure produced by [[BytePipe.take]] if not enough data is present.
    *
    * @param requestedCount the `count` passed to [[BytePipe.take]].
    * @param input the input we consumed (which wasn't enough).
    */
  case class NotEnoughInputException(requestedCount: Int, input: ByteString)
    extends Exception(s"Not enough input: requested $requestedCount, got ${input.size}")
}
