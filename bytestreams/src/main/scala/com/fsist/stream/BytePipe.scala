package com.fsist.stream

import com.fsist.util.concurrent.CancelToken

import scala.concurrent.{Promise, Future, ExecutionContext}
import akka.util.ByteString
import scala.async.Async._
import com.fsist.util.FastAsync._

/** Functions to create and work with types extending Pipe[ByteString, ByteString, _]. */
object BytePipe {

  /** Take these many bytes, produce them as the result, and forward all other bytes unmodified.
    *
    * If fewer than `count` bytes are present in the input stream, the `result` fails with a [[TakeTapNotEnoughInputException]]. */
  def takeTap(count: Int)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Pipe[ByteString, ByteString, ByteString] =
    new PipeSegment[ByteString, ByteString, ByteString] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private var buffer : ByteString = ByteString.empty
      private var done: Boolean = false

      override protected def process(t: Option[ByteString]): Future[Boolean] = async {
        if (done) {
          val _ = fastAwait(emit(t)) // The val _ disables a warning
          t.isEmpty
        }
        else t match {
          case Some(bytes) =>
            buffer ++= bytes

            if (buffer.size >= count) {
              resultPromise.success(buffer.slice(0, count))
              if (buffer.size > count) {
                val _ = fastAwait(emit(Some(buffer.drop(count))))
                done = true
              }
            }

            false
          case None =>
            resultPromise.failure(new TakeTapNotEnoughInputException(count, buffer))
            emit(None)
            true
        }
      }
    } named "BytePipe.takeResult"

  /** The failure produced by [[BytePipe.takeTap]] if not enough data is present.
    *
    * @param requestedCount the `count` passed to [[BytePipe.takeTap]].
    * @param input the input we consumed (which wasn't enough).
    */
  case class TakeTapNotEnoughInputException(requestedCount: Long, input: ByteString)
    extends Exception(s"Not enough input: requested $requestedCount, got ${input.size}")

  /** Forwards this many bytes, then unsusbscribes from the Source and discards the rest of the input we may receive.
    *
    * NOTE: unsubscribing from a Source that represents an open file or other expensive resource doesn't close it,
    * so you should make sure the completion of this pipe triggers the closing of the source if needed.
    *
    * If fewer than `count` bytes are present in the input stream, produces only those that do exist, but does not fail.
    */
  def take(toTake: Long)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Pipe[ByteString, ByteString, Unit] =
    new PipeSegment.WithoutResult[ByteString, ByteString] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private var remaining: Long = toTake

      override protected def process(input: Option[ByteString]): Future[Boolean] = async {
        input match {
          case Some(bytes) =>
            val toPass = Math.min(remaining, bytes.size)
            remaining = remaining - toPass
            fastAwait(emit(Some(bytes.take(toPass.toInt))))
            if (remaining == 0) {
              fastAwait(emit(None))
              cancelSubscription()
              true
            }
            else false
          case None =>
            fastAwait(emit(None))
            true
        }
      }
    } named "BytePipe.take"

  /** Drops this many bytes from the input and forwards all the rest. If fewer than `toDrop` bytes exist in the input,
    * nothing will be forwarded.
    */
  def drop(toDrop: Long)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none) : Pipe[ByteString, ByteString, Unit] =
    new PipeSegment.WithoutResult[ByteString, ByteString] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private var remaining: Long = toDrop

      override protected def process(input: Option[ByteString]): Future[Boolean] = async {
        input match {
          case Some(bytes) =>
            val drop = Math.min(remaining, bytes.size)
            remaining = remaining - drop
            fastAwait(emit(Some(bytes.drop(drop.toInt))))
            false
          case None =>
            fastAwait(emit(None))
            true
        }
      }
    } named "BytePipe.drop"

}
