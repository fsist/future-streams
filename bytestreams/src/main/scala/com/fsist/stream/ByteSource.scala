package com.fsist.stream

import akka.util.ByteString
import com.fsist.util.CancelToken
import java.io.InputStream
import scala.Some
import scala.concurrent._

/** Functions to create and work with types extending Source[ByteString]. */
object ByteSource {

  /** Wraps an InputStream in a Source.
    *
    * WARNING: an InputStream is inherently blocking, and this consumes a thread for each stream wrapped in this way,
    * which is inefficient and does NOT scale. Avoid this method unless you absolutely have to wrap a legacy InputStream.
    */
  def fromStreamAvoidThis(stream: InputStream, bufferSize: Int = 1024*16)
                         (implicit ecc: ExecutionContext, cancel: CancelToken) : Source[ByteString] = new SourceImpl[ByteString] {
    override def cancelToken: CancelToken = cancel
    override def ec: ExecutionContext = ecc

    private val buffer = new Array[Byte](bufferSize)

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


}
