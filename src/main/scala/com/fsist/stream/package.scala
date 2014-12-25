package com.fsist

import akka.http.util.FastFuture

import scala.concurrent.Future

package object stream {
  val futureSuccess: Future[Unit] = FastFuture.successful(())
  val emptyIterable = Iterable.empty[Nothing]
  val futureEmptyIterable: Future[Iterable[Nothing]] = FastFuture.successful(emptyIterable)
}
