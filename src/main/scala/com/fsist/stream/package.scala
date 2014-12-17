package com.fsist

import akka.http.util.FastFuture

import scala.concurrent.Future

package object stream {
  val futureSuccess : Future[Unit] = FastFuture.successful(())
}
