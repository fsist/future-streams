package com.fsist

import scala.concurrent.Future

/** An implementation of Reactive Streams based on Scala futures. See the README.md on Github, and the documentation
  * of the classes [[com.fsist.stream.Source]], [[com.fsist.stream.Sink]] and [[com.fsist.stream.Pipe]].
  */
package object stream {
  /** A shared instance of a successful Future[Unit] used by code in this package. */
  final val success : Future[Unit] = Future.successful(())

  /** A shared instance of a successful Future[Boolean] returning true used by code in this package. */
  final val trueFuture : Future[Boolean] = Future.successful(true)

  /** A shared instance of a successful Future[Boolean] returning false used by code in this package. */
  final val falseFuture : Future[Boolean] = Future.successful(false)
}
