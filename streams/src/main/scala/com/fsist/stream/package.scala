package com.fsist

import scala.concurrent.Future

/** Foresight Streams is an implementation of Reactive Streams based on Scala futures.
  *
  * Please read the Reactive Streams APIs and SPIs - see [[org.reactivestreams.api.Producer]],
  * [[org.reactivestreams.api.Consumer]], [[org.reactivestreams.spi.Subscriber]], and [[org.reactivestreams.spi.Publisher]]
  * or go to www.reactive-streams.org.
  */
package object stream {
  /** A shared instance of a successful Future[Unit] used by code in this package. */
  final val success : Future[Unit] = Future.successful(())

  /** A shared instance of a successful Future[Unit] used by code in this package. */
  final val trueFuture : Future[Boolean] = Future.successful(true)

  /** A shared instance of a successful Future[Unit] used by code in this package. */
  final val falseFuture : Future[Boolean] = Future.successful(false)

}
