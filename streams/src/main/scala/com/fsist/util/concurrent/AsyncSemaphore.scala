package com.fsist.util.concurrent

import scala.concurrent.{ExecutionContext, Future, Promise}

/** A semaphore that provides a Future to those trying to acquire it, instead of blocking.
  *
  * Concurrent attempts to decrement to not succeed in any guaranteed order. 
  *
  * The implementation is a naive one based on a lock; a lock-free implementation using atomic references should be
  * possible but I didn't bother. (It would have to create a Tuple to wrap every pair of (count, promise), and would
  * that really help performance?)
  */
class AsyncSemaphore(initialCount: Long = 0)(implicit ec: ExecutionContext) {
  import com.fsist.util.concurrent.AsyncSemaphore._

  @volatile private var count : Long = initialCount
  // Will be fulfilled when the count is increased from 0, and replaced with a new promise
  @volatile private var promise : Promise[Unit] = Promise[Unit]()
  private val lock = new AnyRef

  def increment(by: Int = 1): Unit = {
    require(by > 0)

    lock.synchronized {
      if (count == 0) {
        promise.success(())
        promise = Promise[Unit]()
      }
      count += by
    }
  }

  def decrement(): Future[Unit] = {
    lock.synchronized {
      if (count > 0) {
        count -= 1
        success
      }
      else promise.future flatMap (_ => decrement())
    }
  }
}

object AsyncSemaphore {
  private val success : Future[Unit] = Future.successful(())
}