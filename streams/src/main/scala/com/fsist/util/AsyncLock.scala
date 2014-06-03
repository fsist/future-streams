package com.fsist.util

import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import scala.concurrent.{Future, Promise}
import java.util.concurrent.ConcurrentLinkedQueue
import com.typesafe.scalalogging.slf4j.Logging

/** An exclusive lock that is acquired by waiting on a future, instead of blocking.
  *
  * This is a naive implementation that isn't intended to be particularly fast in tight loops.
  */
class AsyncLock extends Logging {
  /** The single Lock maintained for this instance and given out to users */
  private val lock = new Lock

  /** Single instance used to preserve reference equality in compareAndSet */
  private val lockOpt = Some(lock)

  /** Contains Some(lockOpt) if the lock is not taken, None if it is taken */
  private val locked = new AtomicReference[Option[Lock]](lockOpt)

  /** Promises we've given out */
  private val promises = new ConcurrentLinkedQueue[Promise[Lock]]()

  /** Acquire the lock. You must call `free` on the returned object. */
  def take: Future[Lock] = {
    if (locked.compareAndSet(lockOpt, None)) {
      Future.successful(lock)
    }
    else {
      val promise = Promise[Lock]()
      promises.add(promise)
      // Check if the lock was already released while we were doing this
      if (locked.compareAndSet(lockOpt, None)) {
        promises.remove(promise)
        Future.successful(lock)
      }
      else promise.future
    }
  }

  /** Represents ownership of the lock. */
  class Lock private[AsyncLock] {
    private def doFree(): Unit = {
      if (!locked.compareAndSet(None, lockOpt)) {
        throw new BugException(s"Tried to release the lock, but it was already released")
      }
    }

    /** Releases the lock. */
    def free(): Unit = {
      promises.poll() match {
        case null => doFree()
        case promise => promise.success(lock)
      }
    }
  }
}
