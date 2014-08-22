package com.fsist.util.concurrent

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream
import com.fsist.stream.Source
import com.fsist.util.FastAsync._
import com.typesafe.scalalogging.slf4j.Logging

import scala.annotation.tailrec
import scala.async.Async._
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future, Promise}

/** An asynchronous concurrent unbounded queue. Enqueueing completes immediately, while dequeueing returns a Future
  * that is completed once an object can be removed from the queue.
  *
  * Attribution: base implementation (enqueue/dequeue part) copied from
  * https://groups.google.com/forum/#!topic/scala-user/lyoAdNs3E1o
  * Originally by Viktor Klang.
  *
  * Implementation notes:
  *
  * An instance holds either a sequence of enqueued values Seq[T], or a sequence of dequeue() promises waiting to be
  * fulfilled.
  *
  * Operations are synchronized using AtomicReference. Each modification is attempted using compareAndSet, and
  * if the comparison failed (i.e. another thread won at the race of modifying the AtomicReference) we start the
  * dequeue/enqueue method from scratch.
  *
  * I replaced the Seq (implicit List) with a Queue to improve performance, since the original code was appending to
  * the end of the List.
  *
  * @tparam T the queue element type
  */
class AsyncQueue[T] extends AtomicReference[Either[Queue[Promise[T]], Queue[T]]](Right(Queue())) with Logging {

  /** Enqueue an item synchronously. */
  @tailrec final def enqueue(t: T): Unit = get match {
    case r@Right(q) => // Queue doesn't have any waiters so enqueue this value
      if (!compareAndSet(r, Right(q.enqueue(t)))) enqueue(t)
    case l@Left(Queue(p)) => // Queue has a single waiter
      if (!compareAndSet(l, Right(Queue()))) enqueue(t)
      else p success t
    case l@Left(q) => // Queue has multiple waiters
      val (p, ps) = q.dequeue
      if (!compareAndSet(l, Left(ps))) enqueue(t)
      else p success t
  }

  /** Dequeue an item. The returned future will be completed eventually once someone has enqueued an item that can be dequeued.
    *
    * Successive calls to this method return logically successive futures (i.e. the first one returned will dequeue first),
    * as long as each call completes its synchronous part (i.e. returns an actual future to the caller) before the next
    * call begins.
    */
  @tailrec final def dequeue(): Future[T] = get match {
    case l@Left(q) => // Already has waiting consumers
      val p = Promise[T]()
      if (!compareAndSet(l, Left(q.enqueue(p)))) dequeue()
      else p.future
    case r@Right(Queue()) => // Empty queue, become first waiting consumer
      val p = Promise[T]()
      if (!compareAndSet(r, Left(Queue(p)))) dequeue()
      else p.future
    case r@Right(q) => // Already has values. get the head
      val (t, ts) = q.dequeue
      if (!compareAndSet(r, Right(ts))) dequeue()
      else Future.successful(t)
  }

  /** Returns a Source[T] that will dequeue items. If other threads also call dequeue() while the Source is
    * running, some items will go to them and will not appear in the Source's output.
    *
    * @param stopOn if this function returns true for any item, the source will stop (call onComplete) after that item.
    *               This does not affect the queue itself, nor other Sources returned later from the same queue
    *               by this method.
    */
  def source(stopOn: T => Boolean = _ => false, clue: String = "AsyncQueue.source")(implicit ec: ExecutionContext): Source[T] = {
    var done: Boolean = false

    Source.generateM[T] {
      if (done) {
        logger.trace(s"$clue: producing EOF")
        stream.noneFuture
      }
      else {
        async {
          val t = fastAwait(dequeue())
          if (stopOn(t)) {
            logger.trace(s"$clue: identified next part, will produce EOF next time")
            done = true
          }
          Some(t)
        }
      }
    } named clue
  }

  /** Returns all entries currently enqueued without dequeueing them. */
  def getEnqueued(): Queue[T] = {
    get() match {
      case Left(_) => Queue()
      case Right(q) => q
    }
  }
}

/** A queue where both insertion and removal are asynchronous, based on a maximum queue size. */
class BoundedAsyncQueue[T](val queueSize: Int)(implicit ec: ExecutionContext) {
  // The null check is because of bugs that happened in several places in a similar way, where we tried passing an
  // implicit ec parameter that was sourced from an implicit val that wasn't yet initialized when we constructed the queue
  require(ec != null)

  // We use one AsyncQueue for the actual items, and another to push acknowledgement tokens backwards.
  // When the acknowledgement queue is empty, the main queue is full.

  private val queue = new AsyncQueue[T]()
  private val acks = new AsyncQueue[Unit]()
  for (i <- 1 to queueSize) acks.enqueue(())

  def enqueue(t: T): Future[Unit] = async {
    fastAwait(acks.dequeue())
    queue.enqueue(t)
  }

  def dequeue(): Future[T] = async {
    val output = fastAwait(queue.dequeue())
    acks.enqueue(())
    output
  }

  /** Returns a Source[T] that will dequeue items. If other threads also call dequeue() while the Source is
    * running, some items will go to them and will not appear in the Source's output.
    *
    * @param stopOn if this function returns true for any item, the source will stop (call onComplete) after that item.
    *               This does not affect the queue itself, nor other Sources returned later from the same queue
    *               by this method.
    */
  def source(stopOn: T => Boolean = _ => false, clue: String = "AsyncQueue.source")(implicit ec: ExecutionContext): Source[T] =
    queue.source(stopOn, clue)(ec)
}

