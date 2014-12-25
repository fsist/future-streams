package com.fsist.util.concurrent

import java.util.concurrent.atomic.AtomicReference

import akka.http.util.FastFuture
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
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
class AsyncQueue[T] extends AtomicReference[Either[Queue[Promise[T]], Queue[T]]](Right(Queue())) with LazyLogging {

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

  /** Try to dequeue an item synchronously if the queue is not empty. This competes synchronously with other calls to
    * `dequeue` and `tryDequeue`, and fairness is not guaranteed, so this method technically may never complete.
    */
  @tailrec final def tryDequeue(): Option[T] = get match {
    case l@Left(q) => // Already has waiting consumers
      None
    case r@Right(Queue()) => // Empty queue
      None
    case r@Right(q) => // Already has values. get the head
      val (t, ts) = q.dequeue
      if (!compareAndSet(r, Right(ts))) tryDequeue()
      else Some(t)
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
class BoundedAsyncQueue[T](val queueSize: Int)(implicit ec: ExecutionContext) extends LazyLogging {
  require(queueSize >= 1)

  // The null check is because of bugs that happened in several places in a similar way, where we tried passing an
  // implicit ec parameter that was sourced from an implicit val that wasn't yet initialized when we constructed the queue
  require(ec != null)

  // We use one AsyncQueue for the actual items, and another to push acknowledgement tokens backwards.
  // When the acknowledgement queue is empty, the main queue is full.

  private val queue = new AsyncQueue[T]()
  private val acks = new AsyncQueue[Unit]()
  for (i <- 1 to queueSize) acks.enqueue(())

  def enqueue(t: T): Future[Unit] = new FastFuture(acks.dequeue()) map (_ => queue.enqueue(t))

  def dequeue(): Future[T] = new FastFuture(queue.dequeue()) map (output => {
    acks.enqueue(())
    output
  })

  /** Try to dequeue an item synchronously if the queue is not empty. This competes synchronously with other calls to
    * `dequeue` and `tryDequeue`, and fairness is not guaranteed, so this method technically may never complete.
    */
  def tryDequeue(): Option[T] = {
    queue.tryDequeue() match {
      case s@Some(t) =>
        acks.enqueue(())
        s
      case None => None
    }
  }

  /** Dequeues as many items as possible synchronously, and returns them in a single batch in an already-completed future.
    * If no items can be dequeued synchronously, returns a future that will complete with a single item, via an ordinary
    * call to `dequeue`.
    */
  def dequeueBatch(): Future[IndexedSeq[T]] = {
    val builder = IndexedSeq.newBuilder[T]

    @tailrec def next(): Unit = tryDequeue() match {
      case Some(t) =>
        builder += t
        next()
      case None =>
        builder.result()
    }

    next()

    val dequeued = builder.result()
    if (dequeued.nonEmpty) Future.successful(dequeued)
    else dequeue() map (IndexedSeq.apply(_))
  }
}

