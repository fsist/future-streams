package com.fsist.stream

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

import akka.http.util.FastFuture
import com.fsist.stream.run.{RunningOutput, RunningStream, FutureStreamBuilder}
import com.fsist.util.concurrent.{BoundedAsyncQueue, AsyncFunc, SyncFunc, Func}
import org.reactivestreams
import org.reactivestreams.{Subscription, Subscriber, Publisher}

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Promise, Future, ExecutionContext}

import scala.language.higherKinds
import scala.language.implicitConversions

/** Any stream component that receives input elements from a Source. */
sealed trait SinkComponent[-In] extends StreamComponentBase {
}

object SinkComponent {
  implicit def get[In](sink: Sink[In, _]): SinkComponent[In] = sink.sinkComponent

  implicit def get[In](pipe: Pipe[In, _]): SinkComponent[In] = pipe.sink
}


/** This trait allows extending the sealed SinkComponent trait inside this package. */
private[stream] trait SinkComponentBase[-In] extends SinkComponent[In]

/** A Sink that sends data outside the stream, calculates a result, and/or has some other useful side effects.
  */
sealed trait StreamOutput[-In, +Res] extends SinkComponent[In] {
  /** A shortcut method that calls `build` and returns the RunningStreamComponent representing `this`. */
  def buildAndGet()(implicit ec: ExecutionContext): RunningOutput[In, Res] = build()(ec)(this)

  /** A shortcut method that calls `build` and returns the future result produced by this component. */
  def buildResult()(implicit ec: ExecutionContext): Future[Res] = buildAndGet()(ec).result

  /** Returns the result future that will eventually be completed when the stream runs. This is identical to the Future
    * returned by the RunningStream for this component, which is also returned by `buildResult`.
    *
    * This method is useful when you want to know about the completion and/or the result in a location other than the one
    * where you actually run the stream, such as when you produce a Sink and give it to someone else to use.
    */
  def futureResult(): Future[Res] = futureResultPromise.future

  // We guarantee not to violate the variance, because we fulfill this promise using the result value of `onComplete`
  // (or the equivalent) on the same component
  private[stream] val futureResultPromise: Promise[Res@uncheckedVariance] = Promise[Res]()
}

/** See the README for the semantics of the three onXxx functions.
  */
sealed trait StreamConsumer[-In, +Res] extends StreamOutput[In, Res] {
  /** Called on each input element, non-concurrently with itself and onComplete. */
  def onNext: Func[In, Unit]

  /** Called on each input element, non-concurrently with itself and onNext.
    *
    * Can only be called once, and no more calls to onNext are allowed afterwards. */
  def onComplete: Func[Unit, Res]

  /** Called if the stream fails. See the README on the semantics of stream failure. This method is guaranteed to be called
    * exactly once.
    *
    * This is called *concurrently* with onNext and onComplete.
    */
  def onError: Func[Throwable, Unit]
}

/** This trait allows extending the sealed StreamConsumer trait inside this package. */
private[stream] trait StreamConsumerBase[-In, +Res] extends StreamConsumer[In, Res]

/** A trait that allows implementing a custom StreamOutput that processes items synchronously.
  *
  * This often allows writing more elegant code for complex stateful consumers.
  */
trait SyncStreamConsumer[-In, +Res] extends StreamConsumer[In, Res] with SyncFunc[In, Unit] with NewBuilder {
  final override def onNext: Func[In, Unit] = this

  final override def onComplete: Func[Unit, Res] = complete()

  final override def apply(in: In): Unit = onNext(in)

  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  /** Called to process each successive element in the stream.
    *
    * Equivalent to StreamOutput.onNext. See the README for concurrency issues.
    */
  def onNext(in: In): Unit

  /** Called on EOF to produce the final result.
    *
    * Equivalent to StreamInput.onComplete. See the README for concurrency issues.
    */
  def complete(): Res

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}


/** A trait that allows implementing a custom StreamOutput that processes items asynchronously.
  *
  * This often allows writing more elegant code for complex stateful consumers.
  */
trait AsyncStreamConsumer[-In, +Res] extends StreamConsumer[In, Res] with AsyncFunc[In, Unit] with NewBuilder {
  final override def onNext: Func[In, Unit] = this

  final override def onComplete: Func[Unit, Res] = AsyncFunc.withEc((a: Unit) => (ec: ExecutionContext) => complete()(ec))

  final override def apply(in: In)(implicit ec: ExecutionContext): Future[Unit] = onNext(in)

  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  /** Called to process each successive element in the stream.
    *
    * Equivalent to StreamOutput.onNext. See the README for concurrency issues.
    */
  def onNext(in: In)(implicit ec: ExecutionContext): Future[Unit]

  /** Called on EOF to produce the final result.
    *
    * Equivalent to StreamInput.onComplete. See the README for concurrency issues.
    */
  def complete()(implicit ec: ExecutionContext): Future[Res]

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}

/** A StreamOutput represented as a triplet of onXxx functions.
  *
  * @see [[com.fsist.stream.StreamOutput]]
  */
final case class SimpleOutput[-In, +Res](builder: FutureStreamBuilder,
                                         onNext: Func[In, Unit],
                                         onComplete: Func[Unit, Res],
                                         onError: Func[Throwable, Unit]) extends StreamConsumer[In, Res]

object SimpleOutput {
  def apply[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit])
                    (implicit builder: FutureStreamBuilder): SimpleOutput[In, Res] =
    apply(builder, onNext, onComplete, onError)
}

/** A StreamOutput or more complex Sink which will become available, and start operating, once `future` is fulfilled. */
final case class DelayedSink[-In, +Res](builder: FutureStreamBuilder, future: Future[Sink[In, Res]]) extends StreamOutput[In, Res]

/** A part of a stream with a single unconnected SinkComponent.
  *
  * It can represent a single component (a StreamOutput), or multiple components (an output, transformers and connectors)
  * which are already fully connected to one another. Its result is that of the original StreamOutput.
  */
final case class Sink[-In, +Res](sinkComponent: SinkComponent[In], output: StreamOutput[Nothing, Res]) {
  implicit def builder: FutureStreamBuilder = output.builder

  /** @see [[SinkComponent.build()]]
    */
  def build()(implicit ec: ExecutionContext): RunningStream = output.build()

  /** A shortcut method that calls `build` and returns the RunningStreamComponent representing `this`. */
  def buildAndGet()(implicit ec: ExecutionContext): RunningOutput[Nothing, Res] = output.buildAndGet()

  /** A shortcut method that calls `build` and returns the future result produced by this component. */
  def buildResult()(implicit ec: ExecutionContext): Future[Res] = output.buildResult()
}

object Sink {
  implicit def apply[In, Res](output: StreamOutput[In, Res]): Sink[In, Res] = Sink(output, output)

  def foreach[In, Res](onNext: In => Unit, onComplete: => Res = Func.nopLiteral, onError: Throwable => Unit = Func.nopLiteral)
                      (implicit builder: FutureStreamBuilder): StreamOutput[In, Res] =
    SimpleOutput(builder, onNext, onComplete, onError)

  def foreachAsync[In, Res](onNext: In => Future[Unit], onComplete: => Future[Res] = futureSuccess,
                            onError: Throwable => Unit = Func.nopLiteral)
                           (implicit builder: FutureStreamBuilder): StreamOutput[In, Res] =
    SimpleOutput(builder, AsyncFunc(onNext), AsyncFunc(onComplete), onError)

  def foreachFunc[In, Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                          (implicit builder: FutureStreamBuilder): StreamOutput[In, Res] =
    SimpleOutput(builder, onNext, onComplete, onError)

  /** A sink that discards its output and calculates nothing. */
  def discard[In]()(implicit builder: FutureStreamBuilder): StreamOutput[In, Unit] =
    SimpleOutput(builder, Func.nop, Func.pass, Func.nop)

  /** Extracts the single element of the input stream as the result.
    *
    * If the stream is empty, fails with NoSuchElementException.
    * If the stream contains more than one element, fails with IllegalArgumentException.
    */
  def single[In]()(implicit builder: FutureStreamBuilder): StreamOutput[In, In] = {
    def b = builder
    new SyncStreamConsumer[In, In] {
      override def builder: FutureStreamBuilder = b

      private var cell: Option[In] = None

      override def onNext(in: In): Unit = {
        if (cell.isEmpty) cell = Some(in)
        else throw new IllegalArgumentException("More than one element in stream")
      }

      override def complete(): In = cell.getOrElse(throw new NoSuchElementException("Stream was empty"))
    }
  }

  /** Creates a Sink that will wait for the `future` to complete and then feed data into the Sink it yields. */
  def flatten[In, Res](future: Future[Sink[In, Res]])
                      (implicit builder: FutureStreamBuilder): StreamOutput[In, Res] =
    DelayedSink(builder, future)

  /** Pass the results of the stream to this consumer. Intended to be used together with `Source.driven`. */
  def drive[In, Res](consumer: StreamConsumer[In, Res])
                    (implicit builder: FutureStreamBuilder): StreamOutput[In, Res] =
    foreachFunc(consumer.onNext, consumer.onComplete, consumer.onError)

  /** A way to pull data from a running Sink. Use with `Sink.asyncPuller`.
    *
    * Generates backpressure using a BoundedAsyncQueue until data is pulled.
    */
  class AsyncPuller[Out](val queue: BoundedAsyncQueue[Option[Out]]) {

    /** Returns the next element. Fails with [[EndOfStreamException]] when the stream completes.
      */
    def pull()(implicit ec: ExecutionContext): Future[Out] = queue.dequeue() map {
      case Some(t) => t
      case None =>
        // Make sure pull always throws from now on
        queue.enqueue(None)
        throw new EndOfStreamException
    }
  }

  /** Creates a way to pull data from a running Sink. */
  def asyncPuller[In](queueSize: Int = 1)
                     (implicit builder: FutureStreamBuilder, ec: ExecutionContext): StreamOutput[In, Unit] with AsyncPuller[In] =
    new AsyncPuller[In](new BoundedAsyncQueue[Option[In]](queueSize)) with AsyncStreamConsumer[In, Unit] {
      override def onNext(in: In)(implicit ec: ExecutionContext): Future[Unit] = queue.enqueue(Some(in))
      override def complete()(implicit ec: ExecutionContext): Future[Unit] = queue.enqueue(None)
    }

  /** Creates a Reactive Streams Publisher. The Publisher will confirm Subscriptions right away, but will only start
    * producing data once the stream is running.
    *
    * This Publisher does not buffer data unless there are subscribers which have not request()ed it.
    * If data arrives when there are no subscribers, it is discarded. If a subscriber is added when the stream
    * is running, it will only receive data produced from that point on.
    *
    * The common use case is to add all subscribers before the stream starts running, thus guaranteeing they will all
    * receive all of the data in the same order (at the speed of the slowest subscriber).
    */
  def to[In]()(implicit builder: FutureStreamBuilder, ec: ExecutionContext): StreamOutput[In, Unit] with Publisher[In] = {

    class SubscriberCanceledException() extends Exception

    // A simple, non-general async latch that takes advantage of the fact that only one caller will be waiting at a time.
    class SubscriberState(val subscriber: Subscriber[_ >: In]) {
      // If this holds a Long, it must be positive.
      private def curr = new AtomicReference[Either[Long, Promise[Unit]]](Right(Promise[Unit]()))

      private val canceled = new AtomicBoolean(false)

      def cancel(): Unit = {
        canceled.set(true)
        curr.get() match {
          case r@Right(promise) => promise.tryFailure(new SubscriberCanceledException)
          case _ =>
        }
      }

      @tailrec final def inc(requested: Long): Unit = {
        if (requested > 0) {
          curr.get match {
            case l@Left(count) =>
              val newCount =
                if (count == Long.MaxValue || requested == Long.MaxValue || (count + requested <= 0)) Long.MaxValue
                else count + requested

              if (!curr.compareAndSet(l, Left(newCount))) inc(requested)

            case r@Right(promise) =>
              if (curr.compareAndSet(r, Left(requested))) promise.success(())
              else inc(requested)
          }
        }
      }

      /** If this returns None, then the count was positive and has been decremented by one, and a single item can
        * be sent to the subscriber.
        *
        * If this returns Some(future), then the caller must wait for the future to complete and then call `get` again.
        *
        * If this returns Some(future) and the future fails with SubscriberCanceledException, the subscriber unsubscribed
        * in a race condition and the caller should not publish anything to it.
        */
      @tailrec final def get(): Option[Future[Unit]] = {
        curr.get() match {
          case l@Left(count) if count > 1 =>
            if (curr.compareAndSet(l, Left(count - 1))) None
            else get()

          case l@Left(count) => // count == 1
            if (curr.compareAndSet(l, Right(Promise[Unit]()))) None
            else get()

          case r@Right(promise) =>
            if (canceled.get) Some(FastFuture.failed(new SubscriberCanceledException))
            else Some(promise.future)
        }
      }
    }

    case class State(subscribers: Vector[SubscriberState] = Vector.empty, completed: Boolean = false,
                     failed: Option[Throwable] = None)

    new AsyncStreamConsumer[In, Unit] with Publisher[In] {
      // The state is always updated with compare-and-set, and before successfully updating no side-effecting operations
      // which depend on the update's success are done.
      private val state = new AtomicReference[State](State())

      override def subscribe(subscriber: Subscriber[_ >: In]): Unit = {
        val requested = new AtomicLong()

        val subscription = new Subscription {
          override def request(n: Long): Unit = requested.addAndGet(n)

          @tailrec override final def cancel(): Unit = {
            val s = state.get()
            val removed = s.copy(subscribers = s.subscribers.filterNot(_.subscriber eq subscriber))

            if (state.compareAndSet(s, removed)) {
              s.subscribers.find(_.subscriber eq subscriber).foreach(_.cancel())
            }
            else cancel()
          }
        }

        @tailrec def add(): Unit = {
          state.get() match {
            case State(_, true, _) =>
              // Already completed, no need to store this subscriber
              subscriber.onSubscribe(subscription)
              subscriber.onComplete()
            case State(_, _, Some(failure)) =>
              // Already failed
              subscriber.onSubscribe(subscription)
              subscriber.onError(failure)
            case s @ State(subs, false, None) =>
              val added = State(subs :+ new SubscriberState(subscriber), false, None)
              if (! state.compareAndSet(s, added)) {
                add()
              }
              else {
                subscriber.onSubscribe(subscription)
              }
          }
        }

        add()
      }

      override def onNext(in: In)(implicit ec: ExecutionContext): Future[Unit] = {
        val subs = state.get.subscribers
        val gets = for (sub <- subs) yield (sub -> sub.get())

        gets.filter(_._2.isEmpty).foreach(_._1.subscriber.onNext(in))

        val remaining = gets.filter(_._2.isDefined)
        if (remaining.isEmpty) futureSuccess
        else {
          FastFuture.sequence(
            remaining.map {
              case (sub, future) =>
                new FastFuture(future.get).map(_ => sub.subscriber.onNext(in))
            }
          ) map (_ => ())
        }
      }

      @tailrec override def complete()(implicit ec: ExecutionContext): Future[Unit] = {
        val s = state.get
        val completed = State(Vector.empty, true, None)
        if (state.compareAndSet(s, completed)) {
          s.subscribers.foreach(_.subscriber.onComplete())
          futureSuccess
        }
        else complete()
      }

      @tailrec override def onError(throwable: Throwable): Unit = {
        val s = state.get
        val failed = State(Vector.empty, false, Some(throwable))
        if (state.compareAndSet(s, failed)) {
          s.subscribers.foreach(_.subscriber.onError(throwable))
        }
        else onError(throwable)
      }
    }
  }
}

