package com.fsist.stream

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger, AtomicReference}

import akka.http.util.FastFuture
import com.fsist.stream.Transform.Aside
import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent._
import org.reactivestreams.{Subscriber, Subscription, Publisher}

import scala.concurrent.{Promise, ExecutionContext, Future}
import scala.util.control.ControlThrowable

import scala.language.implicitConversions

/** Any stream component that produces elements to a downstream Sink. */
sealed trait SourceComponent[+Out] extends StreamComponentBase with SourceOps[Out] {
  override protected def sourceComponent: SourceComponent[Out] = this

  /** Irreversibly join this source with that sink. */
  def connect(next: SinkComponent[Out]): next.type = {
    builder.connect(this, next)
    next
  }

  /** Irreversibly join this source with that sink. */
  def to(sink: SinkComponent[Out]): sink.type = connect(sink)

  /** Irreversibly join this source with that pipe's sink. */
  def to[Next](pipe: Pipe[Out, Next]): pipe.type = {
    connect(pipe.sink)
    pipe
  }

  /** Irreversibly join this source with that sink. */
  def to[Res](sink: Sink[Out, Res]): sink.type = {
    connect(sink.sinkComponent)
    sink
  }

  /** Irreversibly join this source with that transform. */
  def transform[Next](tr: Transform[Out, Next]): tr.type = connect(tr)
}

object SourceComponent {
  implicit def get[Out](source: Source[Out]): SourceComponent[Out] = source.sourceComponent

  implicit def get[Out](pipe: Pipe[_, Out]): SourceComponent[Out] = pipe.sourceComponent
}

/** This trait allows extending the sealed SourceComponent trait inside this package. */
private[stream] trait SourceComponentBase[+Out] extends SourceComponent[Out]

/** Marker trait of all exceptions used by the streams library for flow control.
  *
  * NOTE that extending ControlThrowable means scala.util.control.NonFatal does NOT catch exceptions of this type.
  */
sealed trait StreamControlThrowable extends ControlThrowable

/** Thrown by StreamInput.producer to indicate the stream has completed. */
case class EndOfStreamException() extends Exception("End of stream")

/** A Source that introduces data into the stream from elsewhere, rather than from an upstream component. */
sealed trait StreamInput[+Out] extends SourceComponentBase[Out]

/** A Source that produces data by repeatedly calling the user-provided function `producer`. */
sealed trait StreamProducer[+Out] extends StreamInput[Out] {

  /** Called non-concurrently to produce the source elements. To indicate EOF, this function needs to throw a
    * EndOfStreamException.
    */
  def producer: Func[Unit, Out]

  def onError: Func[Throwable, Unit]
}

/** A trait that allows implementing a custom StreamInput that produces items synchronously.
  *
  * This often allows writing more elegant code for complex stateful producers.
  */
trait SyncStreamProducer[+Out] extends StreamProducer[Out] with SyncFunc[Unit, Out] with NewBuilder {
  final override def producer: Func[Unit, Out] = this

  final override def apply(a: Unit): Out = produce()

  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  /** Called to produce each successive element in the stream. Should throw a EndOfStreamException to indicate EOF.
    *
    * Equivalent to StreamInput.producer. See the README for concurrency issues.
    */
  def produce(): Out

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}

/** A trait that allows implementing a custom StreamInput that produces items asynchronously.
  *
  * This often allows writing more elegant code for complex stateful producers.
  */
trait AsyncStreamProducer[+Out] extends StreamProducer[Out] with AsyncFunc[Unit, Out] with NewBuilder {
  final override def producer: Func[Unit, Out] = this

  final override def apply(a: Unit)(implicit ec: ExecutionContext): Future[Out] = produce()(ec)

  final override def onError: Func[Throwable, Unit] = SyncFunc((th: Throwable) => onError(th))

  /** Called to produce each successive element in the stream. Should throw a EndOfStreamException to indicate EOF.
    *
    * Equivalent to StreamInput.producer. See the README for concurrency issues.
    */
  def produce()(implicit ec: ExecutionContext): Future[Out]

  /** Called if the stream fails. Equivalent to StreamInput.onError. See the README for concurrency issues. */
  def onError(throwable: Throwable): Unit = ()
}

/** A Source producing elements from an Iterator. */
final case class IteratorSource[+Out](builder: FutureStreamBuilder, iter: Iterator[Out]) extends StreamProducer[Out] with SyncFunc[Unit, Out] {
  override def producer: Func[Unit, Out] = this

  override def apply(unit: Unit): Out = if (iter.hasNext) iter.next() else throw new EndOfStreamException

  def onError: Func[Throwable, Unit] = Func.nop
}

/** A Source that generates elements by calling a user-supplied `producer` function. */
final case class GeneratorSource[+Out](builder: FutureStreamBuilder, producer: Func[Unit, Out], onError: Func[Throwable, Unit]) extends StreamProducer[Out]

/** A StreamInput or more complex Source which will become available, and start operating, once `future` is fulfilled. */
final case class DelayedSource[+Out](builder: FutureStreamBuilder, future: Future[Source[Out]]) extends StreamInput[Out]

/** A StreamInput that can be driven directly once the stream is running, providing the most efficient option for
  * sending input into a running stream. Use with `Source.drive`.
  *
  * The interface used to drive the stream is available via `aside` once the containing stream starts running.
  * NOTE that you MUST call the onNext and onComplete functions on the StreamConsumer non-concurrently,
  * or the stream implementation will break.
  */
final case class DrivenSource[Out](builder: FutureStreamBuilder) extends StreamInput[Out] with Aside[StreamConsumer[Out, Unit]] {
  private[stream] val asidePromise = Promise[StreamConsumer[Out, Unit]]()

  override def aside: Future[StreamConsumer[Out, Unit]] = asidePromise.future
}

/** A part of a stream with a single unconnected SourceComponent.
  *
  * It can represent a single component (a StreamInput), or multiple components (inputs, transformers and connectors)
  * which are already fully connected to one another.
  */
final case class Source[+Out](sourceComponent: SourceComponent[Out]) extends SourceOps[Out] {
  def builder: FutureStreamBuilder = sourceComponent.builder

  /** Irreversibly join this source with that sink. */
  def connect(next: SinkComponent[Out]): next.type = {
    builder.connect(sourceComponent, next)
    next
  }

  /** Irreversibly join this source with that sink. */
  def to(sink: SinkComponent[Out]): sink.type = connect(sink)

  /** Irreversibly join this source with that sink. */
  def to[Res](sink: Sink[Out, Res]): sink.type = {
    connect(sink.sinkComponent)
    sink
  }

  /** Irreversibly join this source with that pipe and returns a new Source containing both. */
  def through[Next](pipe: Pipe[Out, Next]): Source[Next] = {
    connect(pipe.sink)
    Source(pipe.source)
  }

  /** Irreversibly join this source with that transform and returns a new Source containing both. */
  def transform[Next](tr: Transform[Out, Next]): Source[Next] = Source(connect(tr))
}

object Source {
  implicit def make[Out](component: SourceComponent[Out]): Source[Out] = Source(component)

  def of[Out](ts: Out*)(implicit builder: FutureStreamBuilder): StreamInput[Out] =
    from(ts)

  def from[Out](iter: Iterable[Out])(implicit builder: FutureStreamBuilder): StreamInput[Out] =
    from(iter.iterator)

  def from[Out](iter: Iterator[Out])(implicit builder: FutureStreamBuilder): StreamInput[Out] =
    IteratorSource(builder, iter)

  def generateFunc[Out](producer: Func[Unit, Out], onError: Func[Throwable, Unit] = Func.nop)
                       (implicit builder: FutureStreamBuilder): StreamInput[Out] =
    GeneratorSource(builder, producer, onError)

  def generate[Out](producer: => Out, onError: Throwable => Unit = Func.nopLiteral)
                   (implicit builder: FutureStreamBuilder): StreamInput[Out] =
    generateFunc(SyncFunc(producer), SyncFunc(onError))

  def generateAsync[Out](producer: => Future[Out], onError: Throwable => Unit = Func.nopLiteral)
                        (implicit builder: FutureStreamBuilder): StreamInput[Out] =
    generateFunc(AsyncFunc(producer), SyncFunc(onError))

  /** Creates a Source that produces no elements. This is just an alias for `Source.of`. */
  def empty[Out]()(implicit builder: FutureStreamBuilder): StreamInput[Out] = of()

  /** Creates a Source that will produce elements from the Source eventually yielded by the `future`.
    *
    * Until `future` completes, this source does nothing.
    */
  def flatten[Out](future: Future[Source[Out]])
                  (implicit builder: FutureStreamBuilder): StreamInput[Out] =
    DelayedSource(builder, future)

  /** Creates an input that produces the elements pushed into this queue. Pushing `None` signifies end of stream, after 
    * which no more elements will be dequeued.
    */
  def from[Out](queue: AsyncQueue[Option[Out]])
               (implicit b: FutureStreamBuilder, ec: ExecutionContext): StreamInput[Out] =
    generateAsync[Out](new FastFuture(queue.dequeue()) map {
      case Some(out) => out
      case None => throw new EndOfStreamException
    })

  /** Creates an input that produces the elements pushed into this queue. Pushing `None` signifies end of stream, after 
    * which no more elements will be dequeued.
    */
  def from[Out](queue: BoundedAsyncQueue[Option[Out]])
               (implicit b: FutureStreamBuilder, ec: ExecutionContext): StreamInput[Out] =
    generateAsync[Out](new FastFuture(queue.dequeue()) map {
      case Some(out) => out
      case None => throw new EndOfStreamException
    })

  /** Creates an input that forwards the elements produced by this Reactive Streams Publisher.
    *
    * Subscribes to the Publisher only once the stream starts running.
    *
    * @param requestAtOnce how many stream elements to request() from the publisher every time it has delivered that
    *                      many elements. The value Long.MaxValue is defined by Reactive Streams as meaning 'unbounded'.
    */
  def from[Out](publisher: Publisher[_ <: Out], requestAtOnce: Long = 1L)
               (implicit b: FutureStreamBuilder, ec: ExecutionContext): StreamInput[Out] =
    new AsyncStreamProducer[Out] {
      private val subscription = new AtomicReference[Subscription]
      private val queue = new AsyncQueue[Option[Out]]()
      private val requested = new AtomicLong(0L)

      private def failStream(t: Throwable): Unit = {
        new FastFuture(b.runningStream).map(_.fail(t))
      }

      // Subscribe to the publisher when the stream starts running and `produce` is called for the first time
      private lazy val subscriber = {
        val subscriber = new Subscriber[Out] {
          override def onError(t: Throwable): Unit = failStream(t)

          override def onSubscribe(s: Subscription): Unit = {
            if (!subscription.compareAndSet(null, s)) {
              failStream(new IllegalArgumentException(s"onSubscribe called twice by Publisher"))
            }

            // Always start by requesting something
            requested.addAndGet(requestAtOnce)
            s.request(requestAtOnce)
          }

          override def onComplete(): Unit = queue.enqueue(None)
          override def onNext(t: Out): Unit = queue.enqueue(Some(t))
        }

        publisher.subscribe(subscriber)
        subscriber
      }

      override def produce()(implicit ec: ExecutionContext): Future[Out] = {
        // Subscribe by forcing lazy val
        subscriber

        queue.dequeue() map { dequeued =>
          if (requested.decrementAndGet() == 0) {
            subscription.get.request(requestAtOnce)
            requested.addAndGet(requestAtOnce)
          }
          dequeued match {
            case Some(t) => t
            case None => throw new EndOfStreamException
          }
        }
      }
    }

  /** A way to push data into a running stream from the outside. Use with `Source.pusher`.
    *
    * Elements are enqueued internally until the stream can process them. If you want back-pressure, use `AsyncPusher` instead.
    * If you start pushing elements before the stream begins running, they are queued and will be consumed
    * when the stream runs.
    *
    * After calling `complete` or `fail`, all further elements pushed in are ignored by the stream, but are still enqueued;
    * this may result in a memory leak if you hold on to the Pusher.
    *
    * Unlike StreamConsumer, if `fail` is called, the Source will first emit previously enqueued elements before failing.
    */
  class Pusher[In](val queue: AsyncQueue[Either[In, Throwable]]) {
    def push(in: In): Unit = queue.enqueue(Left(in))
    def complete(): Unit = queue.enqueue(Right(new EndOfStreamException))
    def fail(e: Throwable): Unit = queue.enqueue(Right(e))
  }

  /** Creates an input that produces the elements pushed into it from the outside.
    *
    * @see [[Pusher]]
    */
  def pusher[Out]()(implicit b: FutureStreamBuilder): Pusher[Out] with StreamInput[Out] =
    new Pusher[Out](new AsyncQueue[Either[Out, Throwable]]) with AsyncStreamProducer[Out] {
      override def builder: FutureStreamBuilder = b

      override def produce()(implicit ec: ExecutionContext): Future[Out] = queue.dequeue() map {
        case Left(out) => out
        case Right(err) => throw err
      }
    }

  /** A way to push data into a running stream from the outside. Use with `Source.asyncPusher`.
    *
    * Elements are enqueued internally until the stream can process them, with backpressure generated by the
    * [[BoundedAsyncQueue]].
    * If you start pushing elements before the stream begins running, they are queued and will be consumed
    * when the stream runs.
    *
    * After calling `complete` or `fail`, all further elements pushed in are ignored by the stream, but are still enqueued;
    * this may result in a memory leak if you hold on to the Pusher.
    *
    * Unlike StreamConsumer, if `fail` is called, the Source will first emit previously enqueued elements before failing.
    */
  class AsyncPusher[In](val queue: BoundedAsyncQueue[Either[In, Throwable]]) {
    def push(in: In): Future[Unit] = queue.enqueue(Left(in))
    def complete(): Future[Unit] = queue.enqueue(Right(new EndOfStreamException))
    def fail(e: Throwable): Future[Unit] = queue.enqueue(Right(e))
  }

  /** Creates an input that produces the elements pushed into it from the outside.
    *
    * @see [[AsyncPusher]]
    */
  def asyncPusher[Out](queueSize: Int = 1)
                      (implicit b: FutureStreamBuilder, ec: ExecutionContext): AsyncPusher[Out] with StreamInput[Out] = {
    new AsyncPusher[Out](new BoundedAsyncQueue[Either[Out, Throwable]](queueSize)) with AsyncStreamProducer[Out] {
      override def builder: FutureStreamBuilder = b

      override def produce()(implicit ec: ExecutionContext): Future[Out] = queue.dequeue() map {
        case Left(out) => out
        case Right(err) => throw err
      }
    }
  }

  /** Creates an input which allows pushing elements directly into the stream.
    *
    * When the stream is built, the `Aside` will yield a StreamConsumer implementation into which data can then be pushed.
    * If its `onNext` and `onComplete` happen to be SyncFuncs, calling them will execute all the following synchronous
    * stream processing stages on the caller's stack. This is the most efficient way of pushing data into a stream
    * from the outside.
    *
    * If the driven stream fails, subsequent calls to any of the StreamConsumer functions fail with that exception.
    * You can use this as a shortcut to fail your own driving mechanism with the same exception.
    *
    * The StreamConsumer's onError doesn't do anything; you can ignore it.
    *
    * Use with care; the StreamConsumer implementationrelies on `onNext` and `onComplete` being called non-concurrently
    * wrt. themselves and one another.
    */
  def driven[Out]()
                 (implicit b: FutureStreamBuilder): StreamInput[Out] with Aside[StreamConsumer[Out, Unit]] =
    DrivenSource[Out](b)

  /** Merges data from several inputs to one output in order, taking all data from the first input, then all data from the
    * second output, and so on. */
  def concat[Out](sources: Seq[SourceComponent[Out]]): SourceComponent[Out] = {
    if (sources.isEmpty) Source.empty[Out]
    else if (sources.size == 1) sources(0)
    else {
      implicit val builder = sources(0).builder

      val concatenator = Connector.concatenate[Out](sources.size)
      concatenator.connectInputs(sources)
      concatenator.output
    }
  }
}
