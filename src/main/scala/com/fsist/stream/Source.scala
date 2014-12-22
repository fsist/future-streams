package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent._

import scala.concurrent.{ExecutionContext, Future}
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

  /** Irreversibly join this source with that sink. */
  def to[Res](sink: Sink[Out, Res]): sink.type = {
    connect(sink.sinkComponent)
    sink
  }

  /** Irreversibly join this source with that pipe and return a new Pipe containing both. */
  def through[Next](pipe: Pipe[Out, Next]): pipe.type = {
    connect(pipe.sink)
    pipe
  }

  /** Irreversibly join this source with that transform. */
  def transform[Next](tr: Transform[Out, Next]): tr.type = connect(tr)
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
sealed trait StreamInput[+Out] extends SourceComponentBase[Out] {

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
trait SyncStreamInput[+Out] extends StreamInput[Out] with SyncFunc[Unit, Out] with NewBuilder {
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
trait AsyncStreamInput[+Out] extends StreamInput[Out] with AsyncFunc[Unit, Out] with NewBuilder {
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
final case class IteratorSource[+Out](builder: FutureStreamBuilder, iter: Iterator[Out]) extends StreamInput[Out] with SyncFunc[Unit, Out] {
  override def producer: Func[Unit, Out] = this

  override def apply(unit: Unit): Out = if (iter.hasNext) iter.next() else throw new EndOfStreamException

  def onError: Func[Throwable, Unit] = Func.nop
}

/** A Source that generates elements by calling a user-supplied `producer` function. */
final case class GeneratorSource[+Out](builder: FutureStreamBuilder, producer: Func[Unit, Out], onError: Func[Throwable, Unit]) extends StreamInput[Out]

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

  def of[Out](ts: Out*)(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    from(ts)

  def from[Out](iter: Iterable[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    from(iter.iterator)

  def from[Out](iter: Iterator[Out])(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    IteratorSource(builder, iter)

  def generateFunc[Out](producer: Func[Unit, Out], onError: Func[Throwable, Unit] = Func.nop)
                       (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    GeneratorSource(builder, producer, onError)

  def generate[Out](producer: => Out, onError: Throwable => Unit = Func.nopLiteral)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    generateFunc(SyncFunc(producer), SyncFunc(onError))

  def generateAsync[Out](producer: Unit => Future[Out], onError: Throwable => Unit = Func.nopLiteral)
                        (implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] =
    generateFunc(AsyncFunc(producer), SyncFunc(onError))

  /** Creates a Source that produces no elements. This is just an alias for `Source.of`. */
  def empty[Out]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder()): StreamInput[Out] = of()

  /** Creates an input that produces the elements pushed into this queue. Pushing `None` signifies end of stream, after 
    * which no more elements will be dequeued.
    */
  def from[Out](queue: AsyncQueue[Option[Out]])
               (implicit b: FutureStreamBuilder = new FutureStreamBuilder(), ec: ExecutionContext): StreamInput[Out] =
    generateAsync[Out](Func(queue.dequeue() map {
      case Some(out) => out
      case None => throw new EndOfStreamException
    }))

  /** Creates an input that produces the elements pushed into this queue. Pushing `None` signifies end of stream, after 
    * which no more elements will be dequeued.
    */
  def from[Out](queue: BoundedAsyncQueue[Option[Out]])
               (implicit b: FutureStreamBuilder = new FutureStreamBuilder(), ec: ExecutionContext): StreamInput[Out] =
    generateAsync[Out](Func(queue.dequeue() map {
      case Some(out) => out
      case None => throw new EndOfStreamException
    }))

  /** A way to push data into a running stream from the outside. Use with `Source.pusher`.
    *
    * Elements are enqueued internally until the stream can process them. If you want back-pressure, use `AsyncPusher` instead.
    *
    * After calling `complete`, all further elements pushed in are ignored by the stream, but are still enqueued;
    * this may result in a memory leak if you hold on to the Pusher.
    */
  class Pusher[In](val queue: AsyncQueue[Option[In]]) {
    def push(in: In): Unit = queue.enqueue(Some(in))

    def complete(): Unit = queue.enqueue(None)
  }

  /** Creates an input that produces the elements pushed into it from the outside using the methods on trait [[Pusher]].
    *
    * This uses an [[AsyncQueue]] internally. Pushed elements are enqueued until consumed by the stream.
    *
    * NOTE: if you start pushing elements before the stream begins running, they are queued and will be consumed
    * when the stream runs.
    */
  def pusher[Out]()(implicit b: FutureStreamBuilder = new FutureStreamBuilder()): Pusher[Out] with StreamInput[Out] =
    new Pusher[Out](new AsyncQueue[Option[Out]]) with AsyncStreamInput[Out] {
      override def builder: FutureStreamBuilder = b

      override def produce()(implicit ec: ExecutionContext): Future[Out] = queue.dequeue() map {
        case Some(out) => out
        case None => throw new EndOfStreamException
      }
    }


  /** A way to push data into a running stream from the outside. Use with `Source.asyncPusher`.
    *
    * Elements are enqueued internally until the stream can process them, with backpressure generated by the
    * [[BoundedAsyncQueue]].
    *
    *
    * After calling `complete`, all further elements pushed in are ignored by the stream, but are still enqueued;
    * this may result in a memory leak if you hold on to the Pusher.
    */
  class AsyncPusher[In](val queue: BoundedAsyncQueue[Option[In]]) {
    def push(in: In): Future[Unit] = queue.enqueue(Some(in))
    def complete(): Future[Unit] = queue.enqueue(None)
  }

  /** Creates an input that produces the elements pushed into it from the outside using the methods on trait [[Pusher]].
    *
    * This uses a [[BoundedAsyncQueue]] internally. Pushed elements are enqueued until consumed by the stream.
    *
    * NOTE: if you start pushing elements before the stream begins running, they are queued and will be consumed
    * when the stream runs.
    */
  def asyncPusher[Out](queueSize: Int = 1)
                      (implicit b: FutureStreamBuilder = new FutureStreamBuilder(), ec: ExecutionContext): AsyncPusher[Out] with StreamInput[Out] = {
    new AsyncPusher[Out](new BoundedAsyncQueue[Option[Out]](queueSize)) with AsyncStreamInput[Out] {
      override def builder: FutureStreamBuilder = b

      override def produce()(implicit ec: ExecutionContext): Future[Out] = queue.dequeue() map {
        case Some(out) => out
        case None => throw new EndOfStreamException
      }
    }
  }
}
