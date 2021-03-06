package com.fsist.stream

import com.fsist.stream.run.{RunningOutput, RunningStream, FutureStreamBuilder}
import com.fsist.util.concurrent.{BoundedAsyncQueue, AsyncFunc, SyncFunc, Func}

import scala.annotation.unchecked.uncheckedVariance
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

  def asyncPuller[In](queueSize: Int = 1)
                     (implicit builder: FutureStreamBuilder, ec: ExecutionContext): StreamOutput[In, Unit] with AsyncPuller[In] =
  new AsyncPuller[In](new BoundedAsyncQueue[Option[In]](queueSize)) with AsyncStreamConsumer[In, Unit] {
    override def onNext(in: In)(implicit ec: ExecutionContext): Future[Unit] = queue.enqueue(Some(in))
    override def complete()(implicit ec: ExecutionContext): Future[Unit] = queue.enqueue(None)
  }
}

