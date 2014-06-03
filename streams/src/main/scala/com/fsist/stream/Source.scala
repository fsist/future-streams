package com.fsist.stream

import com.fsist.util.{BoundedAsyncQueue, CancelToken}
import com.typesafe.scalalogging.slf4j.Logging
import org.reactivestreams.api.{Consumer, Producer}
import org.reactivestreams.spi.{Subscription, Subscriber, Publisher}
import scala.async.Async._
import scala.concurrent.{Promise, ExecutionContext, Future}

/** This trait combines the ReactiveStreams SPI Producer and API Publisher. Please read the ReactiveStreams
  * documentation.
  *
  * In addition, it adds the start() and onSourceDone methods.
  *
  * NOTE that the semantics of when generic ReactiveStreams Producers start actually producing elements are different
  * from those of Source. A Producer will start sending elements as soon as a consumer subscribes and calls request().
  * However, a Source will only start doing so after its start() method is called. This is indended to allow convenient
  * construction of pipelines containing many source-sink+source-... pairs and calling Start only when all of them
  * have been connected.
  *
  * NOTE that if a Source has subscribed consumers, it will simply calculate and discard all the values it would produce
  * normally, without waiting for anything.
  */
trait Source[T] extends Producer[T] with Publisher[T] with NamedLogger {
  /** Default EC used by mapping operations */
  def ec: ExecutionContext

  /** Default cancel token inherited by mapping operations */
  def cancelToken: CancelToken

  /** Start producing elements. The returned future will complete when this source stops (either via exhaustion or with
    * an error).
    *
    * May not be called more than once (although implementations should defensively make subsequent calls
    * either idempotent or no-op failures).
    */
  def start(): Future[Unit]

  /** Returns a future that is completed when the source has generated end-of-input or experienced an error (in which case
    * the future is failed). This future may not be identical to the one returned by start(), but it should complete in
    * the same way. */
  def onSourceDone: Future[Unit]

  /// Beyond this line are combinators and other methods with concrete implementations.

  /** See [[Pipe.mapSink]] */
  def map[K](f: T => K)(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[K] = this >> Pipe.map(f)

  /** See [[Pipe.flatMapSink]] */
  def flatMap[K](f: T => Future[K])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[K] = this >> Pipe.flatMap(f)

  /** See [[Pipe.mapInput]] */
  def mapInput[K](f: Option[T] => Option[K])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[K] = this >> Pipe.mapInput(f)

  /** See [[Pipe.flatMapInput]] */
  def flatMapInput[K](f: Option[T] => Future[Option[K]])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[K] = this >> Pipe.flatMapInput(f)

  /** Combines a source with a pipe and returns a new source representing both. */
  def >>[B](pipe: Pipe[T, B, _])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[B] = {
    val prev = this
    prev.subscribe(pipe)

    new Source[B] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      override def start(): Future[Unit] = {
        prev.start()
        pipe.start()
      }
      override def onSourceDone: Future[Unit] = pipe.onSourceDone
      override def subscribe(subscriber: Subscriber[B]): Unit = pipe.subscribe(subscriber)
      override def produceTo(consumer: Consumer[B]): Unit = pipe.produceTo(consumer)
      override def getPublisher: Publisher[B] = pipe.getPublisher
    } named s"${prev.name} >> ${pipe.name}"
  }

  /** Subscribe the sink to this source, start running, and return the sink's result (`Sink.resultOnDone`). */
  def >>|[R](sink: Sink[T, R])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Future[R] = {
    subscribe(sink)
    start()
    sink.resultOnDone
  }

  /** Creates a Source that publishes all elements in this Source, and after it completes, all elements in the `next` Source.
    */
  def concat(next: Source[T])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[T] = Source.concat(Seq(this, next))

  /** Alias for `concat`. */
  def ++(next: Source[T])(implicit ecc: ExecutionContext = ec, cancel: CancelToken = cancelToken): Source[T] = concat(next)

  /** If `next` is Some pipe, return `pipe >> next.get`, discarding the pipe's result; otherwise return this source. */
  def >>?(next: Option[Pipe[T, T, _]]): Source[T] = next match {
    case Some(pipe) => this >> pipe
    case None => this
  }
}

object Source extends Logging {

  /** Create a Source that generates elements from this Iterator. */
  def from[T](iter: Iterator[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override val ec: ExecutionContext = ecc
    override val cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = Future {
      if (iter.hasNext) Some(iter.next)
      else None
    }
  } named "Source.from"

  /** Create a Source that generates elements from this Iterable. */
  def from[T](iter: Iterable[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    from(iter.iterator)

  /** Create a Source that generates these elements. */
  def apply[T](ts: T*)(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = from(ts.toIterable)

  /** Create an empty Source. */
  def empty[T](implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = apply()

  /** Create a Source that will call this function to produce each new element.
    *
    * The function should return Some(t) as a new element, None to indicate the end of stream, or fail with an error
    * that will be propagated to Sinks.
    *
    * The function will not be called again until after each previously returned Future completes.
    * It will never be called again after it returns None or fails.
    *
    * If the function throws an exception synchronously, it will be treated as if it returned a failed Future.
    */
  def generateM[T](stepper: => Future[Option[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] = new SourceImpl[T] {
    override val ec: ExecutionContext = ecc
    override val cancelToken: CancelToken = cancel

    protected def produce(): Future[Option[T]] = stepper
  } named "Source.generateM"

  /** Create a Source that will call this function to produce each new element.
    *
    * The function should return Some(t) as a new element, None to indicate the end of stream, or throw an exception
    * that will be propagated to Sinks.
    *
    * The function will not be called in parallel. It will never be called again after it returns None or fails.
    */
  def generate[T](stepper: => Option[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new SourceImpl[T] {
      override val ec: ExecutionContext = ecc
      override val cancelToken: CancelToken = cancel

      protected def produce(): Future[Option[T]] = try {
        Future(stepper)
      }
      catch {
        case e: Throwable => Future.failed(e)
      }
    } named "Source.generate"

  /** Wrap an existing Producer in a Source.
    *
    * NOTE the difference between generic Producers and Sources: a Source only starts producing elements after
    * start() is called. To implement start() in the Source returned by this method, we buffer all calls to subscribe()
    * until start() is called.
    */
  def from[T](producer: Producer[T])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new Source[T] {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      /** Completed when the producer sends EOF or error */
      private val completion = Promise[Unit]()

      /** Has start() been called yet? */
      private val started = Promise[Unit]()

      override def start(): Future[Unit] = {
        started.trySuccess(())
        completion.future
      }

      override def onSourceDone: Future[Unit] = completion.future

      override def produceTo(consumer: Consumer[T]): Unit = {
        if (started.isCompleted) producer.produceTo(consumer)
        else started.future map (_ => producer.produceTo(consumer))
      }

      override def getPublisher: Publisher[T] = producer.getPublisher

      override def subscribe(subscriber: Subscriber[T]): Unit = {
        if (started.isCompleted) producer.getPublisher.subscribe(subscriber)
        else started.future map (_ => producer.getPublisher.subscribe(subscriber))
      }
    } named "Source.from(Producer)"

  /** Returns a Source that outputs the concatenated output of all these sources.
    *
    * NOTE that the concatenating source subscribes to and calls start() on each argument Source in turn.
    * If any of the argument Sources are already started, then any output they produce before the concatenating Source
    * switches to them will not appear in the concatenating Source's output.
    *
    * An alternative would be to subscribe to all the argument Sources immediately, and not request() anything from them
    * until it was their turn in the concatenation. However, that would effectively block them and their other subscribers.
    *
    * TODO: replace with a custom Producer/Consumer impl that passes through calls directly and doesn't involve an extra
    * async stage and queue. This should become a PML for Producer which doesn't even depend on our custom Source.
    */
  def concat[T](sources: Iterable[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken = CancelToken.none): Source[T] =
    new Source[T] with NamedLogger {
      override def ec: ExecutionContext = ecc
      override def cancelToken: CancelToken = cancel

      private val iter = sources.iterator
      private var iterCompleted : Boolean = false

      private val pipe = new PipeSegment.Passthrough[T] {
        override def ec: ExecutionContext = ecc
        override def cancelToken: CancelToken = cancel

        override def process(input: Option[T]): Future[Boolean] =
          input match {
            case s: Some[T] => emit(s) map (_ => false)
            case None =>
              if (iterCompleted) {
                emit(None) map (_ => true)
              }
              else {
                falseFuture
              }
          }
      } named "Source.concat internal pipe"

      private def runNext(): Unit = {
        if (iter.hasNext) {
          val source = iter.next()
          logger.trace(s"switching to next source ${source.name}")
          source.subscribe(pipe)
          source.start()
          source.onSourceDone map { _ =>
            // If the source fails, it will notify the pipe of completion
            logger.trace(s"source ${source.name} done")
            runNext()
          }
        }
        else {
          logger.trace(s"out of sources")
          iterCompleted = true
          pipe.onComplete()
        }
      }

      // Lazy val to ensure start() is idempotent
      private[this] lazy val started: Future[Unit] = {
        logger.trace(s"Starting concat pipe")
        pipe.start()
        runNext()
        success
      }

      override def start(): Future[Unit] = started

      override def onSourceDone: Future[Unit] = pipe.onSourceDone
      override def subscribe(subscriber: Subscriber[T]): Unit = pipe.subscribe(subscriber)
      override def produceTo(consumer: Consumer[T]): Unit = pipe.produceTo(consumer)
      override def getPublisher: Publisher[T] = pipe
    } named "Source.concat"

  /** Converts a Future[Source] to a Source that will start producing elements after the `future` completes. */
  def flatten[T](future: Future[Source[T]])(implicit ecc: ExecutionContext, cancel: CancelToken) : Source[T] ={
    val pipe = PipeSegment.Passthrough[T]() named "Source.flatten internal pipe"
    future onSuccess {
      case source => source >>| pipe
    }
    future onFailure {
      case e => pipe.onError(e)
    }
    pipe
  } named "Source.flatten"
}
