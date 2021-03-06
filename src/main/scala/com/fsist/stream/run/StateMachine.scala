package com.fsist.stream.run

import java.util.concurrent.atomic.AtomicInteger

import akka.http.util.FastFuture
import com.fsist.stream._
import com.fsist.util.concurrent._
import com.fsist.util.concurrent.FutureOps._
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.collection.LinearSeq
import scala.collection.immutable.BitSet
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.util.{Success, Failure}
import scala.util.control.NonFatal

// NOTE error handling is global for the entire graph. State machines no longer call downstream consumer.onError.
// When the graph fails, graph.failGraph calls the onError of all uesr components that have one, concurrently with one
// another, and concurrently with any ongoing calls to onNext or onComplete. However, if onComplete has finished
// successfully, onError will not be called for that component.
//
// failGraph fails the completionPromise of each component. Each state machine checks if the promise has been failed
// before calling onNext or onComplete; if yes, it aborts.

// Note: implementations are mutable!

/** A simplified StreamConsumer, without onError and without a Result. Exposed by each state machine with an input. */
private[run] case class Consumer[-In](onNext: Func[In, Unit], onComplete: Func[Unit, Unit])

private[run] sealed trait ConsumerProvider[In] {
  def consumer: Consumer[In]
}

private[run] object ConsumerProvider {
  def apply[T](machine: ConnectorMachine[T], inputIndex: Int): ConsumerProvider[T] =
    new ConsumerProvider[T] {
      override lazy val consumer: Consumer[T] = machine.consumer(inputIndex)
    }
}

private[run] sealed trait StateMachine extends LazyLogging {
  implicit def ec: ExecutionContext

  def running: RunningStreamComponent

  def graph: GraphOps

  /** Backing promise of RunningStreamComponent.completion, see docs there. */
  val completionPromise = Promise[Unit]()

  def userOnError: Func[Throwable, Unit]

  /** Called for all components when the graph fails; they will notice and abort before the next time they call the user's
    * onNext and onComplete. They will also call the user's onError exactly once, unless they have already completed. */
  def fail(th: Throwable)(implicit ec: ExecutionContext): Unit = {
    completionPromise.tryFailure(th)

    // Fire and forget
    (userOnError match {
      case syncf: SyncFunc[Throwable, Unit] => exceptionToFailure(Future {
        syncf(th)
      })

      case asyncf: AsyncFunc[Throwable, Unit] => exceptionToFailure(asyncf(th))
    }) recover {
      case NonFatal(e) => logger.error(s"Error in user onError handler", e)
    }
  }

  def isFailed: Boolean = completionPromise.isCompleted && completionPromise.future.value.get.isFailure

  /** Returns true if NOT completed, false if completed successfully, and throws the failure exception if failed. */
  def throwIfFailed(): Boolean =
    if (!completionPromise.isCompleted) true
    else completionPromise.future.value.get match {
      case Failure(e) => throw e
      case Success(_) => false
    }

  def failure: Option[Throwable] =
    completionPromise.future.value.flatMap(_ match {
      case Failure(e) => Some(e)
      case _ => None
    })
}

/** Operations on the whole graph (the running FutureStream) exposed by the builder to all state machines in the graph. */
private[run] trait GraphOps {

  /** Provided by the builder to all components. When called, fails all components in the graph by calling their
    * `fail` methods. */
  def failGraph(th: Throwable): Unit
}

/** All machines that need to have independent loops started. This includes all inputs, but also e.g. always-async machines
  * which read from an input AsyncQueue. */
private[run] sealed trait RunnableMachine extends StateMachine {

  /** Called in the context of a new Future */
  def run(): Unit
}

private[run] sealed trait StateMachineWithOneOutput[Out] extends StateMachine {
  type TOut = Out

  // Downstream component. Initialized by the stream builder before running
  var next: Option[ConsumerProvider[TOut]] = None
}

private[run] sealed trait ConnectorMachine[T] extends StateMachine {
  type TT = T

  def connector: Connector[T]

  def running: RunningConnector[T]

  // Downstream components. Initialized by the stream builder before running
  val nexts: ArrayBuffer[Option[ConsumerProvider[TT]]] = ArrayBuffer.fill(connector.outputs.size)(None)

  def consumers: IndexedSeq[Consumer[T]]

  def consumer(index: Int): Consumer[T] = consumers(index)
}

/** A Connector which provides the same consumer on every input (or only has one input). */
private[run] sealed trait ConnectorMachineWithUniformInput[T] extends ConnectorMachine[T] with ConsumerProvider[T] {
  override def consumers: IndexedSeq[Consumer[T]] = Vector.fill(connector.inputs.size)(consumer)
}

private[run] object StateMachine extends LazyLogging {

  class ProducerMachine[Out](val input: StreamProducer[Out], val graph: GraphOps)
                            (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] with RunnableMachine {
    override val running: RunningInput[Out] = RunningInput(completionPromise.future, input)

    // Acquire copies of user functions
    val producer = input.producer
    override val userOnError: Func[Throwable, Unit] = input.onError

    override def run(): Unit = {
      require(next.isDefined, "Graph must be fully linked before running")

      val Consumer(onNext, onComplete) = next.get.consumer
      val handleComplete = onComplete.recover {
        case NonFatal(e) => graph.failGraph(e)
      }

      val mainAction = producer ~> onNext

      // The main loop may run synchronously here, because this method (`run`) is invoked in a new Future
      def mainLoop: Future[Unit] = exceptionToFailure(
        mainAction match {
          case syncf: SyncFunc[Unit, Unit] => {
            while (throwIfFailed) {
              syncf(())
            }
            futureSuccess
          }
          case asyncf: AsyncFunc[Unit, Unit] =>
            def startLoop(): Future[Unit] = loop()

            @tailrec
            def loop(): Future[Unit] = {
              throwIfFailed()
              val fut: Future[Unit] = asyncf(())
              fut.value match {
                case Some(Success(())) => loop()
                case Some(Failure(e)) => throw e
                case None => fut flatMap (_ => startLoop())
              }
            }

            startLoop
        }
      )

      mainLoop recover {
        case e: EndOfStreamException =>
          handleComplete.someApply(())
          completionPromise.success(())
        case NonFatal(e) =>
          graph.failGraph(e)
      }
    }
  }

  class DelayedSourceMachine[Out](val input: DelayedSource[Out], val graph: GraphOps)
                                 (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] with RunnableMachine {
    override val running: RunningInput[Out] = RunningInput(completionPromise.future, input)

    override def run(): Unit = input.future map (run(_)) recover {
      case NonFatal(e) => graph.failGraph(e)
    }

    @volatile private var substream: Option[RunningStream] = None
    @volatile private var failed: Option[Throwable] = None

    /** Actually runs the source when the future completes */
    def run(source: Source[Out]): Unit = {
      val Consumer(consumerOnNext, consumerOnComplete) = next.get.consumer

      val sub = source.foreachFunc(
        consumerOnNext, consumerOnComplete
      ).build()
      substream = Some(sub)

      failed match {
        case Some(e) => sub.fail(e)
        case None =>
      }

      sub.completion recover {
        case NonFatal(e) => graph.failGraph(e)
      }

      completionPromise.completeWith(sub.completion)
    }

    override def userOnError: Func[Throwable, Unit] = (e: Throwable) => {
      failed = Some(e)
      substream match {
        case Some(sub) => sub.fail(e)
        case None =>
      }
    }
  }

  class DrivenSourceMachine[Out](val input: DrivenSource[Out], val graph: GraphOps)
                                (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] with RunnableMachine {
    override val running: RunningInput[Out] = RunningInput(completionPromise.future, input)

    @volatile private var failed: Throwable = null

    override def run(): Unit = {
      // This method doesn't actually keep running, since the input is driven by the user.

      val Consumer(consumerOnNext, consumerOnComplete) = next.get.consumer

      def passUnlessFailed[T] = new SyncFunc[T, T] {
        override def apply(t: T): T = {
          val f = failed
          if (f != null) throw f
          else t
        }
      }

      val impl = new StreamConsumerBase[Out, Unit] {
        override def onNext: Func[Out, Unit] = passUnlessFailed[Out] ~> consumerOnNext

        override def onComplete: Func[Unit, Unit] = passUnlessFailed[Unit] ~> consumerOnComplete ~> Func(completionPromise.trySuccess(())) ~> Func.nop

        override def onError: Func[Throwable, Unit] = Func(e => completionPromise.tryFailure(e))

        override def builder: FutureStreamBuilder = input.builder
      }
      input.asidePromise.success(impl)
    }

    override def userOnError: Func[Throwable, Unit] = (e: Throwable) => failed = e
  }

  class ConsumerMachine[In, Res](val output: StreamConsumer[In, Res], val graph: GraphOps)
                                (implicit val ec: ExecutionContext) extends StateMachine with ConsumerProvider[In] {
    val resultPromise = Promise[Res]()
    output.futureResultPromise.completeWith(resultPromise.future)

    completionPromise.future recover {
      case NonFatal(e) => resultPromise.tryFailure(e)
    }

    override val running: RunningOutput[In, Res] = RunningOutput(resultPromise.future)

    // Acquire copies of user functions
    val (userOnNext, userOnComplete, userOnError) = (output.onNext, output.onComplete, output.onError)

    def consumer: Consumer[In] = {
      // From the user's perspective we must guarantee no calls to onNext/onComplete after onNext fails once.
      // We rely on the previous component not calling our own onNext/onComplete if we fail the graph before returning.

      val onNext = userOnNext.composeFailure(graph.failGraph)
      val onComplete = userOnComplete.compose(SyncFunc[Res, Unit] { res =>
        resultPromise.trySuccess(res)
        completionPromise.trySuccess(())
      }).composeFailure(graph.failGraph)

      Consumer(onNext, onComplete)
    }
  }

  class DelayedSinkMachine[In, Res](val output: DelayedSink[In, Res], val graph: GraphOps)
                                   (implicit val ec: ExecutionContext) extends StateMachine with ConsumerProvider[In] {
    val resultPromise = Promise[Res]()
    output.futureResultPromise.completeWith(resultPromise.future)

    completionPromise.future recover {
      case NonFatal(e) => resultPromise.tryFailure(e)
    }

    override val running: RunningOutput[In, Res] = RunningOutput(resultPromise.future)

    private val substream = Promise[RunningStream]()
    @volatile private var failed: Option[Throwable] = None
    private val substreamOnNext = Promise[Func[In, Unit]]()
    private val substreamOnComplete = Promise[Func[Unit, Unit]]()

    output.future.map(run(_)).recover {
      case NonFatal(e) => graph.failGraph(e)
    }

    /** Actually connect the Sink once the future completes */
    private def run(sink: Sink[In, Res]): Unit = {
      val driven = Source.driven[In]()(sink.builder)

      driven.aside map { consumer =>
        substreamOnNext.success(consumer.onNext)
        substreamOnComplete.success(consumer.onComplete)
      }

      val sub = driven.to(sink).build()
      substream.success(sub)

      sub.completion recover {
        case NonFatal(e) => graph.failGraph(e)
      }

      resultPromise.completeWith(sub.apply[Nothing, Res](sink.output).result)
      resultPromise.future map (_ => completionPromise.trySuccess(()))

      failed match {
        case Some(e) => sub.fail(e)
        case None =>
      }
    }

    def consumer: Consumer[In] = {
      val onNext = new AsyncFunc[In, Unit] {
        override def apply(in: In)(implicit ec: ExecutionContext): Future[Unit] = {
          // Privilege performance of already-completed case
          substreamOnNext.future.value match {
            case Some(Success(onNext: SyncFunc[In, Unit])) => FastFuture.successful(onNext(in))
            case Some(Success(onNext: AsyncFunc[In, Unit])) => onNext(in)
            case Some(Failure(e)) => throw e
            case None => substreamOnNext.future flatMap (_ => apply(in))
          }
        }
      }

      val onComplete = new AsyncFunc[Unit, Unit] {
        override def apply(in: Unit)(implicit ec: ExecutionContext): Future[Unit] = {
          // Privilege performance of already-completed case
          (substreamOnComplete.future.value match {
            case Some(Success(onNext: SyncFunc[Unit, Unit])) => FastFuture.successful(onNext(in))
            case Some(Success(onNext: AsyncFunc[Unit, Unit])) => onNext(in)
            case Some(Failure(e)) => throw e
            case None => substreamOnNext.future flatMap (_ => apply(in))
          }) flatMap (_ => resultPromise.future) flatMap (_ => substream.future) flatMap (_.completion)
        }
      }

      Consumer(onNext, onComplete)
    }

    override def userOnError: Func[Throwable, Unit] = new SyncFunc[Throwable, Unit] {
      override def apply(e: Throwable): Unit = {
        failed = Some(e)
        // If the substream hasn't been constructed yet, don't wait for it; the construction function
        // will fail it
        if (substream.isCompleted) substream.future.value.get.get.fail(e)
      }
    }
  }

  class TransformMachine[In, Out](val transform: Transform[In, Out], val graph: GraphOps)
                                 (implicit val ec: ExecutionContext) extends ConsumerProvider[In] with StateMachineWithOneOutput[Out] {

    override val running: RunningTransform[In, Out] = RunningTransform(completionPromise.future, transform)

    // Copy of user function
    override val userOnError: Func[Throwable, Unit] = transform.onError

    override lazy val consumer: Consumer[In] = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Acquire copies of next component's functions
      val consumer = next.get.consumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete

      val afterCompleting = Func(completionPromise.success(())) ~> Func(())

      val (onNext, onComplete) = transform match {
        case NopTransform(builder) =>
          throw new IllegalArgumentException("NopTransform nodes should be eliminated by the stream builder")

        case sync: SyncSingleTransform[In, Out] =>
          val onNext = sync ~> consumerOnNext
          val onComplete = Func(sync.onComplete()) ~> consumerOnComplete
          (onNext, onComplete)

        case async: AsyncSingleTransform[In, Out] =>
          val onNext = async ~> consumerOnNext
          val onComplete = Func(async.onComplete()) ~> consumerOnComplete
          (onNext, onComplete)

        case SingleTransform(builder, trOnNext, trOnComplete, trOnError) =>
          val onNext = trOnNext ~> consumerOnNext
          val onComplete = trOnComplete ~> consumerOnComplete
          (onNext, onComplete)

        case sync: SyncMultiTransform[In, Out] =>
          val onNext = sync ~> Func.foreach(consumerOnNext)
          val onComplete = Func(sync.onComplete()) ~> Func.foreach(consumer.onNext) ~> consumerOnComplete
          (onNext, onComplete)

        case async: AsyncMultiTransform[In, Out] =>
          val onNext = async ~> Func.foreach(consumerOnNext)
          val onComplete = AsyncFunc.withEc((x: Unit) => (ec: ExecutionContext) => async.onComplete()(ec)) ~> Func.foreach(consumer.onNext) ~> consumerOnComplete
          (onNext, onComplete)

        case MultiTransform(builder, trOnNext, trOnComplete, trOnError) =>
          val onNext = trOnNext ~> Func.foreach(consumer.onNext)
          val onComplete = trOnComplete ~> Func.foreach(consumer.onNext) ~> consumerOnComplete
          (onNext, onComplete)

        case DelayedPipe(builder, future) =>
          throw new IllegalArgumentException("DelayedPipe is implemented by DelayedPipeMachine")
      }

      Consumer(onNext composeFailure (graph.failGraph), onComplete ~> afterCompleting composeFailure (graph.failGraph))
    }
  }

  class DelayedPipeMachine[In, Out](val transform: DelayedPipe[In, Out], val graph: GraphOps)
                                   (implicit val ec: ExecutionContext) extends ConsumerProvider[In] with StateMachineWithOneOutput[Out] {
    override val running: RunningTransform[In, Out] = RunningTransform(completionPromise.future, transform)

    @volatile private var substreamOnError: SyncFunc[Throwable, Unit] = Func.nop

    // Not a val because `substreamOnError` might be assigned later, when the substream has been materialized
    override def userOnError: Func[Throwable, Unit] = {
      substreamOnError match {
        case null => transform.onError
        case func => Func.tee(transform.onError, func)
      }
    }

    override lazy val consumer: Consumer[In] = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Acquire copies of next component's functions
      val consumer = next.get.consumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete

      val substream = new FastFuture(
        new FastFuture(transform.future) flatMap { case pipe =>
          val driver = Source.driven[In]()(pipe.builder)
          val stream = driver.to(pipe).foreachFunc(
            consumerOnNext, consumerOnComplete,
            Func((e: Throwable) => graph.failGraph(e))
          ).build()

          substreamOnError = Func((e: Throwable) => stream.fail(e))

          new FastFuture(driver.aside) map {
            case drv => (drv, stream)
          }
        }
      )

      val onNext = new AsyncFunc[In, Unit] {
        override def apply(in: In)(implicit ec: ExecutionContext): Future[Unit] = substream flatMap {
          case (drv, stream) => drv.onNext match {
            case syncf: SyncFunc[In, Unit] => FastFuture.successful(syncf(in))
            case asyncf: AsyncFunc[In, Unit] => asyncf(in)
          }
        }
      }

      val onComplete = new AsyncFunc[Unit, Unit] {
        override def apply(in: Unit)(implicit ec: ExecutionContext): Future[Unit] = substream flatMap {
          case (drv, stream) => drv.onComplete match {
            case syncf: SyncFunc[Unit, Unit] => FastFuture.successful(syncf(in))
            case asyncf: AsyncFunc[Unit, Unit] => asyncf(in)
          }
        }
      }

      Consumer(
        onNext composeFailure (graph.failGraph),
        onComplete ~> Func(completionPromise.trySuccess(())) ~> Func.nop composeFailure (graph.failGraph)
      )
    }
  }

  class NopMachine[T](val nop: NopTransform[T], val graph: GraphOps)
                     (implicit val ec: ExecutionContext) extends ConsumerProvider[T] with StateMachineWithOneOutput[T] {
    override lazy val consumer: Consumer[T] = {
      require(next.isDefined, "Graph must be fully linked before running")

      next.get.consumer
    }

    override val running: RunningTransform[T, T] = RunningTransform(completionPromise.future, nop)

    override def userOnError: Func[Throwable, Unit] = nop.onError
  }

  class MergerMachine[T](val connector: Merger[T], val graph: GraphOps)
                        (implicit val ec: ExecutionContext)
    extends ConnectorMachineWithUniformInput[T] with StateMachineWithOneOutput[T] with RunnableMachine {

    override val running: RunningConnector[T] = RunningConnector(completionPromise.future, connector)

    // We enqueue a None each time one input sees onComplete. The dequeuer, in `run`, counts the None elements
    // and emits its own onComplete when one None has been seen for each input.
    private val queue = new BoundedAsyncQueue[Option[T]](1)

    // The same consumer is used for all inputs, and is concurrent-safe.
    override lazy val consumer: Consumer[T] = {
      require(nexts(0).isDefined, "Graph must be fully linked before running")

      // Assume queue.enqueue can't fail and skip the recovery stuff
      val onNext = AsyncFunc[T, Unit](t => queue.enqueue(Some(t)))
      val onComplete = AsyncFunc[Unit, Unit](_ => queue.enqueue(None))

      Consumer(onNext, onComplete)
    }

    private val inputsTerminated = new AtomicInteger()

    val id = System.identityHashCode(this)

    override def userOnError: Func[Throwable, Unit] = Func.nop

    override def run(): Unit = {
      require(nexts(0).isDefined, s"Graph must be fully linked before running")

      // Acquire copies of user functions
      val consumer = nexts(0).get.consumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete

      val fullOnComplete = consumerOnComplete ~> Func(completionPromise.success(())) ~> Func(()) composeFailure (graph.failGraph)

      def loopStep(): Future[Unit] = new FastFuture(queue.dequeue()).flatMap(item => {
        throwIfFailed

        item match {
          case Some(t) =>
            (consumerOnNext match {
              case syncf: SyncFunc[T, Unit] =>
                syncf(t)
                futureSuccess
              case asyncf: AsyncFunc[T, Unit] =>
                asyncf(t)
            }) flatMap (_ => loopStep())
          case None =>
            val counted = inputsTerminated.incrementAndGet()
            if (counted == connector.inputCount) {
              fullOnComplete match {
                case syncf: SyncFunc[Unit, Unit] =>
                  syncf(())
                  futureSuccess

                case asyncf: AsyncFunc[Unit, Unit] =>
                  asyncf(())
              }
            }
            else {
              loopStep()
            }
        }
      }).recover({
        case NonFatal(e) =>
          graph.failGraph(e)
      })

      // Fire and forget
      loopStep()
    }
  }

  class SplitterMachine[T](val connector: Splitter[T], val graph: GraphOps)
                          (implicit val ec: ExecutionContext) extends ConnectorMachineWithUniformInput[T] {
    override def running: RunningConnector[T] = RunningConnector(completionPromise.future, connector)

    override def userOnError: Func[Throwable, Unit] = Func.nop

    override lazy val consumer: Consumer[T] = {
      val outputs = nexts.map(_.getOrElse(throw new IllegalArgumentException("Graph must be fully linked before running")))

      def chooseOutputs(indexes: BitSet): Vector[Func[T, _]] = {
        val iter = indexes.iterator
        val builder = Vector.newBuilder[Func[T, _]]
        while (iter.hasNext) builder += outputs(iter.next()).consumer.onNext
        builder.result()
      }

      val onNext: Func[T, Unit] = connector.outputChooser match {
        case syncf: SyncFunc[T, BitSet] => Func.flatten(Func((t: T) => {
          val outputs = chooseOutputs(syncf(t))
          Func.tee(outputs: _*)
        }))
        case asyncf: AsyncFunc[T, BitSet] => Func.flatten(AsyncFunc((t: T) => {
          new FastFuture(asyncf(t)).map {
            case indexes: BitSet => Func.tee(chooseOutputs(indexes): _*)
          }
        }))
      }

      val onComplete = Func.tee(outputs.map(_.consumer.onComplete): _*) ~>
        Func(completionPromise.success(())) ~> Func(()) composeFailure (graph.failGraph)

      Consumer(onNext, onComplete)
    }
  }

  class ScattererMachine[T](val connector: Scatterer[T], val graph: GraphOps)
                           (implicit val ec: ExecutionContext) extends ConnectorMachineWithUniformInput[T] {
    override def running: RunningConnector[T] = RunningConnector(completionPromise.future, connector)

    override def userOnError: Func[Throwable, Unit] = Func.nop

    override lazy val consumer: Consumer[T] = {
      val outputs = nexts.map(_.getOrElse(throw new IllegalArgumentException("Graph must be fully linked before running")))

      // Naive implementation, could probably be optimized
      // The `free` queue holds all consumers that are available at that moment

      val free = new AsyncQueue[Consumer[T]]()
      for (output <- outputs) free.enqueue(output.consumer)

      val onNext = new AsyncFunc[T, Unit] {
        private def dispatchAndRequeue(t: T, consumer: Consumer[T]): Future[Unit] = consumer.onNext match {
          case syncf: SyncFunc[T, Unit] =>
            syncf(t)
            free.enqueue(consumer)
            futureSuccess
          case asyncf: AsyncFunc[T, Unit] =>
            new FastFuture(asyncf(t)).map(_ => free.enqueue(consumer))
        }

        override def apply(t: T)(implicit ec: ExecutionContext): Future[Unit] = {
          val fut = new FastFuture(free.dequeue())
          fut.map(consumer =>
            // Deliberate `map` and not `flatMap`; fire and forget this, don't wait for it
            dispatchAndRequeue(t, consumer)
          )
        }
      }

      val onComplete = {
        // Need to wait for all onNext invocations to complete
        AsyncFunc {
          Future.sequence(List.fill(outputs.size)(free.dequeue())) map (_ => ())
        } ~>
          Func.tee[Unit](outputs.map(_.consumer.onComplete): _*) ~>
          Func(completionPromise.success(())) ~> Func(()) composeFailure (graph.failGraph)
      }

      Consumer(onNext, onComplete)
    }
  }

  class ConcatenatorMachine[T](val connector: Concatenator[T], val graph: GraphOps)
                              (implicit val ec: ExecutionContext) extends ConnectorMachine[T] with StateMachineWithOneOutput[T] {
    override def running: RunningConnector[T] = RunningConnector(completionPromise.future, connector)

    private val promises = Vector.fill(connector.inputCount)(Promise[Unit]())
    promises(0).success(())

    override lazy val consumers: IndexedSeq[Consumer[T]] = for (index <- 0 until connector.inputCount) yield {
      require(nexts(0).isDefined, "Graph must be fully linked before running")

      val Consumer(userOnNext, userOnComplete) = nexts(0).get.consumer

      val future = promises(index).future
      val nextPromise = if (index < promises.size - 1) promises(index + 1) else promises(0)

      val waitForFuture = new AsyncFunc[T, T] {
        override def apply(a: T)(implicit ec: ExecutionContext): Future[T] = new FastFuture(future).map(_ => a)
      }

      val onNext = waitForFuture ~> userOnNext
      val onComplete = AsyncFunc((_: Unit) => future) ~> Func({
        nextPromise.trySuccess(())
        ()
      })

      val onComplete2 = if (index < promises.size - 1) onComplete
      else {
        onComplete ~> userOnComplete ~> completionPromise.trySuccess(()) ~> Func.nop
      }

      Consumer(onNext, onComplete2)
    }

    override val userOnError: Func[Throwable, Unit] = (e: Throwable) => promises.foreach(_.tryFailure(e))
  }

}

