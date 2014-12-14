package com.fsist.stream.run

import java.util.concurrent.atomic.AtomicBoolean

import com.fsist.stream._
import com.fsist.stream.run.StateMachine.{TransformMachine, OutputMachine}
import com.fsist.util.concurrent.BoundedAsyncQueue
import com.fsist.util.{BugException, Func, AsyncFunc, SyncFunc}
import com.fsist.util.concurrent.FutureOps._
import com.typesafe.scalalogging.slf4j.Logging

import scala.async.Async._
import com.fsist.util.FastAsync._
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.util.{Success, Failure}
import scala.util.control.NonFatal

// TODO NOTE error handling is global for the entire graph. State machines no longer call downstream consumer.onError.
// When the graph fails, graph.failGraph calls the onError of all uesr components that have one, concurrently with one
// another, and concurrently with any ongoing calls to onNext or onComplete. However, if onComplete has finished
// successfully, onError will not be called for that component.

// failGraph fails the completionPromise of each component. Each state machine checks if the promise has been failed
// before calling onNext or onComplete; if yes, it aborts.

/** Note: implementations are mutable! */

/** A simplified StreamConsumer, without onError and without a Result. Exposed by each state machine with an input. */
private[run] case class Consumer[-In](onNext: Func[In, Unit], onComplete: Func[Unit, Unit])

private[run] sealed trait StateMachine extends Logging {
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
  def throwIfFailed: Boolean = if (!completionPromise.isCompleted) true
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

private[run] sealed trait StateMachineWithInput[In] extends StateMachine {
  def consumer: Consumer[In]
}

private[run] sealed trait ConnectorMachine[T] extends StateMachineWithInput[T] {
  def running: RunningConnector[T, T]
}

private[run] sealed trait StateMachineWithOneOutput[Out] extends StateMachine {
  type TOut = Out
  // Defined to solve some type issues, see https://groups.google.com/forum/#!topic/scala-user/aN-o7ZaNwPo
  var next: Option[StateMachineWithInput[TOut]] = None // Set to Some after construction
}

private[run] object StateMachine extends Logging {

  class InputMachine[Out](val input: StreamInput[Out], val graph: GraphOps)
                         (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] with RunnableMachine {
    override val running: RunningStreamInput[Out] = RunningStreamInput(completionPromise.future, input)

    // Acquire copies of user functions
    val producer = input.producer
    override val userOnError: Func[Throwable, Unit] = input.onError

    override def run(): Unit = {
      require(next.isDefined, "Graph must be fully linked before running")

      val Consumer(onNext, onComplete) = next.get.consumer
      val handledComplete = onComplete.recover {
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
            async {
              while (throwIfFailed) {
                fastAwait(asyncf(()))
              }
            }
        }
      )

      mainLoop recover {
        case e: NoSuchElementException =>
          handledComplete.someApply(())
          completionPromise.success(())
        case NonFatal(e) =>
          graph.failGraph(e)
      }
    }
  }

  class OutputMachine[In, Res](val output: StreamOutput[In, Res], val graph: GraphOps)
                              (implicit val ec: ExecutionContext) extends StateMachineWithInput[In] {
    val resultPromise = Promise[Res]()

    completionPromise.future recover {
      case NonFatal(e) => resultPromise.tryFailure(e)
    }

    override val running: RunningStreamOutput[In, Res] = RunningStreamOutput(resultPromise.future)

    // Acquire copies of user functions
    val (userOnNext, userOnComplete, userOnError) = {
      val target = output.consumer()
      (target.onNext, target.onComplete, target.onError)
    }

    lazy val consumer: Consumer[In] = {
      // From the user's perspective we must guarantee no calls to onNext/onComplete after onNext fails once.
      // We rely on the previous component not calling our own onNext/onComplete if we fail the graph before returning.

      val onNext = userOnNext.composeFailure(graph.failGraph)
      val onComplete = userOnComplete.compose(SyncFunc { res =>
        resultPromise.trySuccess(res)
        completionPromise.trySuccess(())
        ()
      }).composeFailure(graph.failGraph)

      Consumer(onNext, onComplete)
    }
  }

  // TODO TransformMachine and maybe some other machines too don't complete their completionPromise!

  class TransformMachine[In, Out](val transform: Transform[In, Out], val graph: GraphOps)
                                 (implicit val ec: ExecutionContext) extends StateMachineWithInput[In] with StateMachineWithOneOutput[Out] {
    val running: RunningTransform[In, Out] = RunningTransform(completionPromise.future, transform)

    // Acquire copy of user function
    override val userOnError: Func[Throwable, Unit] = transform.onError

    override lazy val consumer: Consumer[In] = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Acquire copies of next component's functions
      val consumer = next.get.consumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete

      transform match {
        case SingleTransform(builder, trOnNext, trOnComplete, trOnError) =>
          val onNext = trOnNext ~> consumerOnNext composeFailure (graph.failGraph)
          val onComplete = trOnComplete ~> consumerOnComplete ~> SyncFunc[Any, Unit](_ => ()) composeFailure (graph.failGraph)

          Consumer(onNext, onComplete)

        case MultiTransform(builder, trOnNext, trOnComplete, trOnError) =>
          val onNext =
            if (trOnNext.isSync && consumerOnNext.isSync) {
              val trOnNextSync = trOnNext.asSync
              val consumerOnNextSync = consumerOnNext.asSync
              SyncFunc((in: In) => {
                val batch = trOnNextSync(in)
                val iter = batch.iterator
                while (iter.hasNext) consumerOnNextSync(iter.next)
              })
            }
            else {
              AsyncFunc((in: In) => async {
                val batch = trOnNext.fastAwait(in)
                val iter = batch.iterator
                while (iter.hasNext) consumerOnNext.fastAwait(iter.next)
              })
            }

          val onComplete =
            (if (trOnComplete.isSync && consumerOnNext.isSync && consumerOnComplete.isSync) {
              val trOnCompleteSync = trOnComplete.asSync
              val consumerOnNextSync = consumerOnNext.asSync
              val consumerOnCompleteSync = consumerOnComplete.asSync
              SyncFunc((unit: Unit) => {
                val lastBatch = trOnCompleteSync(())
                val iter = lastBatch.iterator
                while (iter.hasNext) consumerOnNextSync(iter.next)
                consumerOnCompleteSync(())
              })
            }
            else {
              AsyncFunc((_: Unit) => async {
                val lastBatch = trOnComplete.fastAwait(())
                val iter = lastBatch.iterator
                while (iter.hasNext) consumerOnNext.fastAwait(iter.next)

                // consumerOnComplete.fastAwait(()) // Doesn't work because of some weird macro problem.
                // Should be equivalent to the two following lines.
                if (consumerOnComplete.isSync) consumerOnComplete.asSync.apply(())
                else await(consumerOnComplete.asAsync.apply(()))
              })
            }) ~> SyncFunc[Any, Unit](_ => ())

          Consumer(onNext, onComplete)
      }
    }
  }

  class MergerMachine[T](val merger: Merger[T], val graph: GraphOps)
                        (implicit val ec: ExecutionContext) extends ConnectorMachine[T] with StateMachineWithOneOutput[T] with RunnableMachine {

    import MergerMachine._

    override val running: RunningConnector[T, T] = RunningConnector(completionPromise.future, merger)

    private val queue = new BoundedAsyncQueue[Option[T]](1)

    // The same consumer is used for all inputs, and is concurrent-safe.
    override lazy val consumer: Consumer[T] = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Assume queue.enqueue can't fail and skip the recovery stuff
      val onNext = AsyncFunc[T, Unit](t => queue.enqueue(Some(t)))
      val onComplete = AsyncFunc[Unit, Unit](_ => queue.enqueue(None))

      Consumer(onNext, onComplete)
    }

    override def userOnError: Func[Throwable, Unit] = Func.nop

    override def run(): Unit = {
      // Acquire copies of user functions
      val consumer = next.get.consumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete

      def loopStep(): Future[Unit] = queue.dequeue() flatMap { item =>
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
            consumerOnComplete match {
              case syncf: SyncFunc[Unit, Unit] =>
                syncf(())
                futureSuccess
              case asyncf: AsyncFunc[Unit, Unit] =>
                asyncf(())
            }
        }
      } recover {
        case NonFatal(e) => graph.failGraph(e)
      }

      // Fire and forget
      loopStep()
    }
  }

  object MergerMachine {

    private case object EOF extends Exception

  }

}
