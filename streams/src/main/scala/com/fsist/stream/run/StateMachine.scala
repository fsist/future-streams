package com.fsist.stream.run

import com.fsist.stream._
import com.fsist.stream.run.StateMachine.{TransformMachine, OutputMachine}
import com.fsist.util.{BugException, Func, AsyncFunc, SyncFunc}
import com.fsist.util.concurrent.FutureOps._
import com.typesafe.scalalogging.slf4j.Logging

import scala.async.Async._
import com.fsist.util.FastAsync._
import scala.concurrent.{Future, Promise, ExecutionContext}
import scala.util.control.NonFatal

// TODO make sure error handling is consistent in all StateMachine types, and write out its semantics

/** Note: implementations are mutable! */
private[run] sealed trait StateMachine {
  implicit def ec: ExecutionContext

  def running: RunningStreamComponent
}

private[run] sealed trait StateMachineWithOneOutput[Out] extends StateMachine {
  var next: Option[StateMachine] = None // Set to Some after construction

  def nextConsumer: SafeConsumer[Out, _] = next.get match {
    case output: OutputMachine[Out, _] => output.consumer
    case transform: TransformMachine[Out, _] => transform.consumer
    case other => ???
  }
}

private[run] object StateMachine extends Logging {

  class InputMachine[Out](val input: StreamInput[Out])
                         (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] {
    val completionPromise = Promise[Unit]()
    val running: RunningStreamInput[Out] = RunningStreamInput(completionPromise.future, input)

    def run(): Unit = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Acquire copies of user functions
      val producer = input.producer
      val consumer = nextConsumer
      val onNext = consumer.onNext
      val onComplete = consumer.onComplete
      val onError = consumer.onError

      val mainAction = producer.compose(onNext)

      // Avoid async issue #93
      def mainLoop: Future[Unit] = exceptionToFailure(
        mainAction match {
          case syncf: SyncFunc[Unit, Unit] => {
            while (true) syncf(())
            futureSuccess
          }
          case asyncf: AsyncFunc[Unit, Unit] =>
            async {
              while (true) fastAwait(asyncf(()))
            }
        }
      )

      // Fire and forget
      mainLoop recover {
        // Fire and forget in both cases
        case e: NoSuchElementException =>
          onComplete.someApply(())
          completionPromise.success(())
        case DownstreamHandledException(Some(e)) =>
          completionPromise.failure(e)
        case e@DownstreamHandledException(None) =>
          completionPromise.failure(e)
        case NonFatal(e) =>
          // Must have been in the producer, since the consumer is safe
          onError.someApply(e)
          completionPromise.failure(e)
      }
    }
  }

  class OutputMachine[In, Res](val output: StreamOutput[In, Res])
                              (implicit val ec: ExecutionContext) extends StateMachine {
    val resultPromise = Promise[Res]()
    val running: RunningStreamOutput[In, Res] = RunningStreamOutput(resultPromise.future)

    lazy val consumer: SafeConsumer[In, Res] = {
      val target = output.consumer()
      // Acquire copies of user functions
      val (targetOnNext, targetOnComplete, targetOnError) = (target.onNext, target.onComplete, target.onError)

      val onNext = targetOnNext.recover {
        case NonFatal(e) =>
          resultPromise.tryFailure(e)
          throw DownstreamHandledException(Some(e))
      }

      val onComplete = targetOnComplete.compose(SyncFunc { res => resultPromise.trySuccess(res); res}).recover {
        case NonFatal(e) =>
          resultPromise.tryFailure(e)
          throw DownstreamHandledException(Some(e))
      }

      val onError = Func.tee[Throwable](
        targetOnError.suppressErrors(),
        SyncFunc(err => resultPromise.tryFailure(err))
      )

      SafeConsumer(onNext, onComplete, onError)
    }
  }

  class TransformMachine[In, Out](val transform: Transform[In, Out])
                                 (implicit val ec: ExecutionContext) extends StateMachineWithOneOutput[Out] {
    val completionPromise = Promise[Unit]()
    val running: RunningTransform[In, Out] = RunningTransform(completionPromise.future, transform)

    lazy val consumer: SafeConsumer[In, _] = {
      require(next.isDefined, "Graph must be fully linked before running")

      // Acquire copies of user functions
      val consumer = nextConsumer
      val consumerOnNext = consumer.onNext
      val consumerOnComplete = consumer.onComplete
      val consumerOnError = consumer.onError

      transform match {
        case SingleTransform(_, trOnNext, trOnComplete, trOnError) =>
          val onError = Func.tee(trOnError.suppressErrors(), consumerOnError.suppressErrors())
          val onComplete = trOnComplete.compose(consumerOnComplete).someRecover(onError)
          val onNext = trOnNext.compose(consumerOnNext).someRecover(Func.tee(onError, SyncFunc(t => throw new DownstreamHandledException(Some(t)))))

          SafeConsumer(onNext, onComplete, onError)

        case MultiTransform(_, trOnNext, trOnComplete, trOnError) =>
          val onError = Func.tee(trOnError.suppressErrors(), consumerOnError.suppressErrors())

          val onNext =
            (if (trOnNext.isSync && consumerOnNext.isSync) {
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
            }).someRecover(onError)

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
                consumerOnComplete.fastAwait(())
              })
            }).someRecover(onError)

          SafeConsumer(onNext, onComplete, onError)
      }
    }
  }

  class ConnectorMachine[In, Out](val connector: Connector[In, Out])
                                 (implicit val ec: ExecutionContext) extends StateMachine {
    val completionPromise = Promise[Unit]()
    val running: RunningConnector[In, Out] = RunningConnector(completionPromise.future, connector)
  }

}

/** Guarantees that onNext will only fail with a DownstreamFailedException, and onComplete and onError will never fail at all.
  *
  * Consequently, upstream doesn't ever have to handle errors from onComplete or onError, and it doesn't have to pass
  * errors thrown by onNext back to this consumer's onError.
  */
case class SafeConsumer[-In, +Res](onNext: Func[In, Unit], onComplete: Func[Unit, Res], onError: Func[Throwable, Unit]) extends StreamConsumer[In, Res]

/** When a StreamConsumer method throws this, it indicates the consumer already handled the exception (i.e. passed it
  * downstream and failed its completion promise) and upstream should not pass this error again to its onError. */
case class DownstreamHandledException(inner: Option[Throwable]) extends Throwable
