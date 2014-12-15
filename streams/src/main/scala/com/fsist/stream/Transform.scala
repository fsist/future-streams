package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import akka.http.util.FastFuture
import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.{AsyncFunc, SyncFunc, Func}
import scala.concurrent.{Future, ExecutionContext}

sealed trait Transform[-In, +Out] extends SourceBase[Out] with SinkBase[In] {
  def builder: FutureStreamBuilder
  def onError: Func[Throwable, Unit]
}

final case class SingleTransform[-In, +Out](builder: FutureStreamBuilder, onNext: Func[In, Out],
                                            onComplete: Func[Unit, Unit], onError: Func[Throwable, Unit]) extends Transform[In, Out]

final case class MultiTransform[-In, +Out](builder: FutureStreamBuilder, onNext: Func[In, Seq[Out]],
                                           onComplete: Func[Unit, Seq[Out]], onError: Func[Throwable, Unit]) extends Transform[In, Out]

object Transform {
  def map[In, Out](mapper: Func[In, Out])
                  (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Out] =
    SingleTransform(builder, mapper, Func.nop, Func.nop)

  def filter[In](filter: Func[In, Boolean])
                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): Transform[In, In] = {

    val func: Func[In, List[In]] = filter match {
      case syncf: SyncFunc[In, Boolean] =>
        new SyncFunc[In, List[In]] {
          override def apply(a: In): List[In] = if (syncf(a)) List(a) else List.empty
        }

        //SyncFunc(x => if (syncf(x)) List(x) else List.empty)
      case asyncf: AsyncFunc[In, Boolean] => new AsyncFunc[In, List[In]] {
        override def apply(a: In)(implicit ec: ExecutionContext): Future[List[In]] =
          new FastFuture(asyncf(a)) map (ok => if (ok) List(a) else List.empty)
      }
    }

    MultiTransform[In, In](builder, func, SyncFunc(_ => List.empty), Func.nop)
  }

  def skip[T](count: Long)
             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[T, T] = {
    val counter = new AtomicLong(count)

    val onNext = new SyncFunc[T, Seq[T]] {
      override def apply(a: T): Seq[T] = {
        val counted = counter.decrementAndGet()
        // Prevent eventual wraparound
        if (counted < 1000000) counter.set(0)
        if (counted <= 0) Seq(a) else Seq.empty
      }
    }

    MultiTransform(builder, onNext, Func.const(Seq.empty), Func.nop)
  }
}
