package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.{AsyncFunc, SyncFunc, Func}
import scala.async.Async._
import com.fsist.util.FastAsync._
import scala.concurrent.ExecutionContext

sealed trait Transform[-In, +Out] extends SourceBase[Out] with SinkBase[In]

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
      case syncf: SyncFunc[In, Boolean] => SyncFunc(x => if (syncf(x)) List(x) else List.empty)
      case asyncf: AsyncFunc[In, Boolean] => AsyncFunc(x => async {
        if (fastAwait(asyncf(x))) List(x) else List.empty
      })
    }
    MultiTransform[In, In](builder, func, SyncFunc(_ => List.empty), Func.nop)
  }
}
