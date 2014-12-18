package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import akka.http.util.FastFuture
import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.{AsyncFunc, SyncFunc, Func}
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Future, ExecutionContext}

import scala.language.higherKinds

/** A transformation of an element stream. The input and output elements don't always have a 1-to-1 correspondence. */
sealed trait Transform[-In, +Out] extends SourceBase[Out] with SinkBase[In] {
  def builder: FutureStreamBuilder

  def onError: Func[Throwable, Unit]

  /** Irreversibly connects to the `pipe`'s input Source.
    *
    * Returns a new pipe appending `pipe` to this element.
    */
  def pipe[Next](pipe: Pipe[Out, Next]): Pipe[In, Next] = {
    connect(pipe.sink)
    Pipe(builder, this, pipe.source)
  }

  /** Irreversibly connects to this `next` transform
    *
    * Returns a new pipe containing `this` and the `next` transform.
    */
  def pipe[Next](next: Transform[Out, Next]): Pipe[In, Next] = {
    connect(next)
    Pipe(builder, this, next)
  }
}

/** A transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
final case class NopTransform[T](builder: FutureStreamBuilder) extends Transform[T, T] {
  override def onError: Func[Throwable, Unit] = Func.nop
}

/** Common supertrait of the non-sealed traits the user can extend to implement a Transform. */
sealed trait UserTransform[-In, +Out] extends Transform[In, Out] {
  override def builder: FutureStreamBuilder = new FutureStreamBuilder

  final override def onError: Func[Throwable, Unit] = Func(th => onError(th))

  /** Called on stream failure. See the README for the semantics. */
  def onError(throwable: Throwable) : Unit = ()
}

/** Implement this trait (at least the onNext method) to create a new synchronous one-to-one Transform. */
trait SyncSingleTransform[-In, +Out] extends UserTransform[In, Out] with SyncFunc[In, Out] {
  final override def apply(in: In): Out = onNext(in)

  /** Map each successive stream element. See the README for detailed semantics. */
  def onNext(in: In): Out

  /** Called when the component completes. See the README for detailed semantics. */
  def onComplete(): Unit = ()
}

/** Implement this trait (at least the onNext method) to create a new asynchronous one-to-one Transform. */
trait AsyncSingleTransform[-In, +Out] extends UserTransform[In, Out] with AsyncFunc[In, Out] {
  final override def apply(in: In)(implicit ec: ExecutionContext): Future[Out] = onNext(in)

  /** Map each successive stream element. See the README for detailed semantics. */
  def onNext(in: In)(implicit ec: ExecutionContext): Future[Out]

  /** Called when the component completes. See the README for detailed semantics. */
  def onComplete(): Unit = ()
}

/** Implement this trait (at least the onNext method) to create a new synchronous one-to-one Transform. */
trait SyncManyTransform[-In, +Out] extends UserTransform[In, Out] with SyncFunc[In, Iterable[Out]] {
  final override def apply(in: In): Iterable[Out] = onNext(in)

  /** Map each successive stream element. See the README for detailed semantics. */
  def onNext(in: In): Iterable[Out]

  /** Called when the component completes. See the README for detailed semantics. */
  def onComplete(): Unit = ()
}

/** Implement this trait (at least the onNext method) to create a new synchronous one-to-many Transform. */
trait AsyncManyTransform[-In, +Out] extends UserTransform[In, Out] with AsyncFunc[In, Iterable[Out]] {
  final override def apply(in: In)(implicit ec: ExecutionContext): Future[Iterable[Out]] = onNext(in)

  /** Map each successive stream element. See the README for detailed semantics. */
  def onNext(in: In)(implicit ec: ExecutionContext): Future[Iterable[Out]]

  /** Called when the component completes. See the README for detailed semantics. */
  def onComplete(): Unit = ()
}

/** A 1-to-1 transformation of stream elements, equivalent to a `map`. */
final case class SingleTransform[-In, +Out](builder: FutureStreamBuilder, onNext: Func[In, Out],
                                            onComplete: Func[Unit, Unit], onError: Func[Throwable, Unit]) extends Transform[In, Out]

object SingleTransform {
  def apply[In, Out](onNext: Func[In, Out],
                     onComplete: Func[Unit, Unit] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): SingleTransform[In, Out] =
    apply(builder, onNext, onComplete, onError)
}

/** A 1-to-many transformation of stream elements, equivalent to a `flatMap`. */
final case class MultiTransform[-In, +Out](builder: FutureStreamBuilder, onNext: Func[In, Iterable[Out]],
                                           onComplete: Func[Unit, Iterable[Out]], onError: Func[Throwable, Unit]) extends Transform[In, Out]

object MultiTransform {
  def apply[In, Out](onNext: Func[In, Iterable[Out]],
                     onComplete: Func[Unit, Iterable[Out]] = Iterable.empty[Out], onError: Func[Throwable, Unit] = Func.nop)
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): MultiTransform[In, Out] =
    apply(builder, onNext, onComplete, onError)
}

object Transform {
  /** A transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
  def nop[T]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[T, T] = NopTransform[T](builder)

  def map[In, Out](mapper: Func[In, Out])
                  (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Out] =
    SingleTransform(builder, mapper, Func.nop, Func.nop)

  def flatMap[In, Out](mapper: Func[In, Iterable[Out]])
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Out] =
    MultiTransform(builder, mapper, Iterable.empty[Out], Func.nop)

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

    MultiTransform(func, Func(List.empty))(builder)
  }

  /** WARNING: this implementation discards all input after `count` elements have been taken, but it doesn't prevent
    * the upstream component from producing them, which may be expensive. */
  def take[T](count: Long)
             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[T, T] = {
    var counter: Long = count

    val onNext = new SyncFunc[T, Seq[T]] {
      override def apply(a: T): Seq[T] = {
        counter -= 1

        // Prevent eventual wraparound
        if (counter < -100000000000L) counter = -1

        if (counter < 0) Seq.empty else Seq(a)
      }
    }

    MultiTransform(builder, onNext, Func(Seq.empty), Func.nop)
  }

  def drop[T](count: Long)
             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[T, T] = {
    var counter: Long = count

    val onNext = new SyncFunc[T, Seq[T]] {
      override def apply(a: T): Seq[T] = {
        counter -= 1

        // Prevent eventual wraparound
        if (counter < -100000000000L) counter = -1

        if (counter < 0) Seq(a) else Seq.empty
      }
    }

    MultiTransform(builder, onNext, Func(Seq.empty), Func.nop)
  }

  /** Transforms a stream of iterable sequences into a stream of their elements. */
  def flatten[Elem, M[Elem] <: Iterable[Elem]]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[M[Elem], Elem] =
    flatMap[M[Elem], Elem](Func.pass)

  /** Transforms a stream of sequences by emitting sequences containing no more than `count` elements.
    *
    * The emitted sequences are the same as the original ones, except for the last one, which is possibly truncated.
    */
  def takeElements[Elem, Coll[Elem] <: Traversable[Elem]](count: Long)
                                                         (implicit cbf: CanBuildFrom[Nothing, Elem, Coll[Elem]],
                                                          builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[Coll[Elem], Coll[Elem]] = {
    var remaining: Long = count
    flatMap((input: Coll[Elem]) => {
      if (remaining <= 0) List.empty
      else {
        val size = input.size
        if (size <= remaining) {
          remaining -= size
          List(input)
        }
        else {
          val ret = input.take(remaining.toInt)
          remaining = 0
          List(ret.to[Coll])
        }
      }
    })
  }

  /** Transforms a stream of sequences by dropping leading sequences containing `count` elements.
    *
    * The emitted sequences are a suffix of the original ones, except for the first one, which is itself a suffix of
    * some original sequence.
    */
  def dropElements[Elem, Coll[Elem] <: Traversable[Elem]](count: Long)
                                                         (implicit cbf: CanBuildFrom[Nothing, Elem, Coll[Elem]],
                                                          builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[Coll[Elem], Coll[Elem]] = {
    var remaining: Long = count
    flatMap((input: Coll[Elem]) => {
      if (remaining <= 0) List(input)
      else {
        val size = input.size
        if (size <= remaining) {
          remaining -= size
          List.empty
        }
        else {
          val ret = input.drop(remaining.toInt)
          remaining = 0
          List(ret.to[Coll])
        }
      }
    })
  }
}
