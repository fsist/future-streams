package com.fsist.stream

import java.util.concurrent.atomic.AtomicLong

import akka.http.util.FastFuture
import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.{AsyncFunc, SyncFunc, Func}
import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Promise, Future, ExecutionContext}

import scala.language.higherKinds

/** A transformation of an element stream. The input and output elements don't always have a 1-to-1 correspondence. */
sealed trait Transform[-In, +Out] extends SourceComponentBase[Out] with SinkComponentBase[In] {
  def onError: Func[Throwable, Unit]

  /** Irreversibly connects to the `pipe`'s input Source.
    *
    * Returns a new pipe appending `pipe` to this element.
    */
  def pipe[Next](pipe: Pipe[Out, Next]): Pipe[In, Next] = {
    connect(pipe.sink)
    Pipe(this, pipe.source)
  }

  /** Irreversibly connects to the `next` transform
    *
    * Returns a new pipe containing `this` and the `next` transform.
    */
  def pipe[Next](next: Transform[Out, Next]): Pipe[In, Next] = {
    connect(next)
    Pipe(this, next)
  }

  /** Irreversibly connects to the `sink`.
    *
    * Returns a new Sink containing `this` and the original `sink` combined.
    */
  def combine[Res](next: Sink[Out, Res]): Sink[In, Res] = {
    connect(next.sinkComponent)
    Sink(this, next.output)
  }
}

/** A transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
final case class NopTransform[T](builder: FutureStreamBuilder) extends Transform[T, T] {
  override def onError: Func[Throwable, Unit] = Func.nop
}

/** Common supertrait of the non-sealed traits the user can extend to implement a Transform. */
sealed trait UserTransform[-In, +Out] extends Transform[In, Out] with NewBuilder {
  final override def onError: Func[Throwable, Unit] = Func(th => onError(th))

  /** Called on stream failure. See the README for the semantics. */
  def onError(throwable: Throwable): Unit = ()
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
  def onComplete(): Iterable[Out] = Iterable.empty
}

/** Implement this trait (at least the onNext method) to create a new synchronous one-to-many Transform. */
trait AsyncManyTransform[-In, +Out] extends UserTransform[In, Out] with AsyncFunc[In, Iterable[Out]] {
  final override def apply(in: In)(implicit ec: ExecutionContext): Future[Iterable[Out]] = onNext(in)

  /** Map each successive stream element. See the README for detailed semantics. */
  def onNext(in: In)(implicit ec: ExecutionContext): Future[Iterable[Out]]

  /** Called when the component completes. See the README for detailed semantics. */
  def onComplete()(implicit ec: ExecutionContext): Future[Iterable[Out]] = Future.successful(Iterable.empty)
}

/** A 1-to-1 transformation of stream elements, equivalent to a `map`. */
final case class SingleTransform[-In, +Out](builder: FutureStreamBuilder, onNext: Func[In, Out],
                                            onComplete: Func[Unit, Unit], onError: Func[Throwable, Unit]) extends Transform[In, Out] {
  override def toString(): String = s"SingleTransform@${System.identityHashCode(this)}"
}

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

/** A Transform or more complex Pipe which will become available, and start operating, once `future` is fulfilled.
  *
  * If the future is fulfilled when the stream is built, it acts as an ordinary pipe.
  * Otherwise, components upstream of this transform will pause when they try to push data into it,
  * until the future is completed.
  */
final case class DelayedPipe[-In, +Out](builder: FutureStreamBuilder, future: Future[Pipe[In, Out]],
                                             onError: Func[Throwable, Unit] = Func.nop) extends Transform[In, Out]

object DelayedPipe {
  def apply[In, Out](future: Future[Pipe[In, Out]])
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): DelayedPipe[In, Out] =
    apply(builder, future)
}

object Transform {
  /** A transformation that does nothing. When this is present in a stream, the materialization phase eliminates it. */
  def nop[T]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[T, T] = NopTransform[T](builder)

  def map[In, Out](mapper: Func[In, Out])
                  (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Out] =
    SingleTransform(builder, mapper, Func.nop, Func.nop)

  def flatMap[In, Out](mapper: Func[In, Iterable[Out]], onComplete: Func[Unit, Iterable[Out]] = Iterable.empty[Out])
                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Out] =
    MultiTransform(builder, mapper, onComplete, Func.nop)

  /** The stream will wait for `future` to be completed, and then will materialize and run the provided Pipe.
    *
    * Do not confuse with `flatten`, which transforms a stream of Iterable[T] to a stream of T.
    */
  def flattenPipe[In, Out](future: Future[Pipe[In, Out]],
                           onError: Func[Throwable, Unit] = Func.nop)
                          (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): DelayedPipe[In, Out] =
    DelayedPipe(builder, future, onError)

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

  def foldLeft[In, Res](init: Res)(onNext: Func[(Res, In), Res])
                       (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, Res] = {

    var state = init
    val empty = Iterable.empty[Res]
    val func: Func[In, Iterable[Res]] = onNext match {
      case syncf: SyncFunc[(Res, In), Res] =>
        (in: In) => {
          state = syncf((state, in))
          empty
        }
      case asyncf: AsyncFunc[(Res, In), Res] =>
        new AsyncFunc[In, Iterable[Res]] {
          override def apply(in: In)(implicit ec: ExecutionContext): Future[Iterable[Res]] = asyncf((state, in)) map {
            case newState =>
              state = newState
              empty
          }
        }
    }

    flatMap[In, Res](func, Seq(state))
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
    flatMap[Coll[Elem], Coll[Elem]]((input: Coll[Elem]) => {
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
    flatMap[Coll[Elem], Coll[Elem]]((input: Coll[Elem]) => {
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

  /** Collects all input elements in a collection of type `M` and emits it when the stream completes. */
  def collect[In, M[_]]()(implicit cbf: CanBuildFrom[Nothing, In, M[In]],
                          builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[In, M[In]] = {
    def b = builder
    new SyncManyTransform[In, M[In]] {
      override def builder: FutureStreamBuilder = b

      private val m = cbf.apply()
      private val empty = Iterable.empty[M[In]]

      override def onNext(in: In): Iterable[M[In]] = {
        m += in
        empty
      }

      override def onComplete(): Iterable[M[In]] = {
        val result = m.result()
        m.clear()
        Iterable(result)
      }
    }
  }

  /** Concatenates all input data into one large collection of the same type. When the input terminates, emit the collection
    * as a single element downstream.
    *
    * This works both if the input type Coll is generic (Coll[Elem]), like the standard scala collections, and if it
    * isn't, like akka.util.ByteString.
    */
  def concat[Elem, Coll]()(implicit ev: Coll <:< TraversableOnce[Elem],
                           cbf: CanBuildFrom[Nothing, Elem, Coll],
                           builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[Coll, Coll] = {
    def b = builder
    new SyncManyTransform[Coll, Coll] {
      override def builder: FutureStreamBuilder = b

      private val m = cbf.apply()

      override def onNext(in: Coll): Iterable[Coll] = {
        m ++= in
        Iterable.empty
      }

      override def onComplete(): Iterable[Coll] = Iterable(m.result())
    }
  }

  /** Passes on the head of the stream and discards the rest. If the stream is empty, fails with NoSuchElementException. */
  def head[Elem]()(implicit b: FutureStreamBuilder): Transform[Elem, Elem] = new SyncManyTransform[Elem, Elem] {
    override def builder: FutureStreamBuilder = b

    private var passed = false

    override def onNext(in: Elem): Iterable[Elem] = {
      if (passed) Iterable.empty
      else {
        passed = true
        Iterable(in)
      }
    }

    override def onComplete(): Iterable[Elem] =
      if (passed) Iterable.empty
      else throw new NoSuchElementException("stream was empty")

    override def toString(): String = "head"
  }

  /** Passes on the head of the stream wrapped in a `Some` and discards the rest, or passes `None` if the stream is empty. */
  def headOption[Elem]()(implicit b: FutureStreamBuilder): Transform[Elem, Option[Elem]] = new SyncManyTransform[Elem, Option[Elem]] {
    override def builder: FutureStreamBuilder = b

    private var passed = false

    override def onNext(in: Elem): Iterable[Option[Elem]] = {
      if (passed) Iterable.empty
      else {
        passed = true
        Iterable(Some(in))
      }
    }

    override def onComplete(): Iterable[Option[Elem]] =
      if (passed) Iterable.empty
      else Iterable(None)
  }

  /** Mixed into some transforms, like `tapHead`, that complete a result of type T aside from the main stream
    * and potentially before the stream completes. */
  trait Aside[+T] {
    def aside: Future[T]
  }

  /** A pass-through transform that exposes the first element passed via a Future. */
  def tapHead[Elem]()(implicit b: FutureStreamBuilder = new FutureStreamBuilder): Transform[Elem, Elem] with Aside[Option[Elem]] =
    new SyncSingleTransform[Elem, Elem] with Aside[Option[Elem]] {
      override def builder: FutureStreamBuilder = b

      private val promise = Promise[Option[Elem]]()

      override def aside: Future[Option[Elem]] = promise.future

      override def onNext(in: Elem): Elem = {
        if (!promise.isCompleted) promise.success(Some(in))
        in
      }

      override def onComplete(): Unit = promise.trySuccess(None)
    }

  /** Append the given elements to those in the stream. */
  def append[Elem](elems: Iterable[Elem])
                  (implicit b: FutureStreamBuilder = new FutureStreamBuilder): Transform[Elem, Elem] =
    flatMap[Elem, Elem](
      (e: Elem) => Seq(e),
      elems
    )

  /** Prepend the given elements to those in the stream. */
  def prepend[Elem](elems: Iterable[Elem])
                   (implicit b: FutureStreamBuilder = new FutureStreamBuilder): Transform[Elem, Elem] = new SyncManyTransform[Elem, Elem] {
    private var emitted = false

    override def onNext(in: Elem): Iterable[Elem] = {
      if (emitted) Seq(in)
      else {
        emitted = true
        elems ++ Seq(in)
      }
    }

    override implicit def builder: FutureStreamBuilder = b
  }
}

