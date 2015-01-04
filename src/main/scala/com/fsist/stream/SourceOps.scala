package com.fsist.stream

import java.util.concurrent.atomic.AtomicReference

import akka.http.util.FastFuture
import com.fsist.stream.Transform.Aside
import com.fsist.util.concurrent.{AsyncFunc, Func}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.TraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.BitSet
import scala.concurrent.{Promise, ExecutionContext, Future}

import scala.language.higherKinds

/** Mixed into `Source` implementations to add shortcut methods to constructors of Source, Transform, Sink and Connect.
  *
  * All methods here have three variants:
  * - One taking function literals A => B
  * - Another called xxxAsync taking function literals A => Future[B]
  * - And a third called xxxFunc taking Func objects.
  *
  * Although they have different signatures, making them into overloads would remove the ability to call the synchronous
  * variant (the most common case) with function literals like `source.map(_ + 1)`.
  */
trait SourceOps[+Out] {

  protected def sourceComponent: SourceComponent[Out]

  private implicit def myBuilder = sourceComponent.builder

  // ===================================================================================================================
  // Transform
  // ===================================================================================================================

  // Transform.map

  def map[Next](mapper: Out => Next): SourceComponent[Next] =
    sourceComponent.transform(Transform.map(mapper))

  def map[Next](mapper: Out => Next, onComplete: => Unit): SourceComponent[Next] =
    sourceComponent.transform(Transform.map(mapper, onComplete))

  def mapAsync[Next](mapper: Out => Future[Next]): SourceComponent[Next] =
    sourceComponent.transform(Transform.map(AsyncFunc(mapper)))

  def mapAsync[Next](mapper: Out => Future[Next], onComplete: => Future[Unit]): SourceComponent[Next] =
    sourceComponent.transform(Transform.map(AsyncFunc(mapper), AsyncFunc(onComplete)))

  def mapFunc[Next](mapper: Func[Out, Next], onComplete: Func[Unit, Unit] = Func.nop): SourceComponent[Next] =
    sourceComponent.transform(Transform.map(mapper, onComplete))

  // Transform.flatMap

  def flatMap[Next](mapper: Out => Iterable[Next]): SourceComponent[Next] =
    sourceComponent.transform(Transform.flatMap(mapper))

  def flatMap[Next](mapper: Out => Iterable[Next], onComplete: => Iterable[Next]): SourceComponent[Next] =
    sourceComponent.transform(Transform.flatMap(mapper, onComplete))

  def flatMapAsync[Next](mapper: Out => Future[Iterable[Next]]): SourceComponent[Next] =
    sourceComponent.transform(Transform.flatMap(AsyncFunc(mapper)))

  def flatMapAsync[Next](mapper: Out => Future[Iterable[Next]],
                         onComplete: => Future[Iterable[Next]]): SourceComponent[Next] =
    sourceComponent.transform(Transform.flatMap(AsyncFunc(mapper), AsyncFunc(onComplete)))

  def flatMapFunc[Next](mapper: Func[Out, Iterable[Next]],
                        onComplete: Func[Unit, Iterable[Next]] = Func(emptyIterable)): SourceComponent[Next] =
    sourceComponent.transform(Transform.flatMap(mapper, onComplete))

  // Transform.filter

  def filter(filter: Out => Boolean)
            (implicit ec: ExecutionContext): SourceComponent[Out] =
    sourceComponent.transform(Transform.filter(filter))

  def filterAsync(filter: Out => Future[Boolean])
                 (implicit ec: ExecutionContext): SourceComponent[Out] =
    sourceComponent.transform(Transform.filter(AsyncFunc(filter)))

  def filterFunc(filter: Func[Out, Boolean])
                (implicit ec: ExecutionContext): SourceComponent[Out] =
    sourceComponent.transform(Transform.filter(filter))

  // Transform.fold

  def fold[Super >: Out, Res](init: Res)
                             (onNext: (Res, Super) => Res): SourceComponent[Res] = {
    val tr = Transform.fold[Super, Res](init)(onNext.tupled)
    sourceComponent.transform(tr)
  }

  def foldAsync[Super >: Out, Res](init: Res)
                                  (onNext: ((Res, Super)) => Future[Res]): SourceComponent[Res] = {
    val tr = Transform.fold(init)(AsyncFunc(onNext))
    sourceComponent.transform(tr)
  }

  def foldFunc[Super >: Out, Res](init: Res)
                                 (onNext: Func[(Res, Super), Res]): SourceComponent[Res] = {
    val tr = Transform.fold[Super, Res](init)(onNext)
    sourceComponent.transform(tr)
  }

  // Transform.take

  def take(count: Long): SourceComponent[Out] =
    sourceComponent.transform(Transform.take(count))

  // Transform.drop

  def drop(count: Long): SourceComponent[Out] =
    sourceComponent.transform(Transform.drop(count))

  // Transform.flatten

  def flatten[Elem]()(implicit ev: Out <:< Iterable[Elem]): SourceComponent[Elem] = {
    sourceComponent.transform(Transform.flatten[Elem, Iterable]().asInstanceOf[Transform[Out, Elem]])
  }

  // Transform.takeElements

  def takeElements[Elem](count: Long)
                        (implicit ev: Out <:< Traversable[Elem],
                         cbf: CanBuildFrom[Nothing, Elem, Out@uncheckedVariance]): Transform[_ <: Out, Out] = {
    val tr = Transform.takeElements(count)(ev, cbf, sourceComponent.builder)
    sourceComponent.transform(tr)
  }

  // Transform.dropElements

  def dropElements[Elem](count: Long)
                        (implicit ev: Out <:< Traversable[Elem],
                         cbf: CanBuildFrom[Nothing, Elem, Out@uncheckedVariance]): Transform[_ <: Out, Out] = {
    val tr = Transform.dropElements(count)(ev, cbf, sourceComponent.builder)
    sourceComponent.transform(tr)
  }

  // Transform.collect

  // For the legality of the use of @uncheckedVariance, compare TraversableOnce.To[M]
  def collect[M[_]]()(implicit cbf: CanBuildFrom[Nothing, Out, M[Out@uncheckedVariance]]): Transform[_ <: Out, M[Out@uncheckedVariance]] = {
    val tr = Transform.collect()(cbf, sourceComponent.builder)
    sourceComponent.transform(tr)
  }

  /** This overload of `collect` lets you specify an explicit supertype bound of `Out` (so you cannot upcast past it)
    * and in exchange get a precise non-existential return type. */
  def collectSuper[Super >: Out, M[_]]()(implicit cbf: CanBuildFrom[Nothing, Super, M[Super]]): Transform[Super, M[Super]] = {
    val tr = Transform.collect()(cbf, sourceComponent.builder)
    sourceComponent.transform(tr)
  }

  // Shortcuts for Transform.collect

  def toList(): Transform[_ <: Out, List[Out]] = collect[List]()

  def toSeq(): Transform[_ <: Out, Seq[Out]] = collect[Seq]()

  def toIndexedSeq(): Transform[_ <: Out, IndexedSeq[Out]] = collect[IndexedSeq]()

  def toVector(): Transform[_ <: Out, Vector[Out]] = collect[Vector]()

  def toSet[Super >: Out](): Transform[_ <: Out, Set[Super]] = collectSuper[Super, Set]()

  // Transform.concat

  def concat[Elem, Super >: Out]()(implicit ev: Super <:< TraversableOnce[Elem],
                                   cbf: CanBuildFrom[Nothing, Elem, Super]): Transform[_ <: Out, Super] = {
    val tr = Transform.concat[Elem, Super]() //(ev, cbf, sourceComponent.builder)
    sourceComponent.transform(tr.asInstanceOf[Transform[Out, Super]])
  }

  // Transform.head

  def head(): Transform[_ <: Out, Out] = {
    val tr = Transform.head[Out]()
    sourceComponent.transform(tr)
  }

  // Transform.headOption

  def headOption(): Transform[_ <: Out, Option[Out]] = {
    val tr = Transform.headOption[Out]()
    sourceComponent.transform(tr)
  }

  // Transform.tapHead

  def tapHead(): Transform[_ <: Out, Out] with Aside[Option[Out]] = {
    val tr = Transform.tapHead[Out]()
    sourceComponent.transform(tr)
  }

  // Transform.append

  def append[Super >: Out](elems: Iterable[Super]): Transform[_ <: Out, Super] = {
    val tr = Transform.append[Super](elems)
    sourceComponent.transform(tr)
  }

  def appendThese[Super >: Out](elems: Super*): Transform[_ <: Out, Super] = {
    val tr = Transform.append[Super](elems)
    sourceComponent.transform(tr)
  }

  // Transform.prepend

  def prepend[Super >: Out](elems: Iterable[Super]): Transform[_ <: Out, Super] = {
    val tr = Transform.prepend[Super](elems)
    sourceComponent.transform(tr)
  }

  def prependThese[Super >: Out](elems: Super*): Transform[_ <: Out, Super] = {
    val tr = Transform.prepend[Super](elems)
    sourceComponent.transform(tr)
  }

  // Transform.onError

  def onError(onError: Throwable => Unit): Transform[_ <: Out, Out] = {
    val tr = Transform.onError[Out](onError)
    sourceComponent.transform(tr)
  }

  def onErrorAsync(onError: Throwable => Future[Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.onError[Out](AsyncFunc(onError))
    sourceComponent.transform(tr)
  }

  def onErrorFunc(onError: Func[Throwable, Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.onError[Out](onError)
    sourceComponent.transform(tr)
  }

  // Transform.onComplete

  def onComplete(onComplete: => Unit): Transform[_ <: Out, Out] = {
    val tr = Transform.onComplete[Out](onComplete)
    sourceComponent.transform(tr)
  }

  def onCompleteAsync(onComplete: => Future[Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.onComplete[Out](AsyncFunc(onComplete))
    sourceComponent.transform(tr)
  }

  def onCompleteFunc(onComplete: Func[Unit, Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.onComplete[Out](onComplete)
    sourceComponent.transform(tr)
  }

  // Transform.foreach

  def foreachTr(func: Out => Unit): Transform[_ <: Out, Out] = {
    val tr = Transform.foreach[Out](func)
    sourceComponent.transform(tr)
  }

  def foreachTrAsync(func: Out => Future[Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.foreach[Out](AsyncFunc(func))
    sourceComponent.transform(tr)
  }

  def foreachTrFunc(func: Func[Out, Unit]): Transform[_ <: Out, Out] = {
    val tr = Transform.foreach[Out](func)
    sourceComponent.transform(tr)
  }

  // ===================================================================================================================
  // Sink
  // ===================================================================================================================

  // Sink.foreach

  def foreach[Super >: Out](func: Super => Unit): StreamOutput[Super, Unit] = {
    val output = Sink.foreach[Super, Unit](func)
    sourceComponent.connect(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit,
                                 onComplete: => Res): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete)
    sourceComponent.connect(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit,
                                 onComplete: => Res,
                                 onError: Throwable => Unit): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete, onError)
    sourceComponent.connect(output)
  }

  def foreachAsync[Super >: Out](func: Super => Future[Unit]): StreamOutput[Super, Unit] = {
    val output = Sink.foreachAsync(func)
    sourceComponent.to(output)
  }

  def foreachAsync[Super >: Out, Res](func: Super => Future[Unit],
                                      onComplete: => Future[Res]): StreamOutput[Super, Res] = {
    val output = Sink.foreachAsync(func, onComplete)
    sourceComponent.to(output)
  }

  def foreachAsync[Super >: Out, Res](func: Super => Future[Unit],
                                      onComplete: => Future[Res],
                                      onError: Throwable => Unit): StreamOutput[Super, Res] = {
    val output = Sink.foreachAsync(func, onComplete, onError)
    sourceComponent.to(output)
  }

  def foreachFunc[Super >: Out, Res](func: Func[Super, Unit],
                                     onComplete: Func[Unit, Res] = Func.nop,
                                     onError: Func[Throwable, Unit] = Func.nop): StreamOutput[Super, Res] = {
    val output = Sink.foreachFunc(func, onComplete, onError)
    sourceComponent.to(output)
  }

  // Sink.discard

  def discard(): StreamOutput[_ <: Out, Unit] = {
    val output = Sink.discard[Out]
    sourceComponent.to(output)
  }

  // Sink.single

  def single(): StreamOutput[_, Out] = {
    val sink = Sink.single[Out]()
    sourceComponent.to(sink)
  }

  // Sink.drive

  def drive[Res](consumer: StreamConsumer[Out, Res]): StreamOutput[_ <: Out, Res] = {
    val sink = Sink.drive(consumer)
    sourceComponent.to(sink)
  }

  // ===================================================================================================================
  // Sink + building the result shortcuts
  // ===================================================================================================================

  // Sink.single + buildResult

  def singleResult()(implicit ec: ExecutionContext): Future[Out] = {
    val sink = Sink.single[Out]()
    sourceComponent.to(sink).buildResult()
  }

  // ===================================================================================================================
  // Connector
  // ===================================================================================================================

  // Connector.split

  def split(outputCount: Int, outputChooser: Out => BitSet): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    sourceComponent.to(splitter.inputs(0))
    splitter
  }

  def splitAsync(outputCount: Int, outputChooser: Out => Future[BitSet]): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, AsyncFunc(outputChooser))
    sourceComponent.to(splitter.inputs(0))
    splitter
  }

  def splitFunc(outputCount: Int, outputChooser: Func[Out, BitSet]): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    sourceComponent.to(splitter.inputs(0))
    splitter
  }

  // Connector.tee

  def tee(outputCount: Int): Splitter[_ <: Out] = {
    val splitter = Connector.tee[Out](outputCount)
    sourceComponent.to(splitter.inputs(0))
    splitter
  }

  // Connector.roundRobin

  def roundRobin(outputCount: Int): Splitter[_ <: Out] = {
    val splitter = Connector.roundRobin[Out](outputCount)
    sourceComponent.to(splitter.inputs(0))
    splitter
  }

  // Connector.scatter

  def scatter(outputCount: Int): Scatterer[_ <: Out] = {
    val scatterer = Connector.scatter[Out](outputCount)
    sourceComponent.to(scatterer.inputs(0))
    scatterer
  }

  // Connector.merge

  def merge[Super >: Out](sources: SourceComponent[Super]*): Merger[Super] = {
    val merger = Merger[Super](sources.size + 1)
    merger.connectInputs((sourceComponent +: sources).toList)
    merger
  }

  // ===================================================================================================================
  // Source
  // ===================================================================================================================

  // Source.concat

  /** Concatenate these sources after the current one.
    *
    * This is named concatWith and not simply Concat because SourceOps.concat refers to the unrelated Transform.concat.
    *
    * @see [[com.fsist.stream.Source.concat]] */
  def concatWith[Super >: Out](sources: SourceComponent[Super]*): SourceComponent[Super] =
    Source.concat(sourceComponent +: sources.toIndexedSeq)
}
