package com.fsist.stream

import akka.http.util.FastFuture
import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.concurrent.{AsyncFunc, Func}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.TraversableOnce
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.BitSet
import scala.concurrent.{ExecutionContext, Future}

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
  self: Source[Out] =>

  // Transform.map

  def map[Next](mapper: Out => Next)
               (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.map(mapper))

  def mapAsync[Next](mapper: Out => Future[Next])
                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.map(AsyncFunc(mapper)))

  def mapFunc[Next](mapper: Func[Out, Next])
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.map(mapper))

  // Transform.flatMap

  def flatMap[Next](mapper: Out => Iterable[Next], onComplete: => Iterable[Next] = Iterable.empty)
                   (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.flatMap(mapper, onComplete))

  def flatMapAsync[Next](mapper: Out => Future[Iterable[Next]],
                         onComplete: => Future[Iterable[Next]] = FastFuture.successful(Iterable.empty))
                        (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.flatMap(AsyncFunc(mapper), AsyncFunc(onComplete)))

  def flatMapFunc[Next](mapper: Func[Out, Iterable[Next]], onComplete: Func[Unit, Iterable[Next]] = Func(Iterable.empty))
                       (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.flatMap(mapper, onComplete))

  // Transform.filter

  def filter(filter: Out => Boolean)
            (implicit ec: ExecutionContext, builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.filter(filter))

  def filterAsync(filter: Out => Future[Boolean])
                 (implicit ec: ExecutionContext, builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.filter(AsyncFunc(filter)))

  def filterFunc(filter: Func[Out, Boolean])
                (implicit ec: ExecutionContext, builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.filter(filter))

  // Transform.take

  def take(count: Long)
          (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.take(count))

  // Transform.drop

  def drop(count: Long)
          (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.drop(count))

  // Transform.flatten

  def flatten[Elem]()(implicit ev: Out <:< Iterable[Elem],
                      builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Elem] = {
    transform(Transform.flatten[Elem, Iterable]().asInstanceOf[Transform[Out, Elem]])
  }

  // Transform.takeElements

  def takeElements[Elem, Coll[Elem] <: Traversable[Elem]](count: Long)
                                                      (implicit ev: Out@uncheckedVariance =:= Coll[Elem],
                                                       cbf: CanBuildFrom[Nothing, Elem, Coll[Elem]],
                                                       builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[Out@uncheckedVariance, Out] = {
    val tr = Transform.takeElements(count)(cbf, builder)
    transform(tr.asInstanceOf[Transform[Out, Out]])
  }

  // Transform.dropElements

  def dropElements[Elem, Coll[Elem] <: Traversable[Elem]](count: Long)
                                                         (implicit ev: Out@uncheckedVariance =:= Coll[Elem],
                                                          cbf: CanBuildFrom[Nothing, Elem, Coll[Elem]],
                                                          builder: FutureStreamBuilder = new FutureStreamBuilder): Transform[Out@uncheckedVariance, Out] = {
    val tr = Transform.dropElements(count)(cbf, builder)
    transform(tr.asInstanceOf[Transform[Out, Out]])
  }

  // Sink.foreach

  def foreach[Super >: Out, Res](func: Super => Unit,
                                 onComplete: => Res = Func.nopLiteral,
                                 onError: Throwable => Unit = Func.nopLiteral)
                                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete, onError)
    connect(output)
  }

  def foreachAsync[Super >: Out, Res](func: Super => Future[Unit],
                                      onComplete: => Future[Res] = futureSuccess,
                                      onError: Throwable => Unit = Func.nopLiteral)
                                     (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreachAsync(func, onComplete, onError)
    to(output)
  }

  def foreachFunc[Super >: Out, Res](func: Func[Super, Unit],
                                     onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                                    (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreachFunc(func, onComplete, onError)
    to(output)
  }

  // Sink.foldLeft

  def foldLeft[Super >: Out, Res](init: Res)
                                 (onNext: (Super, Res) => Res,
                                  onError: Throwable => Unit = Func.nop)
                                 (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[Super, Res] = {
    val sink = Sink.foldLeft(init)(Function.tupled(onNext), onError)(builder, ec)
    to(sink)
  }

  def foldLeftAsync[Super >: Out, Res](init: Res)
                                      (onNext: ((Super, Res)) => Future[Res],
                                       onError: Throwable => Future[Unit] = Func.nopAsyncLiteral)
                                      (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[Super, Res] = {
    val sink = Sink.foldLeft(init)(AsyncFunc(onNext), AsyncFunc(onError))(builder, ec)
    to(sink)
  }

  def foldLeftFunc[Super >: Out, Res](init: Res)
                                     (onNext: Func[(Super, Res), Res],
                                      onError: Func[Throwable, Unit] = Func.nop)
                                     (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[Super, Res] = {
    val sink = Sink.foldLeft[Super, Res](init)(onNext, onError)(builder, ec)
    to(sink)
  }

  // Sink.collect

  // For the legality of the use of @uncheckedVariance, compare TraversableOnce.To[M]
  def collect[M[_]]()(implicit cbf: CanBuildFrom[Nothing, Out, M[Out@uncheckedVariance]],
                      builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Out@uncheckedVariance, M[Out@uncheckedVariance]] = {
    val collector = Sink.collect()(cbf, builder)
    to(collector)
  }

  def collectSuper[Super >: Out, M[_]]()(implicit cbf: CanBuildFrom[Nothing, Super, M[Super]],
                                         builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, M[Super]] = {
    val collector = Sink.collect()(cbf, builder)
    to(collector)
  }

  // Shortcuts for `collect`

  def toList()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, List[Out]] = collect[List]()

  def toSeq()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, Seq[Out]] = collect[Seq]()

  def toIndexedSeq()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, IndexedSeq[Out]] = collect[IndexedSeq]()

  def toVector()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, Vector[Out]] = collect[Vector]()

  def toSet[Super >: Out]()(implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, Set[Super]] = collectSuper[Super, Set]()

  // Sink.concat

  def concat[Elem, Coll[Elem] <: TraversableOnce[Elem]]()(implicit ev: Out@uncheckedVariance =:= Coll[Elem],
                                                          cbf: CanBuildFrom[Nothing, Elem, Coll[Elem]],
                                                          builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Out@uncheckedVariance, Out] = {
    val output = Sink.concat()(cbf, builder).asInstanceOf[StreamOutput[Out, Out]]
    to(output)
  }

  // Connector.split

  def split(outputCount: Int, outputChooser: Out => BitSet)
           (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    to(splitter.inputs(0))
    splitter
  }

  def splitAsync(outputCount: Int, outputChooser: Out => Future[BitSet])
                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, AsyncFunc(outputChooser))
    to(splitter.inputs(0))
    splitter
  }

  def splitFunc(outputCount: Int, outputChooser: Func[Out, BitSet])
               (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    to(splitter.inputs(0))
    splitter
  }

  // Connector.tee

  def tee(outputCount: Int)
         (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.tee[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }

  // Connector.roundRobin

  def roundRobin(outputCount: Int)
                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.roundRobin[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }

  // Connector.scatter

  def scatter(outputCount: Int)
             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Scatterer[_ <: Out] = {
    val scatterer = Connector.scatter[Out](outputCount)
    to(scatterer.inputs(0))
    scatterer
  }
}
