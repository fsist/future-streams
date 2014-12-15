package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder
import com.fsist.util.{Func, AsyncFunc}

import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.BitSet
import scala.concurrent.{ExecutionContext, Future}

import scala.language.higherKinds

/** Adds various shortcuts to StreamOutput. Doesn't implement anything new, just provides convenience wrappers
  * for constructors of Source, Transform, Sink and Connect.
  */
trait SourceOps[+Out] {
  self: Source[Out] =>

  // TODO link scaladocs

  // These are just aliases for `connect`
  def to(sink: Sink[Out]): sink.type = connect(sink)

  def transform[Next](tr: Transform[Out, Next]): tr.type = connect(tr)

  // Shortcuts for Transform constructors

  def map[Next](mapper: Out => Next)
               (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Next] =
    transform(Transform.map(mapper))

  def filter(filter: Out => Boolean)
            (implicit ec: ExecutionContext, builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.filter(filter))

  def skip(count: Long)
          (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Source[Out] =
    transform(Transform.skip(count))

  // Shortcuts for Sink constructors

  def foreach[Super >: Out](func: Super => Unit)
                           (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(func)
    to(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit, onComplete: Unit => Res)
                                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete)
    to(output)
  }

  def foreach[Super >: Out, Res](func: Super => Unit, onComplete: Unit => Res, onError: Throwable => Unit)
                                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete, onError)
    to(output)
  }

  def foreach[Super >: Out, Res](func: Func[Super, Unit], onComplete: Func[Unit, Res] = Func.nop, onError: Func[Throwable, Unit] = Func.nop)
                                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Res] = {
    val output = Sink.foreach(func, onComplete, onError)
    to(output)
  }

  def foreachAsync[Super >: Out](func: Super => Future[Unit])
                                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, Unit] = {
    val output = Sink.foreach(AsyncFunc(func))
    to(output)
  }

  def foldLeft[Super >: Out, Res, State](state: State)(onNext: Func[(Super, State), State], onComplete: Func[State, Res],
                                                       onError: Func[Throwable, Unit] = Func.nop)
                                        (implicit builder: FutureStreamBuilder = new FutureStreamBuilder, ec: ExecutionContext): StreamOutput[Super, Res] = {
    val sink = Sink.foldLeft[Super, Res, State](state)(onNext, onComplete, onError)(builder, ec)
    to(sink)
  }

  def collect[Super >: Out, M[_]](onError: Func[Throwable, Unit] = Func.nop)
                                 (implicit cbf: CanBuildFrom[Nothing, Super, M[Super]],
                                  builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[Super, M[Super]] = {
    val collector = Sink.collect(onError)(cbf, builder)
    to(collector)
  }

  /** Collects to a List. Because generic arguments can't have default values, it's useful to have this overload without
    * manually specifying the collection type every time.
    */
  def toList(onError: Func[Throwable, Unit] = Func.nop)
            (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): StreamOutput[_ <: Out, List[Out]] = {
    val collector = Sink.collect[Out, List](onError)
    to(collector)
  }

  // Shortcuts for Connector constructors

  def split(outputCount: Int, outputChooser: Func[Out, BitSet])
           (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.split(outputCount, outputChooser)
    to(splitter.inputs(0))
    splitter
  }

  def tee(outputCount: Int = 2)
         (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.tee[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }

  def roundRobin(outputCount: Int = 2)
                (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Splitter[_ <: Out] = {
    val splitter = Connector.roundRobin[Out](outputCount)
    to(splitter.inputs(0))
    splitter
  }

  def scatter(outputCount: Int = 2)
             (implicit builder: FutureStreamBuilder = new FutureStreamBuilder): Scatterer[_ <: Out] = {
    val scatterer = Connector.scatter[Out](outputCount)
    to(scatterer.inputs(0))
    scatterer
  }
}
