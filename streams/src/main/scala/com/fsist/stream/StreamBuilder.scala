package com.fsist.stream

import scala.concurrent.{ExecutionContext, Future}

/** Common marker trait of stream components: Source and Sink.
  *
  * NOTE: the trait constructor registers this instance with the StreamBuilder returned by `builder`!
  */
trait StreamComponent {
  // This is implicit to become the default parameter value for methods in SourceOps
  implicit def builder: StreamBuilder

  // Register on creation
  builder.register(this)
}

sealed trait RunningStreamComponent {
  def builder: StreamBuilder
  def completion: Future[Unit]
}

final class RunningSource[+Out](val source: Source[Out], val completion: Future[Unit]) extends RunningStreamComponent {
  override def builder: StreamBuilder = source.builder
}

final class RunningSink[-In, +Res](val sink: Sink[In, Res], val result: Future[Res])
                                  (implicit ec: ExecutionContext) extends RunningStreamComponent {
  override def builder: StreamBuilder = sink.builder
  override val completion: Future[Unit] = result map (_ => ())
}

sealed trait FutureStream {
  def builder: StreamBuilder

  /** Starts the stream and returns a future that completes when all stream components have terminated.
    *
    * If called more than once, does nothing and returns the same Future instance.
    */
  def run(): Future[Unit]

  def sources: Map[Source[_], RunningSource[_]]
  def get[Out](source: Source[Out]): Option[RunningSource[Out]] = sources.get(source).map(_.asInstanceOf[RunningSource[Out]])
  def apply[Out](source: Source[Out]): RunningSource[Out] = sources.apply(source).asInstanceOf[RunningSource[Out]]

  def sinks: Map[Sink[_, _], RunningSink[_, _]]
  def get[In, Res](sink: Sink[In, Res]): Option[RunningSink[In, Res]] = sinks.get(sink).map(_.asInstanceOf[RunningSink[In, Res]])
  def apply[In, Res](sink: Sink[In, Res]): RunningSink[In, Res] = sinks.apply(sink).asInstanceOf[RunningSink[In, Res]]
}

private[stream] abstract class FutureStreamImpl(val builder: StreamBuilder,
                                                 val sources: Map[Source[_], RunningSource[_]],
                                                 val sinks: Map[Sink[_, _], RunningSink[_, _]]) extends FutureStream

/** Mutable state describing the stream graph being built. */
class StreamBuilder {
  def register(component: StreamComponent): Unit = ???
  def connect[Super >: Out, Out](source: Source[Out], sink: Sink[Super, _]): Unit = ???

  def build()(implicit ec: ExecutionContext) : FutureStream = ???
}
