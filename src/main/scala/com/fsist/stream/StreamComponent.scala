package com.fsist.stream

import java.util.concurrent.atomic.AtomicReference

import com.fsist.stream.run.{RunningOutput, RunningStream, FutureStreamBuilder}

import scala.concurrent.{Future, ExecutionContext}

/** Common marker trait of stream components: Source, Sink, Transform, etc.
  *
  * NOTE: the trait constructor registers this instance with the StreamBuilder returned by `builder`!
  */
sealed trait StreamComponent {
  // This is implicit to become the default parameter value for methods in SourceOps
  implicit def builder: FutureStreamBuilder

  require(builder eq builder, "`builder` method returns different values on successive calls. " +
    "You probably wrote `override def builder ` where you meant `override val builder...`")

  // Register on creation
  builder.register(this)

  /** Materializes the stream, including all components linked (transitively) to this one, and starts running it.
    *
    * The same result is produced no matter which stream component is used to call `build`.
    *
    * This method may only be called once per stream (see README on the subject of reusing components).
    */
  def build()(implicit ec: ExecutionContext): RunningStream = builder.run()
}

/** This trait allows extending the sealed StreamComponent trait inside this package. */
private[stream] trait StreamComponentBase extends StreamComponent

/** A mixin for trait-based stream component implementations that provides a new builder in a succint way.
  * You can still override it to plug in your own builder.
  */
trait NewBuilder {
  self: StreamComponent =>
  private[this] lazy val myBuilder = new FutureStreamBuilder

  // Don't use a val, because then overriding this with a def becomes impossible, and overriding with another val
  // leads to circular initialization issues
  override implicit def builder: FutureStreamBuilder = myBuilder
}
