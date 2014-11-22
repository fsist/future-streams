package com.fsist.stream

import com.fsist.stream.run.FutureStreamBuilder

import scala.concurrent.ExecutionContext

/** Common marker trait of stream components: Source and Sink.
  *
  * NOTE: the trait constructor registers this instance with the StreamBuilder returned by `builder`!
  */
private[stream] trait StreamComponent {
  // This is implicit to become the default parameter value for methods in SourceOps
  implicit def builder: FutureStreamBuilder

  // Register on creation
  builder.register(this)
}

