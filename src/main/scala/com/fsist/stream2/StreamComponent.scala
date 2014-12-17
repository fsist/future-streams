package com.fsist.stream2

import com.fsist.stream2.run.FutureStreamBuilder

import scala.concurrent.ExecutionContext

/** Common marker trait of stream components: Source and Sink.
  *
  * NOTE: the trait constructor registers this instance with the StreamBuilder returned by `builder`!
  */
sealed trait StreamComponent {
  // This is implicit to become the default parameter value for methods in SourceOps
  implicit def builder: FutureStreamBuilder

  // Register on creation
  builder.register(this)
}

/** This trait allows extending the sealed StreamComponent trait inside this package. */
private[stream2] trait StreamComponentBase extends StreamComponent