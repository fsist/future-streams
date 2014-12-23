package com.fsist.stream

import com.fsist.util.concurrent.{BoundedAsyncQueue, AsyncQueue, Func}
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.Promise
import scala.concurrent.duration._

class StreamInputTest extends FunSuite with StreamTester {
  test("IteratorSource") {
    val range = 1 to 10
    val iter = range.iterator
    val result = Source.from(iter).toList().singleResult().futureValue

    assert(result == range, "All items were passed")
  }

  test("IterableSource") {
    val range = 1 to 10
    val result = Source.from(range).toList().singleResult().futureValue

    assert(result == range, "All items were passed")
  }

  test("GeneratorSource") {
    val range = 1 to 10
    val iter = range.iterator

    val source = Source.generate(if (iter.hasNext) iter.next() else throw new EndOfStreamException)
    val result = source.toList().singleResult().futureValue
    assert(result == range, "All items were generated")
  }

  test("StreamInput completion promise is fulfilled") {
    val source = Source.of(1, 2, 3)
    val stream = source.foreach(Func.nop).build()
    stream(source).completion.futureValue(Timeout(1.second))
  }

  test("from AsyncQueue") {
    val queue = new AsyncQueue[Option[Int]]
    
    val result = Source.from(queue).collect[List].singleResult()
    awaitTimeout(result)(impatience)
    
    queue.enqueue(Some(1))
    queue.enqueue(Some(2))
    awaitTimeout(result)(impatience)
    
    queue.enqueue(None)
    assert(result.futureValue == List(1,2))
  }

  test("from BoundedAsyncQueue") {
    val queue = new BoundedAsyncQueue[Option[Int]](1)

    val result = Source.from(queue).collect[List].singleResult()
    awaitTimeout(result)(impatience)

    queue.enqueue(Some(1)).futureValue
    queue.enqueue(Some(2)).futureValue
    awaitTimeout(result)(impatience)

    queue.enqueue(None).futureValue
    assert(result.futureValue == List(1,2))
  }

  test("Source.pusher") {
    val pusher = Source.pusher[Int]
    val result = pusher.collect[List].singleResult()

    awaitTimeout(result)(impatience)

    pusher.push(1)
    pusher.push(2)
    awaitTimeout(result)(impatience)

    pusher.complete()
    assert(result.futureValue == List(1,2))
  }

  test("Source.asyncPusher") {
    val pusher = Source.asyncPusher[Int](10)
    val result = pusher.collect[List].singleResult()

    awaitTimeout(result)(impatience)

    pusher.push(1).futureValue
    pusher.push(2).futureValue
    awaitTimeout(result)(impatience)

    pusher.complete().futureValue
    assert(result.futureValue == List(1,2))
  }

  test("DelayedSource") {
    val data = 1 to 10
    val promise = Promise[Source[Int]]()
    val stream = Source.flatten(promise.future).toList.singleResult()

    awaitTimeout(stream, "Stream doesn't complete while waiting for delayed source")(impatience)

    val source = Source.from(data)
    promise.success(source)

    assert(stream.futureValue == data)
  }

  test("DelayedSource (when the Future fails)") {
    val data = 1 to 10
    val promise = Promise[Source[Int]]()
    val stream = Source.flatten(promise.future).toList.singleResult()

    val error = new IllegalArgumentException("test")
    promise.failure(error)

    assert(stream.failed.futureValue == error)
  }
}
