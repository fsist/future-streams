package com.fsist.stream

import com.fsist.util.concurrent.{BoundedAsyncQueue, AsyncQueue, Func}
import org.scalatest.FunSuite
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import scala.concurrent.Promise
import scala.concurrent.duration._

class SourceTest extends FunSuite with StreamTester {
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
    val stream = source.discard().build()
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
    assert(result.futureValue == List(1, 2))
  }

  test("from BoundedAsyncQueue") {
    val queue = new BoundedAsyncQueue[Option[Int]](1)

    val result = Source.from(queue).collect[List].singleResult()
    awaitTimeout(result)(impatience)

    queue.enqueue(Some(1)).futureValue
    queue.enqueue(Some(2)).futureValue
    awaitTimeout(result)(impatience)

    queue.enqueue(None).futureValue
    assert(result.futureValue == List(1, 2))
  }

  test("Source.pusher") {
    val pusher = Source.pusher[Int]
    val result = pusher.collect[List].singleResult()

    awaitTimeout(result)(impatience)

    pusher.push(1)
    pusher.push(2)
    awaitTimeout(result)(impatience)

    pusher.complete()
    assert(result.futureValue == List(1, 2))
  }

  test("Source.asyncPusher") {
    val pusher = Source.asyncPusher[Int](10)
    val result = pusher.collect[List].singleResult()

    awaitTimeout(result)(impatience)

    pusher.push(1).futureValue
    pusher.push(2).futureValue
    awaitTimeout(result)(impatience)

    pusher.complete().futureValue
    assert(result.futureValue == List(1, 2))
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

  test("Source.concat") {
    val data = 1 to 5

    for (count <- 0 to 5) {
      val sources = List.fill(count)(Source.from(data))
      val source = Source.concat(sources)
      val result = source.toList.singleResult().futureValue.sorted

      val expected = Seq.fill(count)(data).flatten.sorted
      assert(result == expected, s"Concat $count sources")
    }
  }

  test("Source.concat multi layered") {
    // This is a regression test

    val data = 1 to 5
    val source = Source.from(data).concatWith().concatWith()
    val result = source.toList.singleResult().futureValue.sorted

    val expected = data
    assert(result == expected)
  }


  test("Source.concat multi layered, wide") {
    // This is a regression test

    def merge(level: Int, count: Int): Source[Int] = {
      if (level == 0) Source.empty[Int]
      else Source.concat(for (x <- 1 to count) yield merge(level - 1, count).sourceComponent)
    }

    val source = merge(3, 3)

    val result = source.toList.singleResult().futureValue.sorted

    assert(result == List())
  }

  test("DrivenSource") {
    val data = 1 to 10

    val source = Source.driven[Int]
    awaitTimeout(source.aside, "Consumer should not be provided until stream starts running")(impatience)

    val result = source.toList.singleResult
    awaitTimeout(result, "Stream should not complete while we haven't pushed EOF into the consumer")(impatience)

    val consumer = source.aside.futureValue
    assert(consumer.onNext.isSync, "Entire stream was built synchronously")

    for (i <- data) consumer.onNext.asSync.apply(i)
    consumer.onComplete.asSync.apply(())

    assert(result.futureValue == data)
  }

  test("Completion: ProducerMachine") {
    val source = Source.from(1 to 10)
    val stream = source.discard().build()
    stream.completion.futureValue
    assert(stream.components(source).completion.isCompleted)
  }

  test("Completion: DelayedSourceMachine") {
    val promise = Promise[Source[Int]]()
    val source = Source.flatten[Int](promise.future)
    val stream = source.discard().build()
    promise.success(Source.from(1 to 10))
    stream.completion.futureValue
    assert(stream.components(source).completion.isCompleted)
  }

  test("Completion: DrivenSourceMachine") {
    val source = Source.driven[Int]
    val stream = source.discard().build()

    val consumer = source.aside.futureValue(impatience)
    assert(consumer.onNext.isSync)
    consumer.onNext.asSync(1)
    assert(consumer.onComplete.isSync)
    consumer.onComplete.asSync(())

    stream.completion.futureValue(impatience)
    assert(stream.components(source).completion.isCompleted)
  }
}
