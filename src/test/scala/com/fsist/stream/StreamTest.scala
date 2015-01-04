package com.fsist.stream

/** General tests */
class StreamTest extends StreamTester {
  test("Link several builders") {
    // This is a regression test

    val src = Source.from(1 to 100)
    val mapper = Transform.map((i: Int) => i.toString)
    val toList = Transform.collect[String, List]()
    val sink = Sink.single[List[String]]()

    mapper.to(toList)
    toList.to(sink)
    src.to(mapper)

    val stream = toList.build()
    val result = stream(sink).result.futureValue(impatience)

    assert(result == (1 to 100).toList.map(_.toString))
  }
}
