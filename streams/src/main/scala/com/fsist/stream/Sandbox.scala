package com.fsist.stream

class Parent
class Child extends Parent

object Sandbox {
  import scala.concurrent.ExecutionContext.Implicits.global

  Source.from(1 to 1000).map(_.toString).foreach(println(_)).run()
}
