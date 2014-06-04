package com.fsist.stream

import com.typesafe.scalalogging.slf4j.{Logger, Logging}
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicReference

/** A source-compatible replacement for the com.typesafe.scalalogging.slf4j.Logging trait that allows appending
  * a string to the logger name by calling the `named` method. The Logger object returned by `this.logger` is still a
  * Typesafe Logging object whose methods are macros.
  *
  * This allows instances (sources, sinks, pipe segments) to be tagged with their semantic function. Otherwise, trace
  * logging in a pipeline is not usable.
  */
trait NamedLogger {
  @volatile private var _name : String = ""
  private lazy val className: String = getClass.getName
  private val currentLogger = new AtomicReference[Logger]()

  /** Sets the name that will be used for all log statements from this instance. */
  def named(n: String) : this.type = {
    _name = n
    // Next access to `logger` will reinit, but in case of multiple calls to `named`, don't create multiple loggers
    currentLogger.set(null)
    this
  }

  /** Returns the currently set name for this component. */
  def name: String = _name match {
    case null => ""
    case s => s
  }

  protected def logger: Logger =
    currentLogger.get match {
      case null =>
        val fullName = className + (_name match {
          case null => ""
          case n => "/" + n
        })
        val l = Logger(LoggerFactory getLogger fullName)
        if (currentLogger.compareAndSet(null, l)) l
        else {
          // Someone else won the race to set the logger
          logger
        }
      case l => l
    }
}
