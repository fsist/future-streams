package com.fsist.util

/** Thrown in case of logic errors */
case class BugException(message: String) extends Exception(message)
