package com.fsist.util

import annotation.tailrec
import java.util.concurrent.atomic.AtomicReference
import scala.language.implicitConversions

/** Created by Piotr Gabryanczyk, Atomic is a wrapper for immutable instances, that
  * allows an atomic update of the reference. The update method applies the provided
  * function on the old value and stores the resulting value. If, while update is
  * performed, the reference is changed by another thread, update tries again to
  * apply the function on the new reference.
  *
  * Here's the blog post from which the code was taken:
  *
  * [[http://blog.scala4java.com/2012/03/atomic-update-of-atomicreference.html]]
  */
class Atomic[T] (orig: T) {
  private val atomic: AtomicReference[T] = new AtomicReference[T](orig)

  /** Update gets a function from the previous buffer value to a tuple of new value to update to
    * and a "ricochet" - some value generated in the process of updating the buffer. The method sets the new value
    * atomically, and returns the ricochet.
    *
    * @param f a function from the old value of the Atomic, to a new value to set and a ricochet to return
    * @return The ricochet (a value generated in the process of calculating and setting the new value of the Atomic.
    */
  @tailrec
  final def update[A](f: T => (T,A)) : A = {
    //Take a snapshot of the current value
    val oldValue = atomic.get()

    //Use the provided function to calculate the new value and the ricochet
    val (newValue,ricochet) = f(oldValue)

    //Set the new value atomically. If another thread has changed the old value after we took a snapshot, run f again.
    //Eventually set the new value, and return the ricochet.
    if (atomic.compareAndSet(oldValue, newValue)) ricochet else update(f)
  }

  def get() : T = atomic.get()
  def set(t: T) : Unit = atomic.set(t)

  /** Attempt to apply the update function `f` to the current value until we succeed (racing against other threads
    * modifying this value), and return the new value calculated by `f`.
    *
    * NOTE that `f` MUST have no side effects, as it can be called any number of times.
    */
  @tailrec
  final def update1(f: T => T) : T = {
    val oldValue = atomic.get
    val newValue = f(oldValue)
    if (atomic.compareAndSet(oldValue, newValue)) newValue else update1(f)
  }
}

