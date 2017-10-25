package org.apache.toree

/**
 * A wrapper used to workaround
 * <a href="http://docs.scala-lang.org/overviews/reflection/thread-safety.html">thread safety issues</a> in Scala 2.10 reflection
 * facilities.
 * <p>Any interaction with scala.reflect must be performed only via the [[ReflectionAccessor.useReflection()]] method.</p>
 */
object ReflectionAccessor {
  private val lock: Object = new Object

  /**
    * Executes the specified code within a synchronized block. The same lock is used for synchronisation upon each call.
    *
    * @param code the code to be executed
    *
    * @return what the specified code returns
    */
  def useReflection[T](code: => T): T = {
    lock.synchronized {
      code
    }
  }
}
