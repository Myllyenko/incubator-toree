package org.apache.toree

/**
 * A wrapper used to workaround
 * <a href="http://docs.scala-lang.org/overviews/reflection/thread-safety.html">thread safety issues</a> in Scala 2.10 reflection
 * facilities.
 * <p>Any interaction with scala.reflect must be performed only via the [[ReflectionAccessor.useReflection()]] method.</p>
 * <p>As the issue is Scala 2.10-specific, in Scala 2.11 environment, this class has no "real" functionality.</p>
 */
object ReflectionAccessor {
  /**
    * Executes the specified code without any additional actions.
    *
    * @param code the code to be executed
    *
    * @return what the specified code returns
    */
  def useReflection[T](code: => T): T = {
    code
  }
}
