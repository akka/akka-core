/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object LoggerClass {

  // just to get access to the class context
  private final class TrickySecurityManager extends SecurityManager {
    def getClassStack: Array[Class[_]] = getClassContext
  }

  private val defaultPrefixesToSkip = List("scala.runtime", "akka.actor.typed.internal")

  /**
   * Try to extract a logger class from the call stack, if not possible the provided default is used
   */
  def detectLoggerClassFromStack(default: Class[_], additionalPrefixesToSkip: List[String] = Nil): Class[_] = {
    // TODO use stack walker API when we no longer need to support Java 8
    try {
      def skip(name: String): Boolean = {
        def loop(skipList: List[String]): Boolean = skipList match {
          case Nil => false
          case head :: tail =>
            if (name.startsWith(head)) true
            else loop(tail)
        }

        loop(additionalPrefixesToSkip ::: defaultPrefixesToSkip)
      }

      val trace = new TrickySecurityManager().getClassStack
      var suitableClass: OptionVal[Class[_]] = OptionVal.None
      var idx = 1 // skip this method/class and right away
      while (suitableClass.isEmpty && idx < trace.length) {
        val clazz = trace(idx)
        val name = clazz.getName
        if (!skip(name)) suitableClass = OptionVal.Some(clazz)
        idx += 1
      }
      suitableClass.getOrElse(default)
    } catch {
      case NonFatal(_) => default
    }
  }

}
