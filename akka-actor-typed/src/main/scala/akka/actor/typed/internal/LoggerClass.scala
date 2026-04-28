/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.internal

import scala.util.control.NonFatal
import akka.annotation.InternalApi
import java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object LoggerClass {
  private val defaultPrefixesToSkip = List("scala.runtime", "akka.actor.typed.internal")

  /**
   * Try to extract a logger class from the call stack, if not possible the provided default is used
   */
  def detectLoggerClassFromStack(default: Class[_], additionalPrefixesToSkip: List[String] = Nil): Class[_] = {
    try {
      val walker = StackWalker.getInstance(RETAIN_CLASS_REFERENCE)
      val skipList = additionalPrefixesToSkip ::: defaultPrefixesToSkip
      def isInternal(clazz: Class[_]) = skipList.exists(clazz.getName.startsWith)

      walker.walk { frames =>
        frames.map[Class[_]](f => f.getDeclaringClass).filter(cls => !isInternal(cls)).findFirst().orElse(default)
      }
    } catch {
      case NonFatal(_) => default
    }
  }

}
