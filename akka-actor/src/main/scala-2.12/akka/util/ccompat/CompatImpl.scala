/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://akka.io>
 */

package akka.util.ccompat

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

/**
 * INTERNAL API
 *
 * Based on https://github.com/scala/scala-collection-compat/blob/master/compat/src/main/scala-2.11_2.12/scala/collection/compat/CompatImpl.scala
 * but reproduced here so we don't need to add a dependency on this library. It contains much more than we need right now, and is
 * not promising binary compatibility yet at the time of writing.
 */
private[ccompat] object CompatImpl {
  def simpleCBF[A, C](f: => Builder[A, C]): CanBuildFrom[Any, A, C] = new CanBuildFrom[Any, A, C] {
    def apply(from: Any): Builder[A, C] = apply()
    def apply(): Builder[A, C] = f
  }
}
