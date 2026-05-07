/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

object BoxedType {
  import java.{ lang => jl }

  private val toBoxed = Map[Class[_], Class[_]](
    classOf[Boolean] -> classOf[jl.Boolean],
    classOf[Byte] -> classOf[jl.Byte],
    classOf[Char] -> classOf[jl.Character],
    classOf[Short] -> classOf[jl.Short],
    classOf[Int] -> classOf[jl.Integer],
    classOf[Long] -> classOf[jl.Long],
    classOf[Float] -> classOf[jl.Float],
    classOf[Double] -> classOf[jl.Double],
    classOf[Unit] -> classOf[scala.runtime.BoxedUnit])

  final def apply(c: Class[_]): Class[_] = if (c.isPrimitive) toBoxed(c) else c
}
