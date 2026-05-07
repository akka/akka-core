/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.util

import java.io.{ InputStream, ObjectInputStream, ObjectStreamClass }

/**
 * ClassLoaderObjectInputStream tries to utilize the provided ClassLoader to load Classes and falls
 * back to ObjectInputStreams resolver.
 *
 * @param classLoader - the ClassLoader which is to be used primarily
 * @param is - the InputStream that is wrapped
 */
class ClassLoaderObjectInputStream(classLoader: ClassLoader, is: InputStream) extends ObjectInputStream(is) {
  override protected def resolveClass(objectStreamClass: ObjectStreamClass): Class[_] =
    try Class.forName(objectStreamClass.getName, false, classLoader)
    catch {
      case _: ClassNotFoundException => super.resolveClass(objectStreamClass)
    }
}
