/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream }

import akka.actor.ExtendedActorSystem
import akka.serialization.{ BaseSerializer, JavaSerializer }
import akka.util.ClassLoaderObjectInputStream

/**
 * This Serializer uses standard Java Serialization and is useful for tests where ad-hoc messages are created and sent
 * between actor systems. It needs to be explicitly enabled in the config (or through `ActorSystemSetup`) like so:
 *
 * ```
 * akka.actor.serialization-bindings {
 *   "my.test.AdHocMessage" = java-test
 * }
 * ```
 */
class TestJavaSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  def includeManifest: Boolean = false

  def toBinary(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream
    val out = new ObjectOutputStream(bos)
    JavaSerializer.currentSystem.withValue(system) { out.writeObject(o) }
    out.close()
    bos.toByteArray
  }

  def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    val in = new ClassLoaderObjectInputStream(system.dynamicAccess.classLoader, new ByteArrayInputStream(bytes))
    val obj = JavaSerializer.currentSystem.withValue(system) { in.readObject }
    in.close()
    obj
  }
}
