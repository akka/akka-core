/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.nio.ByteBuffer

import akka.actor.ExtendedActorSystem
import akka.serialization.BaseSerializer
import akka.serialization.ByteBufferSerializer

@deprecated("Moved to akka.serialization.LongSerializer in akka-actor", "2.6.0")
class LongSerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {
  // this serializer is not used unless someone is instantiating it manually, it's not in config
  private val delegate = new akka.serialization.LongSerializer(system)

  override def includeManifest: Boolean = delegate.includeManifest
  override val identifier: Int = delegate.identifier
  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = delegate.toBinary(o, buf)
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = delegate.fromBinary(buf, manifest)
  override def toBinary(o: AnyRef): Array[Byte] = delegate.toBinary(o)
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = delegate.fromBinary(bytes, manifest)
}

@deprecated("Moved to akka.serialization.IntSerializer in akka-actor", "2.6.0")
class IntSerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {
  // this serializer is not used unless someone is instantiating it manually, it's not in config
  private val delegate = new akka.serialization.IntSerializer(system)

  override def includeManifest: Boolean = delegate.includeManifest
  override val identifier: Int = delegate.identifier
  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = delegate.toBinary(o, buf)
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = delegate.fromBinary(buf, manifest)
  override def toBinary(o: AnyRef): Array[Byte] = delegate.toBinary(o)
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = delegate.fromBinary(bytes, manifest)
}

@deprecated("Moved to akka.serialization.StringSerializer in akka-actor", "2.6.0")
class StringSerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {
  // this serializer is not used unless someone is instantiating it manually, it's not in config
  private val delegate = new akka.serialization.StringSerializer(system)

  override def includeManifest: Boolean = delegate.includeManifest
  override val identifier: Int = delegate.identifier
  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = delegate.toBinary(o, buf)
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = delegate.fromBinary(buf, manifest)
  override def toBinary(o: AnyRef): Array[Byte] = delegate.toBinary(o)
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = delegate.fromBinary(bytes, manifest)
}

@deprecated("Moved to akka.serialization.ByteStringSerializer in akka-actor", "2.6.0")
class ByteStringSerializer(val system: ExtendedActorSystem) extends BaseSerializer with ByteBufferSerializer {
  // this serializer is not used unless someone is instantiating it manually, it's not in config
  private val delegate = new akka.serialization.ByteStringSerializer(system)

  override def includeManifest: Boolean = delegate.includeManifest
  override val identifier: Int = delegate.identifier
  override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = delegate.toBinary(o, buf)
  override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = delegate.fromBinary(buf, manifest)
  override def toBinary(o: AnyRef): Array[Byte] = delegate.toBinary(o)
  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = delegate.fromBinary(bytes, manifest)
}
