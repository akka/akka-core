/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import java.lang.reflect.Method
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.actor.{ ActorRef, ExtendedActorSystem }
import akka.event.LogMarker
import akka.event.Logging
import akka.remote.WireFormats.ActorRefData
import akka.serialization.{ BaseSerializer, Serialization }
import akka.serialization.SerializationExtension

object ProtobufSerializer {
  private val ARRAY_OF_BYTE_ARRAY = Array[Class[_]](classOf[Array[Byte]])

  /**
   * Helper to serialize an [[akka.actor.ActorRef]] to Akka's
   * protobuf representation.
   */
  def serializeActorRef(ref: ActorRef): ActorRefData = {
    ActorRefData.newBuilder.setPath(Serialization.serializedActorPath(ref)).build
  }

  /**
   * Helper to materialize (lookup) an [[akka.actor.ActorRef]]
   * from Akka's protobuf representation in the supplied
   * [[akka.actor.ActorSystem]].
   */
  def deserializeActorRef(system: ExtendedActorSystem, refProtocol: ActorRefData): ActorRef =
    system.provider.resolveActorRef(refProtocol.getPath)
}

/**
 * This Serializer serializes `akka.protobufv3.internal.GeneratedMessageV3` and `com.google.protobuf.Message`
 * It is using reflection to find the `parseFrom` and `toByteArray` methods to avoid
 * dependency to `com.google.protobuf`.
 */
class ProtobufSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  private val parsingMethodBindingRef = new AtomicReference[Map[Class[_], Method]](Map.empty)
  private val toByteArrayMethodBindingRef = new AtomicReference[Map[Class[_], Method]](Map.empty)

  private val allowedClassNames: Set[String] = {
    import scala.jdk.CollectionConverters._
    system.settings.config.getStringList("akka.serialization.protobuf.allowed-classes").asScala.toSet
  }

  // This must lazy otherwise it will deadlock the ActorSystem creation
  private lazy val serialization = SerializationExtension(system)

  private val log = Logging.withMarker(system, classOf[ProtobufSerializer])

  override def includeManifest: Boolean = true

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    manifest match {
      case Some(clazz) =>
        @tailrec
        def parsingMethod(method: Method = null): Method = {
          val parsingMethodBinding = parsingMethodBindingRef.get()
          parsingMethodBinding.get(clazz) match {
            case Some(cachedParsingMethod) => cachedParsingMethod
            case None =>
              checkAllowedClass(clazz)
              val unCachedParsingMethod =
                if (method eq null) clazz.getDeclaredMethod("parseFrom", ProtobufSerializer.ARRAY_OF_BYTE_ARRAY: _*)
                else method
              if (parsingMethodBindingRef.compareAndSet(
                    parsingMethodBinding,
                    parsingMethodBinding.updated(clazz, unCachedParsingMethod)))
                unCachedParsingMethod
              else
                parsingMethod(unCachedParsingMethod)
          }
        }
        parsingMethod().invoke(null, bytes)

      case None =>
        throw new IllegalArgumentException("Need a protobuf message class to be able to serialize bytes using protobuf")
    }
  }

  override def toBinary(obj: AnyRef): Array[Byte] = {
    val clazz = obj.getClass
    @tailrec
    def toByteArrayMethod(method: Method = null): Method = {
      val toByteArrayMethodBinding = toByteArrayMethodBindingRef.get()
      toByteArrayMethodBinding.get(clazz) match {
        case Some(cachedtoByteArrayMethod) => cachedtoByteArrayMethod
        case None =>
          val unCachedtoByteArrayMethod =
            if (method eq null) clazz.getMethod("toByteArray")
            else method
          if (toByteArrayMethodBindingRef.compareAndSet(
                toByteArrayMethodBinding,
                toByteArrayMethodBinding.updated(clazz, unCachedtoByteArrayMethod)))
            unCachedtoByteArrayMethod
          else
            toByteArrayMethod(unCachedtoByteArrayMethod)
      }
    }
    toByteArrayMethod().invoke(obj).asInstanceOf[Array[Byte]]
  }

  private def checkAllowedClass(clazz: Class[_]): Unit = {
    if (!isInAllowList(clazz)) {
      val warnMsg = s"Can't deserialize object of type [${clazz.getName}] in [${getClass.getName}]. " +
        "Only classes that are on the allow list are allowed for security reasons. " +
        "Configure allowed classes with akka.actor.serialization-bindings or " +
        "akka.serialization.protobuf.allowed-classes"
      log.warning(LogMarker.Security, warnMsg)
      throw new IllegalArgumentException(warnMsg)
    }
  }

  /**
   * Using the `serialization-bindings` as source for the allowed classes.
   * Note that the intended usage of serialization-bindings is for lookup of
   * serializer when serializing (`toBinary`). For deserialization (`fromBinary`) the serializer-id is
   * used for selecting serializer.
   * Here we use `serialization-bindings` also when deserializing (fromBinary)
   * to check that the manifest class is of a known (registered) type.
   *
   * If an old class is removed from `serialization-bindings` when it's not used for serialization
   * but still used for deserialization (e.g. rolling update with serialization changes) it can
   * be allowed by specifying in `akka.serialization.protobuf.allowed-classes`.
   *
   * That is also possible when changing a binding from a ProtobufSerializer to another serializer (e.g. Jackson)
   * and still bind with the same class (interface).
   */
  private def isInAllowList(clazz: Class[_]): Boolean = {
    isBoundToProtobufSerializer(clazz) || isInAllowListClassName(clazz)
  }

  private def isBoundToProtobufSerializer(clazz: Class[_]): Boolean = {
    try {
      val boundSerializer = serialization.serializerFor(clazz)
      boundSerializer.isInstanceOf[ProtobufSerializer]
    } catch {
      case NonFatal(_) => false // not bound
    }
  }

  private def isInAllowListClassName(clazz: Class[_]): Boolean = {
    allowedClassNames(clazz.getName) ||
    allowedClassNames(clazz.getSuperclass.getName) ||
    clazz.getInterfaces.exists(c => allowedClassNames(c.getName))
  }
}
