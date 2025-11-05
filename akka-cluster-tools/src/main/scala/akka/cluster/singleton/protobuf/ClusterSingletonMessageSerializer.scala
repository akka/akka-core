/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.singleton.protobuf

import java.io.NotSerializableException

import akka.actor.ExtendedActorSystem
import akka.cluster.singleton.ClusterSingletonManager.Internal.HandOverDone
import akka.cluster.singleton.ClusterSingletonManager.Internal.HandOverInProgress
import akka.cluster.singleton.ClusterSingletonManager.Internal.HandOverToMe
import akka.cluster.singleton.ClusterSingletonManager.Internal.TakeOverFromMe
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API: Serializer of ClusterSingleton messages.
 * It is actually not using protobuf, but if we add more messages to
 * the ClusterSingleton we want to make protobuf representations of them.
 */
private[akka] class ClusterSingletonMessageSerializer(val system: ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  private val HandOverToMeManifest = "A"
  private val HandOverInProgressManifest = "B"
  private val HandOverDoneManifest = "C"
  private val TakeOverFromMeManifest = "D"

  private val emptyByteArray = Array.empty[Byte]

  private val fromBinaryMap = collection.immutable.HashMap[String, Array[Byte] => AnyRef](HandOverToMeManifest -> { _ =>
    HandOverToMe
  }, HandOverInProgressManifest -> { _ =>
    HandOverInProgress
  }, HandOverDoneManifest -> { _ =>
    HandOverDone
  }, TakeOverFromMeManifest -> { _ =>
    TakeOverFromMe
  })

  override def manifest(obj: AnyRef): String = obj match {
    case HandOverToMe       => HandOverToMeManifest
    case HandOverInProgress => HandOverInProgressManifest
    case HandOverDone       => HandOverDoneManifest
    case TakeOverFromMe     => TakeOverFromMeManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case HandOverToMe       => emptyByteArray
    case HandOverInProgress => emptyByteArray
    case HandOverDone       => emptyByteArray
    case TakeOverFromMe     => emptyByteArray
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass} in [${getClass.getName}]")
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    fromBinaryMap.get(manifest) match {
      case Some(f) => f(bytes)
      case None =>
        throw new NotSerializableException(
          s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
    }

}
