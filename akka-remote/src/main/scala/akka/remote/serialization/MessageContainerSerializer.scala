/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.serialization

import scala.collection.immutable

import akka.actor.ActorSelectionMessage
import akka.actor.ExtendedActorSystem
import akka.actor.SelectChildName
import akka.actor.SelectChildPattern
import akka.actor.SelectParent
import akka.actor.SelectionPathElement
import akka.protobufv3.internal.ByteString
import akka.remote.ByteStringUtils
import akka.remote.ContainerFormats
import akka.serialization.{ BaseSerializer, SerializationExtension, Serializers }

class MessageContainerSerializer(val system: ExtendedActorSystem) extends BaseSerializer {

  private lazy val serialization = SerializationExtension(system)

  def includeManifest: Boolean = false

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case sel: ActorSelectionMessage => serializeSelection(sel)
    case _                          => throw new IllegalArgumentException(s"Cannot serialize object of type [${obj.getClass.getName}]")
  }

  import ContainerFormats.PatternType._

  private def serializeSelection(sel: ActorSelectionMessage): Array[Byte] = {
    val builder = ContainerFormats.SelectionEnvelope.newBuilder()
    val message = sel.msg.asInstanceOf[AnyRef]
    val serializer = serialization.findSerializerFor(message)
    builder
      .setEnclosedMessage(ByteStringUtils.toProtoByteStringUnsafe(serializer.toBinary(message)))
      .setSerializerId(serializer.identifier)
      .setWildcardFanOut(sel.wildcardFanOut)

    val ms = Serializers.manifestFor(serializer, message)
    if (ms.nonEmpty) builder.setMessageManifest(ByteString.copyFromUtf8(ms))

    sel.elements.foreach {
      case SelectChildName(name) =>
        builder.addPattern(buildPattern(Some(name), CHILD_NAME))
      case SelectChildPattern(patternStr) =>
        builder.addPattern(buildPattern(Some(patternStr), CHILD_PATTERN))
      case SelectParent =>
        builder.addPattern(buildPattern(None, PARENT))
    }

    builder.build().toByteArray
  }

  private def buildPattern(
      matcher: Option[String],
      tpe: ContainerFormats.PatternType): ContainerFormats.Selection.Builder = {
    val builder = ContainerFormats.Selection.newBuilder().setType(tpe)
    matcher.foreach(builder.setMatcher)
    builder
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val selectionEnvelope = ContainerFormats.SelectionEnvelope.parseFrom(bytes)
    val manifest = if (selectionEnvelope.hasMessageManifest) selectionEnvelope.getMessageManifest.toStringUtf8 else ""
    val msg = serialization
      .deserialize(selectionEnvelope.getEnclosedMessage.toByteArray, selectionEnvelope.getSerializerId, manifest)
      .get

    import scala.jdk.CollectionConverters._
    val elements: immutable.Iterable[SelectionPathElement] = selectionEnvelope.getPatternList.asScala.iterator
      .map { x =>
        x.getType match {
          case CHILD_NAME    => SelectChildName(x.getMatcher)
          case CHILD_PATTERN => SelectChildPattern(x.getMatcher)
          case PARENT        => SelectParent
        }

      }
      .to(immutable.IndexedSeq)
    val wildcardFanOut = if (selectionEnvelope.hasWildcardFanOut) selectionEnvelope.getWildcardFanOut else false
    ActorSelectionMessage(msg, elements, wildcardFanOut)
  }
}
