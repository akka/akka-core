/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.ddata.protobuf

//#serializer
import akka.actor.ExtendedActorSystem
import akka.cluster.ddata.GSet
import akka.cluster.ddata.protobuf.SerializationSupport
import akka.serialization.Serializer
import docs.ddata.TwoPhaseSet
import docs.ddata.protobuf.msg.TwoPhaseSetMessages

class TwoPhaseSetSerializer2(val system: ExtendedActorSystem) extends Serializer with SerializationSupport {

  override def includeManifest: Boolean = false

  override def identifier = 99999

  override def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case m: TwoPhaseSet => twoPhaseSetToProto(m).toByteArray
    case _              => throw new IllegalArgumentException(s"Can't serialize object of type ${obj.getClass}")
  }

  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): AnyRef = {
    twoPhaseSetFromBinary(bytes)
  }

  def twoPhaseSetToProto(twoPhaseSet: TwoPhaseSet): TwoPhaseSetMessages.TwoPhaseSet2 = {
    val b = TwoPhaseSetMessages.TwoPhaseSet2.newBuilder()
    if (!twoPhaseSet.adds.isEmpty)
      b.setAdds(otherMessageToProto(twoPhaseSet.adds).toByteString())
    if (!twoPhaseSet.removals.isEmpty)
      b.setRemovals(otherMessageToProto(twoPhaseSet.removals).toByteString())
    b.build()
  }

  def twoPhaseSetFromBinary(bytes: Array[Byte]): TwoPhaseSet = {
    val msg = TwoPhaseSetMessages.TwoPhaseSet2.parseFrom(bytes)
    val adds =
      if (msg.hasAdds)
        otherMessageFromBinary(msg.getAdds.toByteArray).asInstanceOf[GSet[String]]
      else
        GSet.empty[String]
    val removals =
      if (msg.hasRemovals)
        otherMessageFromBinary(msg.getRemovals.toByteArray).asInstanceOf[GSet[String]]
      else
        GSet.empty[String]
    TwoPhaseSet(adds, removals)
  }
}
//#serializer
