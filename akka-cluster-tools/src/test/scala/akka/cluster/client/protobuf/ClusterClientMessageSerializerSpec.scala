/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.client.protobuf

import scala.annotation.nowarn

import akka.actor.ExtendedActorSystem
import akka.cluster.client.ClusterReceptionist.Internal._
import akka.testkit.AkkaSpec

@nowarn("msg=deprecated")
class ClusterClientMessageSerializerSpec extends AkkaSpec {

  val serializer = new ClusterClientMessageSerializer(system.asInstanceOf[ExtendedActorSystem])

  def checkSerialization(obj: AnyRef): Unit = {
    val blob = serializer.toBinary(obj)
    val ref = serializer.fromBinary(blob, serializer.manifest(obj))
    ref should ===(obj)
  }

  "ClusterClientMessages" must {

    "be serializable" in {
      val contactPoints = Vector(
        "akka://system@node-1:2552/system/receptionist",
        "akka://system@node-2:2552/system/receptionist",
        "akka://system@node-3:2552/system/receptionist")
      checkSerialization(Contacts(contactPoints))
      checkSerialization(GetContacts)
      checkSerialization(Heartbeat)
      checkSerialization(HeartbeatRsp)
      checkSerialization(ReceptionistShutdown)
    }
  }
}
