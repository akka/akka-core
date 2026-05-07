/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed.internal

import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.typed.internal.receptionist.ClusterReceptionist
import akka.serialization.SerializationExtension

class AkkaClusterTypedSerializerSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

  val ref = spawn(Behaviors.empty[String])
  val classicSystem = system.toClassic
  val serializer = new AkkaClusterTypedSerializer(classicSystem.asInstanceOf[ExtendedActorSystem])

  "AkkaClusterTypedSerializer" must {

    Seq("ReceptionistEntry" -> ClusterReceptionist.Entry(ref, 666L)(System.currentTimeMillis())).foreach {
      case (scenario, item) =>
        s"resolve serializer for $scenario" in {
          val serializer = SerializationExtension(classicSystem)
          serializer.serializerFor(item.getClass).getClass should be(classOf[AkkaClusterTypedSerializer])
        }

        s"serialize and de-serialize $scenario" in {
          verifySerialization(item)
        }
    }
  }

  def verifySerialization(msg: AnyRef): Unit = {
    serializer.fromBinary(serializer.toBinary(msg), serializer.manifest(msg)) should be(msg)
  }

}
