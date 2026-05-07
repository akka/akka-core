/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.receptionist.Receptionist.Registered
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.Behaviors
import akka.serialization.jackson.CborSerializable
import akka.util.Timeout

object RemoteContextAskSpec {
  def config = ConfigFactory.parseString("""
    akka {
      loglevel = debug
      actor.provider = cluster
      remote.artery {
        canonical {
          hostname = 127.0.0.1
          port = 0
        }
      }
    }
  """)

  case object Pong extends CborSerializable
  case class Ping(respondTo: ActorRef[Pong.type]) extends CborSerializable

  def pingPong = Behaviors.receive[Ping] { (_, msg) =>
    msg match {
      case Ping(sender) =>
        sender ! Pong
        Behaviors.same
    }
  }

  val pingPongKey = ServiceKey[Ping]("ping-pong")

}

class RemoteContextAskSpec
    extends ScalaTestWithActorTestKit(RemoteContextAskSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import RemoteContextAskSpec._

  "Asking another actor through the ActorContext across remoting" must {

    "work" in {
      val node1 = Cluster(system)
      val node1Probe = TestProbe[AnyRef]()(system)
      node1.manager ! Join(node1.selfMember.address)

      Receptionist(system).ref ! Receptionist.Subscribe(pingPongKey, node1Probe.ref)
      node1Probe.expectMessageType[Receptionist.Listing]

      val system2 = ActorSystem(pingPong, system.name, system.settings.config)
      val node2 = Cluster(system2)
      node2.manager ! Join(node1.selfMember.address)

      val node2Probe = TestProbe[AnyRef]()(system2)
      Receptionist(system2).ref ! Receptionist.Register(pingPongKey, system2, node2Probe.ref)
      node2Probe.expectMessageType[Registered]

      // wait until the service is seen on the first node
      val remoteRef = node1Probe.expectMessageType[Receptionist.Listing].serviceInstances(pingPongKey).head

      spawn(Behaviors.setup[AnyRef] { ctx =>
        implicit val timeout: Timeout = 3.seconds

        ctx.ask(remoteRef, Ping.apply) {
          case Success(pong) => pong
          case Failure(ex)   => ex
        }

        Behaviors.receiveMessage { msg =>
          node1Probe.ref ! msg
          Behaviors.same
        }
      })

      node1Probe.expectMessageType[Pong.type]

    }

  }

}
