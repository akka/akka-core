/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.io.NotSerializableException
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Dropped
import akka.actor.PoisonPill
import akka.actor.Props
import akka.remote.RARP
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestProbe
import akka.util.ByteString

object RemoteMessageSerializationSpec {
  class ProxyActor(val one: ActorRef, val another: ActorRef) extends Actor {
    def receive = {
      case s if sender().path == one.path     => another ! s
      case s if sender().path == another.path => one ! s
    }
  }
}

class RemoteMessageSerializationSpec extends ArteryMultiNodeSpec with ImplicitSender {

  val maxPayloadBytes = RARP(system).provider.remoteSettings.Artery.Advanced.MaximumFrameSize

  val remoteSystem = newRemoteSystem()
  val remotePort = port(remoteSystem)

  "Remote message serialization" should {

    "drop unserializable messages" in {
      object Unserializable
      EventFilter[NotSerializableException](pattern = ".*No configured serialization.*", occurrences = 1).intercept {
        verifySend(Unserializable) {
          expectNoMessage(1.second) // No AssociationErrorEvent should be published
        }
      }
    }

    "allow messages up to payload size" in {
      val maxProtocolOverhead = 500 // Make sure we're still under size after the message is serialized, etc
      val big = byteStringOfSize(maxPayloadBytes - maxProtocolOverhead)
      verifySend(big) {
        expectMsg(3.seconds, big)
      }
    }

    "drop sent messages over payload size" in {
      val droppedProbe = TestProbe()
      system.eventStream.subscribe(droppedProbe.ref, classOf[Dropped])
      val oversized = byteStringOfSize(maxPayloadBytes + 1)
      EventFilter[OversizedPayloadException](start = "Failed to serialize oversized message", occurrences = 1)
        .intercept {
          verifySend(oversized) {
            expectNoMessage(1.second) // No AssociationErrorEvent should be published
          }
        }
      droppedProbe.expectMsgType[Dropped].message should ===(oversized)
    }

    // TODO max payload size is not configurable yet, so we cannot send a too big message, it fails no sending side
    "drop received messages over payload size" ignore {
      // Receiver should reply with a message of size maxPayload + 1, which will be dropped and an error logged
      EventFilter[OversizedPayloadException](pattern = ".*Discarding oversized payload received.*", occurrences = 1)
        .intercept {
          verifySend(maxPayloadBytes + 1) {
            expectNoMessage(1.second) // No AssociationErrorEvent should be published
          }
        }
    }

    "be able to serialize a local actor ref from another actor system" in {
      remoteSystem.actorOf(TestActors.echoActorProps, "echo")
      val local = localSystem.actorOf(TestActors.echoActorProps, "echo")

      val remoteEcho =
        system.actorSelection(rootActorPath(remoteSystem) / "user" / "echo").resolveOne(3.seconds).futureValue
      remoteEcho ! local
      expectMsg(3.seconds, local)
    }

  }

  private def verifySend(msg: Any)(afterSend: => Unit): Unit = {

    val bigBounceId = s"bigBounce-${ThreadLocalRandom.current.nextInt()}"
    val bigBounceOther = remoteSystem.actorOf(Props(new Actor {
      def receive = {
        case x: Int => sender() ! byteStringOfSize(x)
        case x      => sender() ! x
      }
    }), bigBounceId)

    val bigBounceHere =
      RARP(system).provider.resolveActorRef(s"akka://${remoteSystem.name}@localhost:$remotePort/user/$bigBounceId")

    try {
      bigBounceHere ! msg
      afterSend
      expectNoMessage(500.millis)
    } finally {
      bigBounceOther ! PoisonPill
    }
  }

  private def byteStringOfSize(size: Int) = ByteString.fromArray(Array.fill(size)(42: Byte))

}
