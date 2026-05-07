/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span
import org.scalatest.time.Span.convertSpanToDuration

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.remote.UniqueAddress
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestProbe

class OutboundIdleShutdownSpec
    extends ArteryMultiNodeSpec("""
  akka.loglevel=INFO
  akka.remote.artery.advanced.stop-idle-outbound-after = 1 s
  akka.remote.artery.advanced.connection-timeout = 2 s
  akka.remote.artery.advanced.remove-quarantined-association-after = 1 s
  akka.remote.artery.advanced.compression {
    actor-refs.advertisement-interval = 5 seconds
  }
  """)
    with ImplicitSender
    with Eventually {

  override implicit val patience: PatienceConfig = {
    import akka.testkit.TestDuration
    PatienceConfig(testKitSettings.DefaultTimeout.duration.dilated * 2, Span(200, org.scalatest.time.Millis))
  }

  private def isArteryTcp: Boolean =
    RARP(system).provider.transport.asInstanceOf[ArteryTransport].settings.Transport == ArterySettings.Tcp

  private def assertStreamActive(association: Association, queueIndex: Int, expected: Boolean): Unit = {
    if (queueIndex == Association.ControlQueueIndex) {
      // the control stream is not stopped, but for TCP the connection is closed
      if (expected)
        association.isStreamActive(queueIndex) shouldBe expected
      else if (isArteryTcp && !association.isRemovedAfterQuarantined()) {
        association.associationState.controlIdleKillSwitch.isDefined shouldBe expected
      }
    } else {
      association.isStreamActive(queueIndex) shouldBe expected
    }

  }

  private def futureUniqueRemoteAddress(association: Association): Future[UniqueAddress] = {
    val p = Promise[UniqueAddress]()
    association.associationState.addUniqueRemoteAddressListener(a => p.success(a))
    p.future
  }

  "Outbound streams" should {

    "be stopped when they are idle" in withAssociation { (_, remoteAddress, _, localArtery, _) =>
      val association = localArtery.association(remoteAddress)
      withClue("When initiating a connection, both the control and ordinary streams are opened") {
        assertStreamActive(association, Association.ControlQueueIndex, expected = true)
        assertStreamActive(association, Association.OrdinaryQueueIndex, expected = true)
      }

      eventually {
        assertStreamActive(association, Association.ControlQueueIndex, expected = false)
        assertStreamActive(association, Association.OrdinaryQueueIndex, expected = false)
      }
    }

    "still be resumable after they have been stopped" in withAssociation {
      (_, remoteAddress, remoteEcho, localArtery, localProbe) =>
        val firstAssociation = localArtery.association(remoteAddress)

        eventually {
          assertStreamActive(firstAssociation, Association.ControlQueueIndex, expected = false)
          assertStreamActive(firstAssociation, Association.OrdinaryQueueIndex, expected = false)
        }

        withClue("re-initiating the connection should be the same as starting it the first time") {

          eventually {
            remoteEcho.tell("ping", localProbe.ref)
            localProbe.expectMsg("ping")
            val secondAssociation = localArtery.association(remoteAddress)
            assertStreamActive(secondAssociation, Association.ControlQueueIndex, expected = true)
            assertStreamActive(secondAssociation, Association.OrdinaryQueueIndex, expected = true)
          }

        }
    }

    "eliminate quarantined association when not used" in withAssociation { (_, remoteAddress, _, localArtery, _) =>
      val association = localArtery.association(remoteAddress)
      withClue("When initiating a connection, both the control and ordinary streams are opened") {
        assertStreamActive(association, Association.ControlQueueIndex, expected = true)
        assertStreamActive(association, Association.OrdinaryQueueIndex, expected = true)
      }

      val remoteUid = futureUniqueRemoteAddress(association).futureValue.uid

      localArtery.quarantine(remoteAddress, Some(remoteUid), "Test")

      eventually {
        assertStreamActive(association, Association.ControlQueueIndex, expected = false)
        assertStreamActive(association, Association.OrdinaryQueueIndex, expected = false)
      }

      // the outbound streams are inactive and association quarantined, then it's completely removed
      eventually {
        localArtery.remoteAddresses should not contain remoteAddress
      }
    }

    "remove inbound compression after quarantine" in withAssociation { (_, remoteAddress, _, localArtery, _) =>
      val association = localArtery.association(remoteAddress)
      val remoteUid = futureUniqueRemoteAddress(association).futureValue.uid

      localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

      eventually {
        assertStreamActive(association, Association.OrdinaryQueueIndex, expected = false)
      }
      // compression still exists when idle
      localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

      localArtery.quarantine(remoteAddress, Some(remoteUid), "Test")
      // after quarantine it should be removed
      eventually {
        localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should not contain remoteUid
      }
    }

    "remove inbound compression after restart with same host:port" in withAssociation {
      (remoteSystem, remoteAddress, _, localArtery, localProbe) =>
        val association = localArtery.association(remoteAddress)
        val remoteUid = futureUniqueRemoteAddress(association).futureValue.uid

        localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid)

        shutdown(remoteSystem, verifySystemShutdown = true)

        val remoteSystem2 = newRemoteSystem(
          Some(s"""
          akka.remote.artery.canonical.hostname = ${remoteAddress.host.get}
          akka.remote.artery.canonical.port = ${remoteAddress.port.get}
          """),
          name = Some(remoteAddress.system))
        try {

          remoteSystem2.actorOf(TestActors.echoActorProps, "echo2")

          def remoteEcho = system.actorSelection(RootActorPath(remoteAddress) / "user" / "echo2")

          val echoRef = eventually {
            remoteEcho.resolveOne(1.seconds).futureValue
          }

          echoRef.tell("ping2", localProbe.ref)
          localProbe.expectMsg("ping2")

          val association2 = localArtery.association(remoteAddress)
          val remoteUid2 = futureUniqueRemoteAddress(association2).futureValue.uid

          remoteUid2 should !==(remoteUid)

          eventually {
            localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should contain(remoteUid2)
          }
          eventually {
            localArtery.inboundCompressionAccess.get.currentCompressionOriginUids.futureValue should not contain remoteUid
          }
        } finally {
          shutdown(remoteSystem2)
        }
    }

    /**
     * Test setup fixture:
     * 1. A 'remote' ActorSystem is created to spawn an Echo actor,
     * 2. A TestProbe is spawned locally to initiate communication with the Echo actor
     * 3. Details (remoteAddress, remoteEcho, localArtery, localProbe) are supplied to the test
     */
    def withAssociation(test: (ActorSystem, Address, ActorRef, ArteryTransport, TestProbe) => Any): Unit = {
      val remoteSystem = newRemoteSystem()
      try {
        remoteSystem.actorOf(TestActors.echoActorProps, "echo")
        val remoteAddress = RARP(remoteSystem).provider.getDefaultAddress

        def remoteEcho = system.actorSelection(RootActorPath(remoteAddress) / "user" / "echo")

        // use little less resolve timeout than the patience timeout for good error messages
        val echoRef = remoteEcho.resolveOne(convertSpanToDuration(patience.timeout) - 200.millis).futureValue
        val localProbe = new TestProbe(localSystem)

        echoRef.tell("ping", localProbe.ref)
        localProbe.expectMsg("ping")

        val artery = RARP(system).provider.transport.asInstanceOf[ArteryTransport]

        test(remoteSystem, remoteAddress, echoRef, artery, localProbe)

      } finally {
        shutdown(remoteSystem)
      }
    }
  }
}
