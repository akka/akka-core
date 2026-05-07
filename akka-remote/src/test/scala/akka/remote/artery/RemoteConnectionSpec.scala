/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.duration._

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.remote.RARP
import akka.remote.artery.Association.UidCollisionException
import akka.remote.artery.AssociationState.UidKnown
import akka.remote.artery.AssociationState.UidUnknown
import akka.testkit.{ EventFilter, ImplicitSender, TestActors, TestEvent, TestProbe }

class RemoteConnectionSpec extends ArteryMultiNodeSpec with ImplicitSender {

  def muteSystem(system: ActorSystem): Unit = {
    system.eventStream.publish(
      TestEvent.Mute(
        EventFilter.error(start = "AssociationError"),
        EventFilter.warning(start = "AssociationError"),
        EventFilter.warning(pattern = "received dead letter.*")))
  }

  "Remoting between systems" should {

    "handle uid collision when connection TO two systems with same uid" in {
      val localProbe = new TestProbe(localSystem)
      val localPort = RARP(localSystem).provider.getDefaultAddress.getPort().get

      val conflictUid = localSystem.asInstanceOf[ExtendedActorSystem].uid + 1
      val echoName = "echoA"

      val remotePort1 = freePort()
      val remoteSystem1 =
        newRemoteSystem(extraConfig = Some(s"""
          akka.test-only-uid = $conflictUid
          akka.remote.artery.canonical.port=$remotePort1
          """))
      val remote1Probe = new TestProbe(remoteSystem1)

      val remotePort2 = freePort()
      val remoteSystem2 =
        newRemoteSystem(extraConfig = Some(s"""
          // same uid as remoteSystem1
          akka.test-only-uid = $conflictUid
          akka.remote.artery.canonical.port=$remotePort2
          """))
      val remote2Probe = new TestProbe(remoteSystem2)

      localProbe.expectNoMessage(2.seconds)
      localSystem.actorOf(TestActors.echoActorProps, echoName)
      remoteSystem1.actorOf(TestActors.echoActorProps, echoName)
      remoteSystem2.actorOf(TestActors.echoActorProps, echoName)

      val selectionFromLocalToRemote1 =
        localSystem.actorSelection(s"akka://${remoteSystem1.name}@localhost:$remotePort1/user/$echoName")
      val selectionFromLocalToRemote2 =
        localSystem.actorSelection(s"akka://${remoteSystem2.name}@localhost:$remotePort2/user/$echoName")
      val selectionFromRemote1ToLocal =
        remoteSystem1.actorSelection(s"akka://${localSystem.name}@localhost:$localPort/user/$echoName")
      val selectionFromRemote2ToLocal =
        remoteSystem2.actorSelection(s"akka://${localSystem.name}@localhost:$localPort/user/$echoName")

      selectionFromLocalToRemote1.tell("ping1a", localProbe.ref)
      localProbe.expectMsg("ping1a")

      selectionFromRemote1ToLocal.tell("ping1b", remote1Probe.ref)
      remote1Probe.expectMsg("ping1b")

      EventFilter[UidCollisionException]().intercept {
        selectionFromLocalToRemote2.tell("ping2a", localProbe.ref)
        // doesn't get through
        localProbe.expectNoMessage()
      }(localSystem)
      RARP(localSystem).provider.transport
        .asInstanceOf[ArteryTransport]
        .association(RARP(remoteSystem2).provider.getDefaultAddress)
        .associationState
        .uniqueRemoteAddressState() shouldBe UidUnknown // handshake not completed

      EventFilter[UidCollisionException]().intercept {
        selectionFromRemote2ToLocal.tell("ping2b", remote2Probe.ref)
        // doesn't get through in other direction
        remote2Probe.expectNoMessage()
      }(localSystem)

      awaitAssert {
        RARP(remoteSystem2).provider.transport
          .asInstanceOf[ArteryTransport]
          .association(RARP(localSystem).provider.getDefaultAddress)
          .associationState
          .uniqueRemoteAddressState() shouldBe UidKnown // handshake was completed in other direction
      }

      RARP(localSystem).provider.transport
        .asInstanceOf[ArteryTransport]
        .association(RARP(remoteSystem1).provider.getDefaultAddress)
        .associationState
        .uniqueRemoteAddressState() shouldBe UidKnown // still intact

      // still works
      selectionFromLocalToRemote1.tell("ping1a again", localProbe.ref)
      localProbe.expectMsg("ping1a again")

      // still works in other direction
      selectionFromRemote1ToLocal.tell("ping1b again", remote1Probe.ref)
      remote1Probe.expectMsg("ping1b again")
    }

    "handle uid collision when connection FROM two systems with same uid" in {
      // same kind of test as above, but connection is first established from the other remote systems
      val localProbe = new TestProbe(localSystem)
      val localPort = RARP(localSystem).provider.getDefaultAddress.getPort().get

      val conflictUid = localSystem.asInstanceOf[ExtendedActorSystem].uid + 2
      val echoName = "echoB"

      val remotePort1 = freePort()
      val remoteSystem1 =
        newRemoteSystem(extraConfig = Some(s"""
          akka.test-only-uid = $conflictUid
          akka.remote.artery.canonical.port=$remotePort1
          """))
      val remote1Probe = new TestProbe(remoteSystem1)

      val remotePort2 = freePort()
      val remoteSystem2 =
        newRemoteSystem(extraConfig = Some(s"""
          // same uid as remoteSystem1
          akka.test-only-uid = $conflictUid
          akka.remote.artery.canonical.port=$remotePort2
          """))
      val remote2Probe = new TestProbe(remoteSystem2)

      localProbe.expectNoMessage(2.seconds)
      localSystem.actorOf(TestActors.echoActorProps, echoName)
      remoteSystem1.actorOf(TestActors.echoActorProps, echoName)
      remoteSystem2.actorOf(TestActors.echoActorProps, echoName)

      val selectionFromLocalToRemote1 =
        localSystem.actorSelection(s"akka://${remoteSystem1.name}@localhost:$remotePort1/user/$echoName")
      val selectionFromLocalToRemote2 =
        localSystem.actorSelection(s"akka://${remoteSystem2.name}@localhost:$remotePort2/user/$echoName")
      val selectionFromRemote1ToLocal =
        remoteSystem1.actorSelection(s"akka://${localSystem.name}@localhost:$localPort/user/$echoName")
      val selectionFromRemote2ToLocal =
        remoteSystem2.actorSelection(s"akka://${localSystem.name}@localhost:$localPort/user/$echoName")

      selectionFromRemote1ToLocal.tell("ping1b", remote1Probe.ref)
      remote1Probe.expectMsg("ping1b")

      selectionFromLocalToRemote1.tell("ping1a", localProbe.ref)
      localProbe.expectMsg("ping1a")

      EventFilter[UidCollisionException]().intercept {
        selectionFromRemote2ToLocal.tell("ping2b", remote2Probe.ref)
        // doesn't get through
        localProbe.expectNoMessage()
      }(localSystem)
      RARP(localSystem).provider.transport
        .asInstanceOf[ArteryTransport]
        .association(RARP(remoteSystem2).provider.getDefaultAddress)
        .associationState
        .uniqueRemoteAddressState() shouldBe UidUnknown // handshake not completed

      EventFilter[UidCollisionException]().intercept {
        selectionFromLocalToRemote2.tell("ping2a", localProbe.ref)
        // doesn't get through in other direction
        remote2Probe.expectNoMessage()
      }(localSystem)

      awaitAssert {
        RARP(remoteSystem2).provider.transport
          .asInstanceOf[ArteryTransport]
          .association(RARP(localSystem).provider.getDefaultAddress)
          .associationState
          .uniqueRemoteAddressState() shouldBe UidKnown // handshake was completed in other direction
      }

      RARP(localSystem).provider.transport
        .asInstanceOf[ArteryTransport]
        .association(RARP(remoteSystem1).provider.getDefaultAddress)
        .associationState
        .uniqueRemoteAddressState() shouldBe UidKnown // still intact

      // still works
      selectionFromRemote1ToLocal.tell("ping1b again", remote1Probe.ref)
      remote1Probe.expectMsg("ping1b again")

      // still works in other direction
      selectionFromLocalToRemote1.tell("ping1a again", localProbe.ref)
      localProbe.expectMsg("ping1a again")
    }

    "be able to connect to system even if it's not there at first" in {
      muteSystem(localSystem)
      val localProbe = new TestProbe(localSystem)
      val echoName = "echoC"

      val remotePort = freePort()

      // try to talk to it before it is up
      val selection =
        localSystem.actorSelection(s"akka://$nextGeneratedSystemName@localhost:$remotePort/user/$echoName")
      selection.tell("ping", localProbe.ref)
      localProbe.expectNoMessage(1.seconds)

      // then start the remote system and try again
      val remoteSystem = newRemoteSystem(extraConfig = Some(s"akka.remote.artery.canonical.port=$remotePort"))

      muteSystem(remoteSystem)
      localProbe.expectNoMessage(2.seconds)
      remoteSystem.actorOf(TestActors.echoActorProps, echoName)

      within(5.seconds) {
        awaitAssert {
          selection.tell("ping", localProbe.ref)
          localProbe.expectMsg(500.millis, "ping")
        }
      }
    }

    "allow other system to connect even if it's not there at first" in {
      val localSystem = newRemoteSystem()
      val echoName = "echoD"

      val localPort = port(localSystem)
      muteSystem(localSystem)

      val localProbe = new TestProbe(localSystem)
      localSystem.actorOf(TestActors.echoActorProps, echoName)

      val remotePort = freePort()

      // try to talk to remote before it is up
      val selection =
        localSystem.actorSelection(s"akka://$nextGeneratedSystemName@localhost:$remotePort/user/$echoName")
      selection.tell("ping", localProbe.ref)
      localProbe.expectNoMessage(1.seconds)

      // then when it is up, talk from other system
      val remoteSystem = newRemoteSystem(extraConfig = Some(s"akka.remote.artery.canonical.port=$remotePort"))

      muteSystem(remoteSystem)
      localProbe.expectNoMessage(2.seconds)
      val otherProbe = new TestProbe(remoteSystem)
      val otherSender = otherProbe.ref
      val thisSelection = remoteSystem.actorSelection(s"akka://${localSystem.name}@localhost:$localPort/user/$echoName")
      within(5.seconds) {
        awaitAssert {
          thisSelection.tell("ping", otherSender)
          otherProbe.expectMsg(500.millis, "ping")
        }
      }
    }
  }

}
