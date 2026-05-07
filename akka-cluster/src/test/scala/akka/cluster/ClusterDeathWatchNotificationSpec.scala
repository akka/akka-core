/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.remote.artery.ArteryMultiNodeSpec
import akka.remote.artery.ArterySpecSupport
import akka.testkit._

object ClusterDeathWatchNotificationSpec {

  val config = ConfigFactory.parseString("""
    akka {
        loglevel = INFO
        actor {
            provider = cluster
        }
    }
    akka.remote.artery.canonical.port = 0
    """).withFallback(ArterySpecSupport.defaultConfig)

  object Sender {
    def props(receiver: ActorRef, sendOnStop: Vector[String]): Props =
      Props(new Sender(receiver, sendOnStop))
  }

  class Sender(receiver: ActorRef, sendOnStop: Vector[String]) extends Actor {
    override def receive: Receive = {
      case msg => sender() ! msg
    }

    override def postStop(): Unit = {
      sendOnStop.foreach(receiver ! _)
    }
  }
}

class ClusterDeathWatchNotificationSpec
    extends ArteryMultiNodeSpec(ClusterDeathWatchNotificationSpec.config)
    with ImplicitSender {
  import ClusterDeathWatchNotificationSpec.Sender

  private def system1: ActorSystem = system
  private val system2 = newRemoteSystem(name = Some(system.name))
  private val system3 = newRemoteSystem(name = Some(system.name))
  private val systems = Vector(system1, system2, system3)

  private val messages = (1 to 100).map(_.toString).toVector

  private def setupSender(sys: ActorSystem, receiverProbe: TestProbe, name: String): Unit = {
    val receiverPath = receiverProbe.ref.path.toStringWithAddress(address(system1))
    val otherProbe = TestProbe()(sys)
    sys.actorSelection(receiverPath).tell(Identify(None), otherProbe.ref)
    val receiver = otherProbe.expectMsgType[ActorIdentity](5.seconds).ref.get
    receiver.path.address.hasGlobalScope should ===(true) // should be remote
    sys.actorOf(Sender.props(receiver, messages), name)
  }

  private def identifySender(sys: ActorSystem, name: String): ActorRef = {
    system1.actorSelection(rootActorPath(sys) / "user" / name) ! Identify(None)
    val sender = expectMsgType[ActorIdentity](5.seconds).ref.get
    sender
  }

  "join cluster" in within(10.seconds) {
    systems.foreach { sys =>
      Cluster(sys).join(Cluster(system1).selfAddress)
    }
    awaitAssert {
      systems.foreach { sys =>
        Cluster(sys).state.members.size should ===(systems.size)
        Cluster(sys).state.members.iterator.map(_.status).toSet should ===(Set(MemberStatus.Up))
      }
    }
  }

  // https://github.com/akka/akka-core/issues/30135
  "receive Terminated after ordinary messages" in {
    val receiverProbe = TestProbe()
    setupSender(system2, receiverProbe, "sender")
    val sender = identifySender(system2, "sender")

    receiverProbe.watch(sender)
    // make it likely that the watch has been established
    sender.tell("echo", receiverProbe.ref)
    receiverProbe.expectMsg("echo")

    sender ! PoisonPill
    receiverProbe.receiveN(messages.size).toVector shouldBe messages
    receiverProbe.expectTerminated(sender)
  }

  "receive Terminated after ordinary messages when system is shutdown" in {
    val receiverProbe1 = TestProbe()
    setupSender(system2, receiverProbe1, "sender1")
    val sender1 = identifySender(system2, "sender1")

    val receiverProbe2 = TestProbe()
    setupSender(system2, receiverProbe2, "sender2")
    val sender2 = identifySender(system2, "sender2")

    val receiverProbe3 = TestProbe()
    setupSender(system2, receiverProbe3, "sender3")
    val sender3 = identifySender(system2, "sender3")

    receiverProbe1.watch(sender1)
    receiverProbe2.watch(sender2)
    receiverProbe3.watch(sender3)
    // make it likely that the watch has been established
    sender1.tell("echo1", receiverProbe1.ref)
    receiverProbe1.expectMsg("echo1")
    sender2.tell("echo2", receiverProbe2.ref)
    receiverProbe2.expectMsg("echo2")
    sender3.tell("echo3", receiverProbe3.ref)
    receiverProbe3.expectMsg("echo3")

    system2.log.debug("terminating")
    system2.terminate()
    receiverProbe1.receiveN(messages.size, 5.seconds).toVector shouldBe messages
    receiverProbe1.expectTerminated(sender1)
    receiverProbe2.receiveN(messages.size).toVector shouldBe messages
    receiverProbe2.expectTerminated(sender2)
    receiverProbe3.receiveN(messages.size).toVector shouldBe messages
    receiverProbe3.expectTerminated(sender3)
  }

  "receive Terminated after ordinary messages when system is leaving" in {
    val receiverProbe1 = TestProbe()
    setupSender(system3, receiverProbe1, "sender1")
    val sender1 = identifySender(system3, "sender1")

    val receiverProbe2 = TestProbe()
    setupSender(system3, receiverProbe2, "sender2")
    val sender2 = identifySender(system3, "sender2")

    val receiverProbe3 = TestProbe()
    setupSender(system3, receiverProbe3, "sender3")
    val sender3 = identifySender(system3, "sender3")

    receiverProbe1.watch(sender1)
    receiverProbe2.watch(sender2)
    receiverProbe3.watch(sender3)
    // make it likely that the watch has been established
    sender1.tell("echo1", receiverProbe1.ref)
    receiverProbe1.expectMsg("echo1")
    sender2.tell("echo2", receiverProbe2.ref)
    receiverProbe2.expectMsg("echo2")
    sender3.tell("echo3", receiverProbe3.ref)
    receiverProbe3.expectMsg("echo3")

    system3.log.debug("leaving")
    Cluster(system1).leave(Cluster(system3).selfAddress)

    receiverProbe1.receiveN(messages.size, 5.seconds).toVector shouldBe messages
    receiverProbe1.expectTerminated(sender1)
    receiverProbe2.receiveN(messages.size).toVector shouldBe messages
    receiverProbe2.expectTerminated(sender2)
    receiverProbe3.receiveN(messages.size).toVector shouldBe messages
    receiverProbe3.expectTerminated(sender3)
  }

}
