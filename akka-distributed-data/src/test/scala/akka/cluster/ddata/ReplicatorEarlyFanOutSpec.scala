/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberDowned
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberExited
import akka.cluster.ClusterEvent.MemberLeft
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.cluster.TestMember
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.Replicator.Internal._
import akka.testkit._

object ReplicatorEarlyFanOutSpec {
  val KeyA = GSetKey[String]("A")

  /**
   * Stands in for the Replicator of another node: it is started with the same name as the Replicator
   * under test, so that it receives what that Replicator's aggregators send to this node.
   */
  class ReplicaStub(probe: ActorRef) extends Actor {
    def receive = {
      case msg @ (_: Write | _: Read) => probe.forward(msg)
      case _                          => // gossip
    }
  }

  class DurableStoreStub(probe: ActorRef) extends Actor {
    def receive = {
      case DurableStore.LoadAll => sender() ! DurableStore.LoadAllCompleted
      case msg                  => probe.forward(msg)
    }
  }
}

/**
 * Sends the Replicator the cluster events directly, while its aggregators communicate over remoting with
 * stubs on other actor systems. The timeouts are long enough that the fallback timer doesn't fire during a
 * test, so that a secondary being contacted can only have been caused by the cluster event.
 */
class ReplicatorEarlyFanOutSpec extends AkkaSpec("""
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0
      akka.cluster.jmx.multi-mbeans-in-same-jvm = on
      """) with ImplicitSender {
  import ReplicatorEarlyFanOutSpec._

  private val consistencyTimeout = 30.seconds.dilated
  // well before the fallback timer at consistencyTimeout / 5
  private val beforeFallbackTimer = consistencyTimeout / 10

  private val writeTo2 = WriteTo(2, consistencyTimeout)
  private val readFrom2 = ReadFrom(2, consistencyTimeout)

  private val primarySystem = ActorSystem(system.name, system.settings.config)
  private val secondarySystem = ActorSystem(system.name, system.settings.config)

  private val primaryAddress = Cluster(primarySystem).selfUniqueAddress
  private val secondaryAddress = Cluster(secondarySystem).selfUniqueAddress

  private var replicatorCounter = 0

  override protected def afterTermination(): Unit = {
    shutdown(primarySystem)
    shutdown(secondarySystem)
  }

  private def member(node: UniqueAddress, status: MemberStatus, upNumber: Int): Member =
    TestMember.withUniqueAddress(node, status, Set.empty, "default", upNumber)

  private def primaryMember(status: MemberStatus): Member = member(primaryAddress, status, upNumber = 1)
  private def secondaryMember(status: MemberStatus): Member = member(secondaryAddress, status, upNumber = 2)

  private class Setup(configure: ReplicatorSettings => ReplicatorSettings = identity) {
    replicatorCounter += 1
    private val name = s"replicator-$replicatorCounter"

    val primary = TestProbe()(primarySystem)
    val secondary = TestProbe()(secondarySystem)
    primarySystem.actorOf(Props(new ReplicaStub(primary.ref)), name)
    secondarySystem.actorOf(Props(new ReplicaStub(secondary.ref)), name)

    // With prefer-oldest the order of the nodes is deterministic. Two other nodes and a consistency of WriteTo(2)
    // or ReadFrom(2) gives one primary (the oldest) and one secondary.
    val replicator = system.actorOf(
      Replicator.props(configure(ReplicatorSettings(system).withPreferOldest(true).withDeltaCrdtEnabled(false))),
      name)
    replicator ! MemberUp(primaryMember(MemberStatus.Up))
    replicator ! MemberUp(secondaryMember(MemberStatus.Up))

    def startWrite(): Unit = {
      replicator ! Update(KeyA, GSet.empty[String], writeTo2)(_ + "a")
      primary.expectMsgType[Write]
    }

    def startRead(): Unit = {
      replicator ! Get(KeyA, readFrom2)
      primary.expectMsgType[Read]
    }
  }

  private val primaryLeavingEvents: List[(String, MemberEvent)] = List(
    "MemberLeft" -> MemberLeft(primaryMember(MemberStatus.Leaving)),
    "MemberExited" -> MemberExited(primaryMember(MemberStatus.Exiting)),
    "MemberDowned" -> MemberDowned(primaryMember(MemberStatus.Down)))

  private val primaryLessLikelyToAckEvents: List[(String, MemberEvent)] =
    primaryLeavingEvents :+
    ("MemberRemoved" -> MemberRemoved(primaryMember(MemberStatus.Removed), MemberStatus.Exiting))

  "Replicator" must {
    primaryLessLikelyToAckEvents.foreach {
      case (eventName, event) =>
        s"send writes to secondaries before the fallback timer on $eventName for a primary" in new Setup {
          startWrite()

          replicator ! event

          secondary.expectMsgType[Write](beforeFallbackTimer)
          secondary.reply(WriteAck)
          expectMsg(UpdateSuccess(KeyA, None))
        }

        s"send reads to secondaries before the fallback timer on $eventName for a primary" in new Setup {
          startRead()

          replicator ! event

          secondary.expectMsgType[Read](beforeFallbackTimer)
          secondary.reply(ReadResult(None))
          expectMsg(NotFound(KeyA, None))
        }
    }

    primaryLeavingEvents.foreach {
      case (eventName, event) =>
        s"not use a node as a primary for later writes after $eventName" in new Setup {
          replicator ! event

          replicator ! Update(KeyA, GSet.empty[String], writeTo2)(_ + "a")

          secondary.expectMsgType[Write]
          primary.expectNoMessage(200.millis)
          secondary.reply(WriteAck)
          expectMsg(UpdateSuccess(KeyA, None))
        }

        s"not use a node as a primary for later reads after $eventName" in new Setup {
          replicator ! event

          replicator ! Get(KeyA, readFrom2)

          secondary.expectMsgType[Read]
          primary.expectNoMessage(200.millis)
          secondary.reply(ReadResult(None))
          expectMsg(NotFound(KeyA, None))
        }
    }

    "send later writes to secondaries at startup when every primary candidate is leaving" in new Setup {
      replicator ! MemberLeft(primaryMember(MemberStatus.Leaving))
      replicator ! MemberLeft(secondaryMember(MemberStatus.Leaving))

      startWrite()

      secondary.expectMsgType[Write](beforeFallbackTimer)
      secondary.reply(WriteAck)
      expectMsg(UpdateSuccess(KeyA, None))
    }

    "send later reads to secondaries at startup when every primary candidate is leaving" in new Setup {
      replicator ! MemberLeft(primaryMember(MemberStatus.Leaving))
      replicator ! MemberLeft(secondaryMember(MemberStatus.Leaving))

      startRead()

      secondary.expectMsgType[Read](beforeFallbackTimer)
      secondary.reply(ReadResult(None))
      expectMsg(NotFound(KeyA, None))
    }

    "not send to secondaries on a member event for a node which isn't a primary" in new Setup {
      startWrite()

      replicator ! MemberExited(secondaryMember(MemberStatus.Exiting))

      secondary.expectNoMessage(200.millis)
      primary.reply(WriteAck)
      expectMsg(UpdateSuccess(KeyA, None))
    }

    "not send member events to the durable store" in {
      val durableStoreProbe = TestProbe()
      new Setup(
        _.withDurableKeys(Set("durable-*")).withDurableStoreProps(Props(new DurableStoreStub(durableStoreProbe.ref)))) {
        startWrite()

        replicator ! MemberExited(primaryMember(MemberStatus.Exiting))

        // the aggregator was notified
        secondary.expectMsgType[Write](beforeFallbackTimer)
        durableStoreProbe.expectNoMessage(200.millis)
      }
    }
  }
}
