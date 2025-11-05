/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._
import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.Dropped
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object ReplicatorGossipSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.cluster.distributed-data {
      # only gossip in this test
      delta-crdt.enabled = off
      gossip-interval = 1s 
      max-delta-elements = 400
      log-data-size-exceeding = 2000
    }
    akka.remote.artery {
      log-frame-size-exceeding = 2000
      advanced.maximum-frame-size = 50000
    }
    """))

}

class ReplicatorGossipSpecMultiJvmNode1 extends ReplicatorGossipSpec
class ReplicatorGossipSpecMultiJvmNode2 extends ReplicatorGossipSpec

class ReplicatorGossipSpec extends MultiNodeSpec(ReplicatorGossipSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import ReplicatorGossipSpec._

  override def initialParticipants: Int = roles.size

  private val cluster = Cluster(system)
  private implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  private val replicator = system.actorOf(Replicator.props(ReplicatorSettings(system)), "replicator")

  private def threeDigits(n: Int): String = s"00$n".takeRight(3)
  private val smallORSetKeys = (1 to 999).map(n => ORSetKey[String](s"small-${threeDigits(n)}")).toVector
  private val largeORSetKeys = (1 to 99).map(n => ORSetKey[String](s"large-${threeDigits(n)}")).toVector

  private val rnd = new Random
  private val smallPayloadSize = 100
  // note that those are full utf-8 strings so can be more than 1 byte per char
  def smallPayload(): String = rnd.nextString(smallPayloadSize)
  private val largePayloadSize = 4000
  def largePayload(): String = rnd.nextString(largePayloadSize)

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Replicator gossip" must {

    "catch up when adding node" in {
      join(first, first)

      val droppedProbe = TestProbe()
      system.eventStream.subscribe(droppedProbe.ref, classOf[Dropped])

      val numberOfSmall = 500
      val numberOfLarge = 15

      runOn(first) {
        (0 until numberOfSmall).foreach { i =>
          replicator ! Update(smallORSetKeys(i), ORSet.empty[String], WriteLocal)(_ :+ smallPayload())
          expectMsgType[UpdateSuccess[_]]
        }
        (0 until numberOfLarge).foreach { i =>
          replicator ! Update(largeORSetKeys(i), ORSet.empty[String], WriteLocal)(_ :+ largePayload())
          expectMsgType[UpdateSuccess[_]]
        }
      }
      enterBarrier("updated-first")

      join(second, first)
      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(2))
        }
      }
      enterBarrier("second-added")

      runOn(second) {
        within(10.seconds) {
          awaitAssert {
            (0 until numberOfSmall).foreach { i =>
              replicator ! Get(smallORSetKeys(i), ReadLocal)
              expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements.head.length should ===(smallPayloadSize)
            }
            (0 until numberOfLarge).foreach { i =>
              replicator ! Get(largeORSetKeys(i), ReadLocal)
              expectMsgType[GetSuccess[ORSet[String]]].dataValue.elements.head.length should ===(largePayloadSize)
            }
          }
        }
      }
      enterBarrier("second-caught-up")

      droppedProbe.expectNoMessage()

      enterBarrier("done-1")
    }
  }

}
