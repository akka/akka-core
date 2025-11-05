/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.ThreadLocalRandom

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.Address
import akka.remote.DefaultFailureDetectorRegistry
import akka.remote.FailureDetector

object ClusterHeartbeatSenderStateSpec {
  class FailureDetectorStub extends FailureDetector {

    sealed trait Status
    object Up extends Status
    object Down extends Status
    object Unknown extends Status

    private var status: Status = Unknown

    def markNodeAsUnavailable(): Unit = status = Down

    def markNodeAsAvailable(): Unit = status = Up

    override def isAvailable: Boolean = status match {
      case Unknown | Up => true
      case Down         => false
    }

    override def isMonitoring: Boolean = status != Unknown

    override def heartbeat(): Unit = status = Up

  }
}

class ClusterHeartbeatSenderStateSpec extends AnyWordSpec with Matchers {
  import ClusterHeartbeatSenderStateSpec._

  val aa = UniqueAddress(Address("akka", "sys", "aa", 2552), 1L)
  val bb = UniqueAddress(Address("akka", "sys", "bb", 2552), 2L)
  val cc = UniqueAddress(Address("akka", "sys", "cc", 2552), 3L)
  val dd = UniqueAddress(Address("akka", "sys", "dd", 2552), 4L)
  val ee = UniqueAddress(Address("akka", "sys", "ee", 2552), 5L)

  private def emptyState: ClusterHeartbeatSenderState = emptyState(aa)

  private def emptyState(selfUniqueAddress: UniqueAddress) =
    ClusterHeartbeatSenderState(
      ring = HeartbeatNodeRing(selfUniqueAddress, Set(selfUniqueAddress), Set.empty, monitoredByNrOfMembers = 3),
      oldReceiversNowUnreachable = Set.empty[UniqueAddress],
      failureDetector = new DefaultFailureDetectorRegistry[Address](() => new FailureDetectorStub))

  private def fd(state: ClusterHeartbeatSenderState, node: UniqueAddress): FailureDetectorStub =
    state.failureDetector
      .asInstanceOf[DefaultFailureDetectorRegistry[Address]]
      .failureDetector(node.address)
      .get
      .asInstanceOf[FailureDetectorStub]

  "A ClusterHeartbeatSenderState" must {

    "return empty active set when no nodes" in {
      emptyState.activeReceivers.isEmpty should ===(true)
    }

    "init with empty" in {
      emptyState.init(Set.empty, Set.empty).activeReceivers should ===(Set.empty)
    }

    "init with self" in {
      emptyState.init(Set(aa, bb, cc), Set.empty).activeReceivers should ===(Set(bb, cc))
    }

    "init without self" in {
      emptyState.init(Set(bb, cc), Set.empty).activeReceivers should ===(Set(bb, cc))
    }

    "use added members" in {
      emptyState.addMember(bb).addMember(cc).activeReceivers should ===(Set(bb, cc))
    }

    "use added members also when unreachable" in {
      emptyState.addMember(bb).addMember(cc).unreachableMember(bb).activeReceivers should ===(Set(bb, cc))
    }

    "not use removed members" in {
      emptyState.addMember(bb).addMember(cc).removeMember(bb).activeReceivers should ===(Set(cc))
    }

    "use specified number of members" in {
      // they are sorted by the hash (uid) of the UniqueAddress
      emptyState.addMember(cc).addMember(dd).addMember(bb).addMember(ee).activeReceivers should ===(Set(bb, cc, dd))
    }

    "use specified number of members + unreachable" in {
      // they are sorted by the hash (uid) of the UniqueAddress
      emptyState
        .addMember(cc)
        .addMember(dd)
        .addMember(bb)
        .addMember(ee)
        .unreachableMember(cc)
        .activeReceivers should ===(Set(bb, cc, dd, ee))
    }

    "update failure detector in active set" in {
      val s1 = emptyState.addMember(bb).addMember(cc).addMember(dd)
      val s2 = s1.heartbeatRsp(bb).heartbeatRsp(cc).heartbeatRsp(dd).heartbeatRsp(ee)
      s2.failureDetector.isMonitoring(bb.address) should ===(true)
      s2.failureDetector.isMonitoring(cc.address) should ===(true)
      s2.failureDetector.isMonitoring(dd.address) should ===(true)
      s2.failureDetector.isMonitoring(ee.address) should ===(false)
    }

    "continue to use unreachable" in {
      val s1 = emptyState.addMember(cc).addMember(dd).addMember(ee)
      val s2 = s1.heartbeatRsp(cc).heartbeatRsp(dd).heartbeatRsp(ee)
      fd(s2, ee).markNodeAsUnavailable()
      s2.failureDetector.isAvailable(ee.address) should ===(false)
      s2.addMember(bb).activeReceivers should ===(Set(bb, cc, dd, ee))
    }

    "remove unreachable when coming back" in {
      val s1 = emptyState.addMember(cc).addMember(dd).addMember(ee)
      val s2 = s1.heartbeatRsp(cc).heartbeatRsp(dd).heartbeatRsp(ee)
      fd(s2, dd).markNodeAsUnavailable()
      fd(s2, ee).markNodeAsUnavailable()
      val s3 = s2.addMember(bb)
      s3.activeReceivers should ===(Set(bb, cc, dd, ee))
      val s4 = s3.heartbeatRsp(bb).heartbeatRsp(cc).heartbeatRsp(dd).heartbeatRsp(ee)
      s4.activeReceivers should ===(Set(bb, cc, dd))
      s4.failureDetector.isMonitoring(ee.address) should ===(false)
    }

    "remove unreachable when member removed" in {
      val s1 = emptyState.addMember(cc).addMember(dd).addMember(ee)
      val s2 = s1.heartbeatRsp(cc).heartbeatRsp(dd).heartbeatRsp(ee)
      fd(s2, cc).markNodeAsUnavailable()
      fd(s2, ee).markNodeAsUnavailable()
      val s3 = s2.addMember(bb).heartbeatRsp(bb)
      s3.activeReceivers should ===(Set(bb, cc, dd, ee))
      val s4 = s3.removeMember(cc).removeMember(ee)
      s4.activeReceivers should ===(Set(bb, dd))
      s4.failureDetector.isMonitoring(cc.address) should ===(false)
      s4.failureDetector.isMonitoring(ee.address) should ===(false)
    }

    "behave correctly for random operations" in {
      val rnd = ThreadLocalRandom.current
      val nodes =
        (1 to rnd.nextInt(10, 200)).map(n => UniqueAddress(Address("akka", "sys", "n" + n, 2552), n.toLong)).toVector
      def rndNode() = nodes(rnd.nextInt(0, nodes.size))
      val selfUniqueAddress = rndNode()
      var state = emptyState(selfUniqueAddress)
      val Add = 0
      val Remove = 1
      val Unreachable = 2
      val HeartbeatRsp = 3
      for (i <- 1 to 100000) {
        val operation = rnd.nextInt(Add, HeartbeatRsp + 1)
        val node = rndNode()
        try {
          operation match {
            case Add =>
              if (node != selfUniqueAddress && !state.ring.nodes.contains(node)) {
                val oldUnreachable = state.oldReceiversNowUnreachable
                state = state.addMember(node)
                // keep unreachable
                (oldUnreachable.diff(state.activeReceivers)) should ===(Set.empty)
                state.failureDetector.isMonitoring(node.address) should ===(false)
                state.failureDetector.isAvailable(node.address) should ===(true)
              }

            case Remove =>
              if (node != selfUniqueAddress && state.ring.nodes.contains(node)) {
                val oldUnreachable = state.oldReceiversNowUnreachable
                state = state.removeMember(node)
                // keep unreachable, unless it was the removed
                if (oldUnreachable(node))(oldUnreachable.diff(state.activeReceivers)) should ===(Set(node))
                else
                  (oldUnreachable.diff(state.activeReceivers)) should ===(Set.empty)

                state.failureDetector.isMonitoring(node.address) should ===(false)
                state.failureDetector.isAvailable(node.address) should ===(true)
                state.activeReceivers should not contain (node)
              }

            case Unreachable =>
              if (node != selfUniqueAddress && state.activeReceivers(node)) {
                state.failureDetector.heartbeat(node.address) // make sure the fd is created
                fd(state, node).markNodeAsUnavailable()
                state.failureDetector.isMonitoring(node.address) should ===(true)
                state.failureDetector.isAvailable(node.address) should ===(false)
                state = state.unreachableMember(node)
              }

            case HeartbeatRsp =>
              if (node != selfUniqueAddress && state.ring.nodes.contains(node)) {
                val oldUnreachable = state.oldReceiversNowUnreachable
                val oldRingReceivers = state.ring.myReceivers
                state = state.heartbeatRsp(node)

                if (oldUnreachable(node))
                  state.oldReceiversNowUnreachable should not contain (node)

                if (oldUnreachable(node) && !oldRingReceivers(node))
                  state.failureDetector.isMonitoring(node.address) should ===(false)

                if (oldRingReceivers(node))
                  state.failureDetector.isMonitoring(node.address) should ===(true)

                state.ring.myReceivers should ===(oldRingReceivers)
                state.failureDetector.isAvailable(node.address) should ===(true)

              }

            case _ => throw new RuntimeException()
          }
        } catch {
          case e: Throwable =>
            println(
              s"Failure context: i=$i, node=$node, op=$operation, " +
              s"oldReceiversNowUnreachable=${state.oldReceiversNowUnreachable}, " +
              s"ringReceivers=${state.ring.myReceivers}, ringNodes=${state.ring.nodes}")
            throw e
        }
      }

    }

  }
}
