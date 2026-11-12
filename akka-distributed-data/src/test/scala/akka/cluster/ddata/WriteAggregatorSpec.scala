/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Props
import akka.actor.SuppressedDeadLetter
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.TestMember
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.Replicator.Internal._
import akka.testkit._

object WriteAggregatorSpec {

  val KeyA = GSetKey[String]("A")
  val KeyB = ORSetKey[String]("B")

  def writeAggregatorProps(
      data: GSet[String],
      consistency: Replicator.WriteConsistency,
      probes: Map[UniqueAddress, ActorRef],
      selfUniqueAddress: UniqueAddress,
      nodes: Vector[UniqueAddress],
      unreachable: Set[UniqueAddress],
      replyTo: ActorRef,
      durable: Boolean,
      lessLikelyToAck: Set[UniqueAddress] = Set.empty): Props =
    Props(
      new TestWriteAggregator(
        KeyA,
        data,
        None,
        consistency,
        probes,
        selfUniqueAddress,
        nodes,
        unreachable,
        replyTo,
        durable,
        lessLikelyToAck))

  def writeAggregatorPropsWithDelta(
      data: ORSet[String],
      delta: Delta,
      consistency: Replicator.WriteConsistency,
      probes: Map[UniqueAddress, ActorRef],
      selfUniqueAddress: UniqueAddress,
      nodes: Vector[UniqueAddress],
      unreachable: Set[UniqueAddress],
      replyTo: ActorRef,
      durable: Boolean,
      lessLikelyToAck: Set[UniqueAddress] = Set.empty): Props =
    Props(
      new TestWriteAggregator(
        KeyB,
        data,
        Some(delta),
        consistency,
        probes,
        selfUniqueAddress,
        nodes,
        unreachable,
        replyTo,
        durable,
        lessLikelyToAck))

  class TestWriteAggregator(
      key: Key.KeyR,
      data: ReplicatedData,
      delta: Option[Delta],
      consistency: Replicator.WriteConsistency,
      probes: Map[UniqueAddress, ActorRef],
      selfUniqueAddress: UniqueAddress,
      nodes: Vector[UniqueAddress],
      unreachable: Set[UniqueAddress],
      replyTo: ActorRef,
      durable: Boolean,
      lessLikelyToAck: Set[UniqueAddress])
      extends WriteAggregator(
        key,
        DataEnvelope(data),
        delta,
        consistency,
        None,
        selfUniqueAddress,
        nodes,
        unreachable,
        lessLikelyToAck,
        shuffle = false,
        replyTo,
        durable) {

    override def replica(address: UniqueAddress): ActorSelection =
      context.actorSelection(probes(address).path)

    override def senderAddress(): Address =
      probes.find { case (_, r) => r == sender() }.get._1.address
  }

  def writeAckAdapterProps(replica: ActorRef): Props =
    Props(new WriteAckAdapter(replica))

  class WriteAckAdapter(replica: ActorRef) extends Actor {
    var replicator: Option[ActorRef] = None

    def receive = {
      case WriteAck =>
        replicator.foreach(_ ! WriteAck)
      case WriteNack =>
        replicator.foreach(_ ! WriteNack)
      case DeltaNack =>
        replicator.foreach(_ ! DeltaNack)
      case msg: MemberLessLikelyToAck =>
        replicator.foreach(_ ! msg)
      case msg =>
        replicator = Some(sender())
        replica ! msg
    }
  }

  object TestMock {
    def apply()(implicit system: ActorSystem) = new TestMock(system)
  }
  class TestMock(_application: ActorSystem) extends TestProbe(_application) {
    val writeAckAdapter = system.actorOf(WriteAggregatorSpec.writeAckAdapterProps(this.ref))
  }
}

class WriteAggregatorSpec extends AkkaSpec(s"""
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0
      akka.cluster.distributed-data.durable.lmdb {
        dir = target/WriteAggregatorSpec-${System.currentTimeMillis}-ddata
        map-size = 10 MiB
      }
      """) with ImplicitSender {
  import WriteAggregatorSpec._

  val protocol = "akka"

  val nodeA = UniqueAddress(Address(protocol, "Sys", "a", 2552), 17L)
  val nodeB = UniqueAddress(Address(protocol, "Sys", "b", 2552), 17L)
  val nodeC = UniqueAddress(Address(protocol, "Sys", "c", 2552), 17L)
  val nodeD = UniqueAddress(Address(protocol, "Sys", "d", 2552), 17L)
  // 4 replicas + the local => 5
  val nodes = Vector(nodeA, nodeB, nodeC, nodeD)

  val data = GSet.empty + "A" + "B"
  val timeout = 3.seconds.dilated
  val writeThree = WriteTo(3, timeout)
  val writeMajority = WriteMajority(timeout)
  val writeAll = WriteAll(timeout)

  val selfUniqueAddress: UniqueAddress = Cluster(system).selfUniqueAddress

  def probes(probe: ActorRef): Map[UniqueAddress, ActorRef] =
    nodes.toSeq.map(_ -> system.actorOf(WriteAggregatorSpec.writeAckAdapterProps(probe))).toMap

  /**
   * Create a tuple for each node with the WriteAckAdapter and the TestProbe
   */
  def probes(): Map[UniqueAddress, TestMock] = {
    nodes.toSeq.map(_ -> TestMock()).toMap
  }

  def member(node: UniqueAddress) =
    TestMember.withUniqueAddress(node, MemberStatus.Leaving, Set.empty, "default")

  "WriteAggregator" must {
    "send to at least N/2+1 replicas when WriteMajority" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send to more when no immediate reply" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set(nodeC, nodeD),
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      // no reply
      testProbes(nodeB).expectMsgType[Write]
      testProbes(nodeB).lastSender ! WriteAck
      // Make sure that unreachable nodes do not get a message until 1/5 of the time the reachable nodes did not answer
      val t = timeout / 5 - 50.milliseconds.dilated
      import system.dispatcher
      Future.sequence {
        Seq(Future { testProbes(nodeC).expectNoMessage(t) }, Future { testProbes(nodeD).expectNoMessage(t) })
      }.futureValue
      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeC).lastSender ! WriteAck
      testProbes(nodeD).expectMsgType[Write]
      testProbes(nodeD).lastSender ! WriteAck

      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "timeout when less than required acks" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      probe.expectMsgType[Write]
      // no reply
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      // no reply
      probe.expectMsgType[Write]
      // no reply
      expectMsg(UpdateTimeout(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send to secondaries immediately when an unacknowledged primary is leaving" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeB).expectMsgType[Write]

      testProbes(nodeA).writeAckAdapter ! MemberLessLikelyToAck(member(nodeA))

      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeD).expectMsgType[Write]

      // One acknowledgement is not enough, and a late acknowledgement from the leaving primary still counts.
      testProbes(nodeC).lastSender ! WriteAck
      expectNoMessage(100.millis)
      testProbes(nodeA).lastSender ! WriteAck

      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "not send to secondaries when the leaving primary has already acknowledged" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeA).lastSender ! WriteAck
      testProbes(nodeB).expectMsgType[Write]

      testProbes(nodeA).writeAckAdapter ! MemberLessLikelyToAck(member(nodeA))

      import system.dispatcher
      Future.sequence {
        Seq(Future { testProbes(nodeC).expectNoMessage(100.millis) }, Future {
          testProbes(nodeD).expectNoMessage(100.millis)
        })
      }.futureValue

      testProbes(nodeB).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "not send to secondaries when a node outside the primary set is leaving" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeB).expectMsgType[Write]

      aggr ! MemberLessLikelyToAck(member(nodeC))

      import system.dispatcher
      Future.sequence {
        Seq(Future { testProbes(nodeC).expectNoMessage(100.millis) }, Future {
          testProbes(nodeD).expectNoMessage(100.millis)
        })
      }.futureValue

      testProbes(nodeA).lastSender ! WriteAck
      testProbes(nodeB).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send to secondaries only once for repeated membership events and the fallback timer" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeB).expectMsgType[Write]

      val leaving = MemberLessLikelyToAck(member(nodeA))
      aggr ! leaving
      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeD).expectMsgType[Write]

      aggr ! leaving
      testProbes(nodeC).expectNoMessage(timeout / 5 + 200.millis.dilated)
      testProbes(nodeD).expectNoMessage(100.millis)

      testProbes(nodeA).lastSender ! WriteAck
      testProbes(nodeB).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "not use a node known to be leaving as a primary when there are enough other nodes" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false,
          lessLikelyToAck = Set(nodeA)))

      testProbes(nodeB).expectMsgType[Write]
      testProbes(nodeC).expectMsgType[Write]

      // neither are primaries, and no primary is leaving, so nothing sent to the secondaries
      testProbes(nodeA).expectNoMessage(100.millis)
      testProbes(nodeD).expectNoMessage(100.millis)

      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeC).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send to secondaries at startup when a node known to be leaving has to be a primary" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false,
          lessLikelyToAck = Set(nodeA, nodeB, nodeC)))

      // primaries: nodeD and nodeA, secondaries: nodeB and nodeC
      testProbes(nodeD).expectMsgType[Write]
      testProbes(nodeA).expectMsgType[Write]
      // well before the fallback timer at timeout / 5
      testProbes(nodeB).expectMsgType[Write](timeout / 10)
      testProbes(nodeC).expectMsgType[Write](timeout / 10)

      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeD).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "prefer a reachable node known to be leaving over an unreachable node as a primary" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      // 3 primaries (nodeB, nodeC, nodeA) and the secondary nodeD, which is sent to since nodeA is leaving
      // (if nodeD were a primary, then nodeA would be a secondary which wouldn't be sent to before the fallback timer)
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          WriteTo(4, timeout),
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set(nodeD),
          testActor,
          durable = false,
          lessLikelyToAck = Set(nodeA)))

      testProbes(nodeB).expectMsgType[Write]
      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeA).expectMsgType[Write](timeout / 10)
      testProbes(nodeD).expectMsgType[Write](timeout / 10)

      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeC).lastSender ! WriteAck
      testProbes(nodeA).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "suppress the dead letter when a member event arrives after it has stopped" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeB).expectMsgType[Write]
      testProbes(nodeA).lastSender ! WriteAck
      testProbes(nodeB).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)

      val deadLetterProbe = TestProbe()
      system.eventStream.subscribe(deadLetterProbe.ref, classOf[SuppressedDeadLetter])
      val lateEvent = MemberLessLikelyToAck(member(nodeA))
      aggr ! lateEvent
      deadLetterProbe.fishForSpecificMessage() {
        case SuppressedDeadLetter(`lateEvent`, _, _) => ()
      }
      system.eventStream.unsubscribe(deadLetterProbe.ref)
    }

    "calculate majority with minCap" in {
      val minCap = 5

      import ReadWriteAggregator._

      calculateMajority(minCap, 3, 0) should be(3)
      calculateMajority(minCap, 4, 0) should be(4)
      calculateMajority(minCap, 5, 0) should be(5)
      calculateMajority(minCap, 6, 0) should be(5)
      calculateMajority(minCap, 7, 0) should be(5)
      calculateMajority(minCap, 8, 0) should be(5)
      calculateMajority(minCap, 9, 0) should be(5)
      calculateMajority(minCap, 10, 0) should be(6)
      calculateMajority(minCap, 11, 0) should be(6)
      calculateMajority(minCap, 12, 0) should be(7)
    }

    "calculate majority with additional" in {
      import ReadWriteAggregator._

      calculateMajority(0, 3, 1) should be(3)
      calculateMajority(0, 3, 2) should be(3)
      calculateMajority(0, 4, 1) should be(4)
      calculateMajority(0, 5, 1) should be(4)
      calculateMajority(0, 5, 2) should be(5)
      calculateMajority(0, 6, 1) should be(5)
      calculateMajority(0, 7, 1) should be(5)
      calculateMajority(0, 8, 1) should be(6)
      calculateMajority(0, 8, 2) should be(7)
      calculateMajority(0, 9, 1) should be(6)
      calculateMajority(0, 10, 1) should be(7)
      calculateMajority(0, 11, 1) should be(7)
      calculateMajority(0, 11, 3) should be(9)
    }

    "calculate majority with additional and minCap" in {
      import ReadWriteAggregator._

      calculateMajority(5, 9, 1) should be(6)
      calculateMajority(7, 9, 1) should be(7)
      calculateMajority(10, 9, 1) should be(9)
    }
  }

  "WriteAggregator with delta" must {
    implicit val node = DistributedData(system).selfUniqueAddress
    val fullState1 = ORSet.empty[String] :+ "a" :+ "b"
    val fullState2 = fullState1.resetDelta :+ "c"
    val delta = Delta(DataEnvelope(fullState2.delta.get), 2L, 2L)

    "send deltas first" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeMajority,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      probe.expectMsgType[DeltaPropagation]
      probe.lastSender ! WriteAck
      probe.expectMsgType[DeltaPropagation]
      probe.lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send full state to secondaries when a primary is leaving" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[DeltaPropagation]
      testProbes(nodeB).expectMsgType[DeltaPropagation]

      aggr ! MemberLessLikelyToAck(member(nodeA))

      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeD).expectMsgType[Write]

      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeC).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send deltas to the replacement primaries when a node known to be leaving isn't a primary" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false,
          lessLikelyToAck = Set(nodeA)))

      testProbes(nodeB).expectMsgType[DeltaPropagation]
      testProbes(nodeC).expectMsgType[DeltaPropagation]
      testProbes(nodeA).expectNoMessage(100.millis)
      testProbes(nodeD).expectNoMessage(100.millis)

      // a replacement primary which has missed earlier deltas gets the full state
      testProbes(nodeC).lastSender ! DeltaNack
      testProbes(nodeC).expectMsgType[Write]

      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeC).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "still retry with full state to unacknowledged primaries after sending early to secondaries" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeMajority,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[DeltaPropagation]
      testProbes(nodeB).expectMsgType[DeltaPropagation]

      aggr ! MemberLessLikelyToAck(member(nodeA))

      testProbes(nodeC).expectMsgType[Write]
      testProbes(nodeD).expectMsgType[Write]

      // not enough for the quorum, and nodeB shouldn't be included in the retry
      testProbes(nodeB).lastSender ! WriteAck

      // from the fallback timer
      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeB).expectNoMessage(100.millis)
      testProbes(nodeC).expectNoMessage(100.millis)
      testProbes(nodeD).expectNoMessage(100.millis)

      testProbes(nodeA).lastSender ! WriteAck
      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "retry with full state when no immediate reply or nack" in {
      val testProbes = probes()
      val testProbeRefs = testProbes.map { case (a, tm) => a -> tm.writeAckAdapter }
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeAll,
          testProbeRefs,
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      testProbes(nodeA).expectMsgType[DeltaPropagation]
      // no reply
      testProbes(nodeB).expectMsgType[DeltaPropagation]
      testProbes(nodeB).lastSender ! WriteAck
      testProbes(nodeC).expectMsgType[DeltaPropagation]
      testProbes(nodeC).lastSender ! WriteAck
      testProbes(nodeD).expectMsgType[DeltaPropagation]
      testProbes(nodeD).lastSender ! DeltaNack

      // here is the second round
      testProbes(nodeA).expectMsgType[Write]
      testProbes(nodeA).lastSender ! WriteAck
      testProbes(nodeD).expectMsgType[Write]
      testProbes(nodeD).lastSender ! WriteAck
      testProbes(nodeB).expectNoMessage(100.millis)
      testProbes(nodeC).expectNoMessage(100.millis)

      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "timeout when less than required acks" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorPropsWithDelta(
          fullState2,
          delta,
          writeAll,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = false))

      probe.expectMsgType[DeltaPropagation]
      // no reply
      probe.expectMsgType[DeltaPropagation]
      probe.lastSender ! WriteAck
      probe.expectMsgType[DeltaPropagation]
      // no reply
      probe.expectMsgType[DeltaPropagation]
      // nack
      probe.lastSender ! DeltaNack
      // the nack will triggger an immediate Write
      probe.expectMsgType[Write]
      // no reply

      // only 1 ack so we expect 3 full state Write
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.expectMsgType[Write]

      // still not enough acks
      expectMsg(UpdateTimeout(WriteAggregatorSpec.KeyB, None))
      watch(aggr)
      expectTerminated(aggr)
    }
  }

  "Durable WriteAggregator" must {
    "not reply before local confirmation" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeThree,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = true))

      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      expectNoMessage(200.millis)

      // the local write
      aggr ! UpdateSuccess(WriteAggregatorSpec.KeyA, None)

      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "tolerate WriteNack if enough WriteAck" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeThree,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = true))

      aggr ! UpdateSuccess(WriteAggregatorSpec.KeyA, None) // the local write
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck

      expectMsg(UpdateSuccess(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "reply with StoreFailure when too many nacks" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = true))

      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack
      aggr ! UpdateSuccess(WriteAggregatorSpec.KeyA, None) // the local write
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack
      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack

      expectMsg(StoreFailure(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }

    "timeout when less than required acks" in {
      val probe = TestProbe()
      val aggr = system.actorOf(
        WriteAggregatorSpec.writeAggregatorProps(
          data,
          writeMajority,
          probes(probe.ref),
          selfUniqueAddress,
          nodes,
          Set.empty,
          testActor,
          durable = true))

      probe.expectMsgType[Write]
      // no reply
      probe.expectMsgType[Write]
      probe.lastSender ! WriteAck
      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack
      probe.expectMsgType[Write]
      probe.lastSender ! WriteNack

      expectMsg(UpdateTimeout(WriteAggregatorSpec.KeyA, None))
      watch(aggr)
      expectTerminated(aggr)
    }
  }

}
