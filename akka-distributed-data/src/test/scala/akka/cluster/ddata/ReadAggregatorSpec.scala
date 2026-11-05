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
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.cluster.TestMember
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator._
import akka.cluster.ddata.Replicator.Internal._
import akka.testkit._

object ReadAggregatorSpec {

  val KeyA = GSetKey[String]("A")

  def readAggregatorProps(
      consistency: Replicator.ReadConsistency,
      probes: Map[UniqueAddress, ActorRef],
      selfUniqueAddress: UniqueAddress,
      nodes: Vector[UniqueAddress],
      localValue: Option[DataEnvelope],
      replyTo: ActorRef): Props =
    Props(new TestReadAggregator(consistency, probes, selfUniqueAddress, nodes, localValue, replyTo))

  class TestReadAggregator(
      consistency: Replicator.ReadConsistency,
      probes: Map[UniqueAddress, ActorRef],
      selfUniqueAddress: UniqueAddress,
      nodes: Vector[UniqueAddress],
      localValue: Option[DataEnvelope],
      replyTo: ActorRef)
      extends ReadAggregator(
        KeyA,
        consistency,
        None,
        selfUniqueAddress,
        nodes,
        Set.empty,
        shuffle = false,
        localValue,
        replyTo) {

    override def replica(address: UniqueAddress): ActorSelection =
      context.actorSelection(probes(address).path)

    override def senderAddress(): Address =
      probes.find { case (_, r) => r == sender() }.get._1.address
  }

  def readResultAdapterProps(replica: ActorRef): Props =
    Props(new ReadResultAdapter(replica))

  class ReadResultAdapter(replica: ActorRef) extends Actor {
    var aggregator: Option[ActorRef] = None

    def receive = {
      case result: ReadResult =>
        aggregator.foreach(_ ! result)
      case msg: MemberLessLikelyToAck =>
        aggregator.foreach(_ ! msg)
      case msg =>
        aggregator = Some(sender())
        replica ! msg
    }
  }

  class TestMock(_application: ActorSystem) extends TestProbe(_application) {
    val readResultAdapter = system.actorOf(ReadAggregatorSpec.readResultAdapterProps(this.ref))
  }

  def parentProps(aggregatorProps: Props, readRepairProbe: ActorRef): Props =
    Props(new ReadAggregatorParent(aggregatorProps, readRepairProbe))

  class ReadAggregatorParent(aggregatorProps: Props, readRepairProbe: ActorRef) extends Actor {
    val aggregator = context.watch(context.actorOf(aggregatorProps))

    def receive = {
      case repair: ReadRepair =>
        readRepairProbe.tell(repair, sender())
      case Terminated(`aggregator`) =>
        context.stop(self)
      case msg =>
        aggregator.forward(msg)
    }
  }
}

class ReadAggregatorSpec extends AkkaSpec(s"""
      akka.actor.provider = "cluster"
      akka.remote.artery.canonical.port = 0
      """) with ImplicitSender {
  import ReadAggregatorSpec._

  val protocol = "akka"

  val nodeA = UniqueAddress(Address(protocol, "Sys", "a", 2552), 17L)
  val nodeB = UniqueAddress(Address(protocol, "Sys", "b", 2552), 17L)
  val nodeC = UniqueAddress(Address(protocol, "Sys", "c", 2552), 17L)
  val nodeD = UniqueAddress(Address(protocol, "Sys", "d", 2552), 17L)
  // 4 replicas + the local => 5
  val nodes = Vector(nodeA, nodeB, nodeC, nodeD)

  val data = GSet.empty + "A" + "B"
  val timeout = 3.seconds.dilated
  val readMajority = ReadMajority(timeout)

  val selfUniqueAddress: UniqueAddress = Cluster(system).selfUniqueAddress

  def member(node: UniqueAddress) =
    TestMember.withUniqueAddress(node, MemberStatus.Leaving, Set.empty, "default")

  def probes(): Map[UniqueAddress, TestMock] =
    nodes.iterator.map(_ -> new TestMock(system)).toMap

  def aggregator(
      testProbes: Map[UniqueAddress, TestMock],
      readRepairProbe: ActorRef,
      localValue: Option[DataEnvelope] = Some(DataEnvelope(data))): ActorRef = {
    val probeRefs = testProbes.map { case (address, probe) => address -> probe.readResultAdapter }
    val props =
      ReadAggregatorSpec.readAggregatorProps(readMajority, probeRefs, selfUniqueAddress, nodes, localValue, testActor)
    system.actorOf(ReadAggregatorSpec.parentProps(props, readRepairProbe))
  }

  def completeReadRepair(readRepairProbe: TestProbe): Unit = {
    readRepairProbe.expectMsgType[ReadRepair]
    expectNoMessage(100.millis)
    readRepairProbe.lastSender ! ReadRepairAck
  }

  "ReadAggregator" must {
    "send to secondaries immediately when an unacknowledged primary is leaving" in {
      val testProbes = probes()
      val readRepairProbe = TestProbe()
      val aggr = aggregator(testProbes, readRepairProbe.ref)

      testProbes(nodeA).expectMsgType[Read]
      testProbes(nodeB).expectMsgType[Read]

      testProbes(nodeA).readResultAdapter ! MemberLessLikelyToAck(member(nodeA))

      testProbes(nodeC).expectMsgType[Read]
      testProbes(nodeD).expectMsgType[Read]

      val updatedData = data + "C"
      testProbes(nodeC).lastSender ! ReadResult(Some(DataEnvelope(updatedData)))
      expectNoMessage(100.millis)

      // A late response from the leaving primary still counts toward the original quorum.
      testProbes(nodeA).lastSender ! ReadResult(Some(DataEnvelope(data)))
      completeReadRepair(readRepairProbe)
      expectMsg(GetSuccess(KeyA, None)(updatedData))
      watch(aggr)
      expectTerminated(aggr)
    }

    "not send to secondaries when the leaving primary has already replied" in {
      val testProbes = probes()
      val readRepairProbe = TestProbe()
      val aggr = aggregator(testProbes, readRepairProbe.ref)

      testProbes(nodeA).expectMsgType[Read]
      testProbes(nodeA).lastSender ! ReadResult(Some(DataEnvelope(data)))
      testProbes(nodeB).expectMsgType[Read]

      testProbes(nodeA).readResultAdapter ! MemberLessLikelyToAck(member(nodeA))

      import system.dispatcher
      Future.sequence {
        Seq(Future { testProbes(nodeC).expectNoMessage(100.millis) }, Future {
          testProbes(nodeD).expectNoMessage(100.millis)
        })
      }.futureValue

      testProbes(nodeB).lastSender ! ReadResult(Some(DataEnvelope(data)))
      completeReadRepair(readRepairProbe)
      expectMsg(GetSuccess(KeyA, None)(data))
      watch(aggr)
      expectTerminated(aggr)
    }

    "not send to secondaries when a node outside the primary set is leaving" in {
      val testProbes = probes()
      val readRepairProbe = TestProbe()
      val aggr = aggregator(testProbes, readRepairProbe.ref)

      testProbes(nodeA).expectMsgType[Read]
      testProbes(nodeB).expectMsgType[Read]

      aggr ! MemberLessLikelyToAck(member(nodeC))

      import system.dispatcher
      Future.sequence {
        Seq(Future { testProbes(nodeC).expectNoMessage(100.millis) }, Future {
          testProbes(nodeD).expectNoMessage(100.millis)
        })
      }.futureValue

      testProbes(nodeA).lastSender ! ReadResult(Some(DataEnvelope(data)))
      testProbes(nodeB).lastSender ! ReadResult(Some(DataEnvelope(data)))
      completeReadRepair(readRepairProbe)
      expectMsg(GetSuccess(KeyA, None)(data))
      watch(aggr)
      expectTerminated(aggr)
    }

    "send to secondaries only once for repeated membership events and the fallback timer" in {
      val testProbes = probes()
      val readRepairProbe = TestProbe()
      val aggr = aggregator(testProbes, readRepairProbe.ref)

      testProbes(nodeA).expectMsgType[Read]
      testProbes(nodeB).expectMsgType[Read]

      val leaving = MemberLessLikelyToAck(member(nodeA))
      aggr ! leaving
      testProbes(nodeC).expectMsgType[Read]
      testProbes(nodeD).expectMsgType[Read]

      aggr ! leaving
      testProbes(nodeC).expectNoMessage(timeout / 5 + 200.millis.dilated)
      testProbes(nodeD).expectNoMessage(100.millis)

      testProbes(nodeA).lastSender ! ReadResult(Some(DataEnvelope(data)))
      testProbes(nodeB).lastSender ! ReadResult(Some(DataEnvelope(data)))
      completeReadRepair(readRepairProbe)
      expectMsg(GetSuccess(KeyA, None)(data))
      watch(aggr)
      expectTerminated(aggr)
    }

    "reply with NotFound without read repair when the quorum has no value" in {
      val testProbes = probes()
      val readRepairProbe = TestProbe()
      val aggr = aggregator(testProbes, readRepairProbe.ref, localValue = None)

      testProbes(nodeA).expectMsgType[Read]
      testProbes(nodeB).expectMsgType[Read]
      testProbes(nodeA).lastSender ! ReadResult(None)
      testProbes(nodeB).lastSender ! ReadResult(None)

      expectMsg(NotFound(KeyA, None))
      readRepairProbe.expectNoMessage(100.millis)
      watch(aggr)
      expectTerminated(aggr)
    }
  }
}
