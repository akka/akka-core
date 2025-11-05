/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.Direction
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object JepsenInspiredInsertSpec extends MultiNodeConfig {
  val controller = role("controller")
  val n1 = role("n1")
  val n2 = role("n2")
  val n3 = role("n3")
  val n4 = role("n4")
  val n5 = role("n5")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = INFO
    akka.actor.provider = "cluster"
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.testconductor.barrier-timeout = 60 s
    """))

  testTransport(on = true)

}

class JepsenInspiredInsertSpecMultiJvmNode1 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode2 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode3 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode4 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode5 extends JepsenInspiredInsertSpec
class JepsenInspiredInsertSpecMultiJvmNode6 extends JepsenInspiredInsertSpec

class JepsenInspiredInsertSpec
    extends MultiNodeSpec(JepsenInspiredInsertSpec)
    with STMultiNodeSpec
    with ImplicitSender {
  import JepsenInspiredInsertSpec._
  import Replicator._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val replicator = DistributedData(system).replicator
  val nodes = roles.drop(1) // controller not part of active nodes
  val nodeCount = nodes.size
  val timeout = 3.seconds.dilated
  val delayMillis = 0
  val totalCount = 200
  //  val delayMillis = 20
  //  val totalCount = 2000
  val expectedData = (0 until totalCount).toSet
  val data: Map[RoleName, Seq[Int]] = {
    val nodeIndex = nodes.zipWithIndex.map { case (n, i) => i -> n }.toMap
    (0 until totalCount).groupBy(i => nodeIndex(i % nodeCount))
  }
  lazy val myData: Seq[Int] = data(myself)

  def sleepDelay(): Unit =
    if (delayMillis != 0) {
      val rndDelay = ThreadLocalRandom.current().nextInt(delayMillis)
      if (rndDelay != 0) Thread.sleep(delayMillis)
    }

  def sleepBeforePartition(): Unit = {
    if (delayMillis != 0)
      Thread.sleep(delayMillis * totalCount / nodeCount / 10)
  }

  def sleepDuringPartition(): Unit =
    Thread.sleep(math.max(5000, delayMillis * totalCount / nodeCount / 2))

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Insert from 5 nodes" must {

    "setup cluster" in {
      runOn(nodes: _*) {
        nodes.foreach { join(_, n1) }

        within(10.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(nodes.size))
          }
        }
      }

      runOn(controller) {
        nodes.foreach { n =>
          enterBarrier(n.name + "-joined")
        }
      }

      enterBarrier("after-setup")
    }
  }

  "replicate values when all nodes connected" in {
    val key = ORSetKey[Int]("A")
    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData.map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteLocal, Some(i))(_ :+ i), writeProbe.ref)
        writeProbe.receiveOne(3.seconds)
      }
      val successWriteAcks = writeAcks.collect { case success: UpdateSuccess[_] => success }
      val failureWriteAcks = writeAcks.collect { case fail: UpdateFailure[_]    => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadLocal), readProbe.ref)
          val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
          result.elements should be(expectedData)
        }
      }

    }

    enterBarrier("after-test-1")
  }

  "write/read to majority when all nodes connected" in {
    val key = ORSetKey[Int]("B")
    val readMajority = ReadMajority(timeout)
    val writeMajority = WriteMajority(timeout)
    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData.map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), writeMajority, Some(i))(_ :+ i), writeProbe.ref)
        writeProbe.receiveOne(timeout + 1.second)
      }
      val successWriteAcks = writeAcks.collect { case success: UpdateSuccess[_] => success }
      val failureWriteAcks = writeAcks.collect { case fail: UpdateFailure[_]    => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("data-written-2")

      // read from majority of nodes, which is enough to retrieve all data
      val readProbe = TestProbe()
      replicator.tell(Get(key, readMajority), readProbe.ref)
      val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
      //val survivors = result.elements.size
      result.elements should be(expectedData)

    }

    runOn(controller) {
      enterBarrier("data-written-2")
    }

    enterBarrier("after-test-2")
  }

  "replicate values after partition" in {
    val key = ORSetKey[Int]("C")
    runOn(controller) {
      sleepBeforePartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.blackhole(a, b, Direction.Both).await
      sleepDuringPartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.passThrough(a, b, Direction.Both).await
      enterBarrier("partition-healed-3")
    }

    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData.map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), WriteLocal, Some(i))(_ :+ i), writeProbe.ref)
        writeProbe.receiveOne(3.seconds)
      }
      val successWriteAcks = writeAcks.collect { case success: UpdateSuccess[_] => success }
      val failureWriteAcks = writeAcks.collect { case fail: UpdateFailure[_]    => fail }
      successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
      successWriteAcks.size should be(myData.size)
      failureWriteAcks should be(Nil)
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("partition-healed-3")

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadLocal), readProbe.ref)
          val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
          result.elements should be(expectedData)
        }
      }

    }

    enterBarrier("after-test-3")
  }

  "write to majority during 3+2 partition and read from majority after partition" in {
    val key = ORSetKey[Int]("D")
    val readMajority = ReadMajority(timeout)
    val writeMajority = WriteMajority(timeout)
    runOn(controller) {
      sleepBeforePartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.blackhole(a, b, Direction.Both).await
      sleepDuringPartition()
      for (a <- List(n1, n4, n5); b <- List(n2, n3))
        testConductor.passThrough(a, b, Direction.Both).await
      enterBarrier("partition-healed-4")
    }

    runOn(nodes: _*) {
      val writeProbe = TestProbe()
      val writeAcks = myData.map { i =>
        sleepDelay()
        replicator.tell(Update(key, ORSet(), writeMajority, Some(i))(_ :+ i), writeProbe.ref)
        writeProbe.receiveOne(timeout + 1.second)
      }
      val successWriteAcks = writeAcks.collect { case success: UpdateSuccess[_] => success }
      val failureWriteAcks = writeAcks.collect { case fail: UpdateFailure[_]    => fail }
      runOn(n1, n4, n5) {
        successWriteAcks.map(_.request.get).toSet should be(myData.toSet)
        successWriteAcks.size should be(myData.size)
        failureWriteAcks should be(Nil)
      }
      runOn(n2, n3) {
        // without delays all could theoretically have been written before the blackhole
        if (delayMillis != 0)
          failureWriteAcks should not be (Nil)
      }
      (successWriteAcks.size + failureWriteAcks.size) should be(myData.size)

      enterBarrier("partition-healed-4")

      // on the 2 node side, read from majority of nodes is enough to read all writes
      runOn(n2, n3) {
        val readProbe = TestProbe()
        replicator.tell(Get(key, readMajority), readProbe.ref)
        val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
        //val survivors = result.elements.size
        result.elements should be(expectedData)
      }
      // but on the 3 node side, read from majority doesn't mean that we are guaranteed to see
      // the writes from the other side, yet

      // eventually all nodes will have the data
      within(15.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadLocal), readProbe.ref)
          val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
          result.elements should be(expectedData)
        }
      }
    }

    enterBarrier("after-test-4")
  }

}
