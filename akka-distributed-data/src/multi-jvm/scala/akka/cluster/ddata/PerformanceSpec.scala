/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Deploy
import akka.actor.Props
import akka.cluster.Cluster
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.testkit._

object PerformanceSpec extends MultiNodeConfig {
  val n1 = role("n1")
  val n2 = role("n2")
  val n3 = role("n3")
  val n4 = role("n4")
  val n5 = role("n5")

  commonConfig(ConfigFactory.parseString(s"""
    akka.loglevel = ERROR
    akka.stdout-loglevel = ERROR
    akka.loggers = ["akka.event.Logging$$DefaultLogger"]
    akka.actor.provider = "cluster"
    akka.log-dead-letters = off
    akka.log-dead-letters-during-shutdown = off
    akka.remote.artery.log-frame-size-exceeding=1000b
    akka.testconductor.barrier-timeout = 60 s
    akka.cluster.distributed-data.gossip-interval = 1 s

    #akka.cluster.distributed-data.durable.keys = ["*"]
    #akka.cluster.distributed-data.durable.lmdb.dir = target/PerformanceSpec-${System.currentTimeMillis}-ddata
    #akka.cluster.distributed-data.durable.lmdb.write-behind-interval = 200ms

    #akka.cluster.distributed-data.delta-crdt.enabled = off
    """))

  def countDownProps(latch: TestLatch): Props = Props(new CountDown(latch)).withDeploy(Deploy.local)

  class CountDown(latch: TestLatch) extends Actor {
    def receive = {
      case _ =>
        latch.countDown()
        if (latch.isOpen)
          context.stop(self)
    }
  }

}

class PerformanceSpecMultiJvmNode1 extends PerformanceSpec
class PerformanceSpecMultiJvmNode2 extends PerformanceSpec
class PerformanceSpecMultiJvmNode3 extends PerformanceSpec
class PerformanceSpecMultiJvmNode4 extends PerformanceSpec
class PerformanceSpecMultiJvmNode5 extends PerformanceSpec

class PerformanceSpec extends MultiNodeSpec(PerformanceSpec) with STMultiNodeSpec with ImplicitSender {
  import PerformanceSpec._
  import Replicator._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val replicator = DistributedData(system).replicator
  val timeout = 3.seconds.dilated
  val factor = 1 // use 3 here for serious tuning
  val repeatCount = 3 // use at least 10 here for serious tuning

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  def repeat(
      description: String,
      keys: Iterable[ORSetKey[Int]],
      n: Int,
      expectedAfterReplication: Option[Set[Int]] = None,
      oneByOne: Boolean = false)(
      block: (ORSetKey[Int], Int, ActorRef) => Unit,
      afterEachKey: ORSetKey[Int] => Unit = _ => ()): Unit = {

    keys.foreach { key =>
      val startTime = System.nanoTime()
      runOn(n1) {
        val latch = TestLatch(n)
        val oneByOneProbe = TestProbe()
        val replyTo =
          if (oneByOne) oneByOneProbe.ref
          else system.actorOf(countDownProps(latch))

        var i = 0
        while (i < n) {
          block(key, i, replyTo)
          i += 1
          if (oneByOne)
            oneByOneProbe.receiveOne(timeout)
        }
        if (!oneByOne)
          Await.ready(latch, 10.seconds + (2.second * factor))
      }
      expectedAfterReplication.foreach { expected =>
        enterBarrier("repeat-" + key + "-before-awaitReplicated")
        awaitReplicated(key, expected)
        enterBarrier("repeat-" + key + "-after-awaitReplicated")
      }
      runOn(n1) {
        val endTime = System.nanoTime()
        val durationMs = (endTime - startTime).nanos.toMillis
        val tps = (n * 1000.0 / durationMs).toInt
        println(s"## $n $description took $durationMs ms, $tps TPS")
      }

      afterEachKey(key)
      enterBarrier("repeat-" + key + "-done")
    }
  }

  def awaitReplicated(keys: Iterable[ORSetKey[Int]], expectedData: Set[Int]): Unit =
    keys.foreach { key =>
      awaitReplicated(key, expectedData)
    }

  def awaitReplicated(key: ORSetKey[Int], expectedData: Set[Int]): Unit = {
    within(20.seconds) {
      awaitAssert {
        val readProbe = TestProbe()
        replicator.tell(Get(key, ReadLocal), readProbe.ref)
        val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
        result.elements should be(expectedData)
      }
    }
  }

  "Performance" must {

    "setup cluster" taggedAs PerformanceTest in {
      roles.foreach { join(_, n1) }

      within(10.seconds) {
        awaitAssert {
          replicator ! GetReplicaCount
          expectMsg(ReplicaCount(roles.size))
        }
      }

      enterBarrier("after-setup")
    }

    "be great for ORSet Update WriteLocal" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("A" + n))
      val n = 1000 * factor
      val expectedData = (0 until n).toSet
      repeat("ORSet Update WriteLocal", keys, n)({ (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ :+ i), replyTo)
      }, key => awaitReplicated(key, expectedData))

      enterBarrier("after-1")
    }

    "be blazingly fast for ORSet Get ReadLocal" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("A" + n))
      repeat("Get ReadLocal", keys, 100000 * factor) { (key, _, replyTo) =>
        replicator.tell(Get(key, ReadLocal), replyTo)
      }
      enterBarrier("after-2")
    }

    "be good for ORSet Update WriteLocal and gossip replication" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("B" + n))
      val n = 200 * factor
      val expected = Some((0 until n).toSet)
      repeat("ORSet Update WriteLocal + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ :+ i), replyTo)
      }
      enterBarrier("after-3")
    }

    "be good for ORSet Update WriteLocal and gossip of existing keys" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("B" + n))
      val n = 200 * factor
      val expected = Some((0 until n).toSet ++ (0 until n).map(-_).toSet)
      repeat("ORSet Update WriteLocal existing + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), WriteLocal)(_ :+ (-i)), replyTo)
      }
      enterBarrier("after-4")
    }

    "be good for ORSet Update WriteTwo and gossip replication" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("C" + n))
      val n = 200 * factor
      val expected = Some((0 until n).toSet)
      val writeTwo = WriteTo(2, timeout)
      repeat("ORSet Update WriteTwo + gossip", keys, n, expected) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), writeTwo)(_ :+ i), replyTo)
      }
      enterBarrier("after-5")
    }

    "be awesome for GCounter Update WriteLocal" taggedAs PerformanceTest in {
      val startTime = System.nanoTime()
      val n = 1000 * factor
      val key = GCounterKey("D")
      runOn(n1, n2, n3) {
        val latch = TestLatch(n)
        val replyTo = system.actorOf(countDownProps(latch))
        for (_ <- 0 until n)
          replicator.tell(Update(key, GCounter(), WriteLocal)(_ :+ 1), replyTo)
        Await.ready(latch, 5.seconds + (1.second * factor))
        enterBarrier("update-done-6")
        runOn(n1) {
          val endTime = System.nanoTime()
          val durationMs = (endTime - startTime).nanos.toMillis
          val tps = (3 * n * 1000.0 / durationMs).toInt
          println(s"## ${3 * n} GCounter Update took $durationMs ms, $tps TPS")
        }
      }
      runOn(n4, n5) {
        enterBarrier("update-done-6")
      }

      within(20.seconds) {
        awaitAssert {
          val readProbe = TestProbe()
          replicator.tell(Get(key, ReadLocal), readProbe.ref)
          val result = readProbe.expectMsgPF() { case g @ GetSuccess(`key`, _) => g.get(key) }
          result.value should be(3 * n)
        }
      }
      enterBarrier("replication-done-6")
      runOn(n1) {
        val endTime = System.nanoTime()
        val durationMs = (endTime - startTime).nanos.toMillis
        val tps = (n * 1000.0 / durationMs).toInt
        println(s"## $n GCounter Update + gossip took $durationMs ms, $tps TPS")
      }

      enterBarrier("after-6")
    }

    "be good for ORSet one-by-one deltas" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("E" + n))
      val n = 300 * factor
      val writeMajority = WriteMajority(timeout)
      repeat("ORSet Update one-by-one deltas", keys, n, oneByOne = true) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), writeMajority)(_ :+ i), replyTo)
      }
      enterBarrier("after-7")
    }

    "be good for ORSet deltas" taggedAs PerformanceTest in {
      val keys = (1 to repeatCount).map(n => ORSetKey[Int]("F" + n))
      val n = 200 * factor
      val writeMajority = WriteMajority(timeout)
      repeat("ORSet Update deltas", keys, n, oneByOne = false) { (key, i, replyTo) =>
        replicator.tell(Update(key, ORSet(), writeMajority)(_ :+ i), replyTo)
      }
      enterBarrier("after-8")
    }

  }

}
