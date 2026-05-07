/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.nio.file.Paths
import java.util.concurrent.TimeUnit.NANOSECONDS
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.HdrHistogram.Histogram

import akka.actor._
import akka.cluster.MemberStatus
import akka.cluster.sharding.ShardRegion.{ CurrentShardRegionState, GetShardRegionState, Passivate }
import akka.testkit._

object ClusterShardingRememberEntitiesPerfSpec {
  val NrRegions = 6
  // use 5 for "real" testing
  val NrIterations = 2
  // use 5 for "real" testing
  val NrOfMessagesFactor = 1

  case class In(id: Long, created: Long = System.currentTimeMillis())
  case class Out(latency: Long)

  class LatencyEntity extends Actor with ActorLogging {

    override def receive: Receive = {
      case In(_, created) =>
        sender() ! Out(System.currentTimeMillis() - created)
      case _: Stop =>
//        log.debug("Stop received {}", self.path.name)
        context.parent ! Passivate("stop")
      case "stop" =>
//        log.debug("Final Stop received {}", self.path.name)
        context.stop(self)
      case msg => throw new RuntimeException("unexpected msg " + msg)
    }
  }

  object LatencyEntity {

    val extractEntityId: ShardRegion.ExtractEntityId = {
      case in: In         => (in.id.toString, in)
      case msg @ Stop(id) => (id.toString, msg)
    }

    val extractShardId: ShardRegion.ExtractShardId = _ => "0"
  }

  case class Stop(id: Int)

}

object ClusterShardingRememberEntitiesPerfSpecConfig
    extends MultiNodeClusterShardingConfig(
      rememberEntities = true,
      additionalConfig = """
    akka.loglevel = DEBUG 
    akka.testconductor.barrier-timeout = 3 minutes
    akka.remote.artery.advanced.outbound-message-queue-size = 10000
    akka.remote.artery.advanced.maximum-frame-size = 512 KiB
    # comment next line to enable durable lmdb storage
    akka.cluster.sharding.distributed-data.durable.keys = []
    akka.cluster.sharding {
      remember-entities = on
    }
    """) {

  val first = role("first")
  val second = role("second")
  val third = role("third")

  nodeConfig(third)(ConfigFactory.parseString(s"""
    akka.cluster.sharding.distributed-data.durable.lmdb {
      # use same directory when starting new node on third (not used at same time)
      dir = "$targetDir/sharding-third"
    }
    """))

}

class ClusterShardingRememberEntitiesPerfSpecMultiJvmNode1 extends ClusterShardingRememberEntitiesPerfSpec
class ClusterShardingRememberEntitiesPerfSpecMultiJvmNode2 extends ClusterShardingRememberEntitiesPerfSpec
class ClusterShardingRememberEntitiesPerfSpecMultiJvmNode3 extends ClusterShardingRememberEntitiesPerfSpec

abstract class ClusterShardingRememberEntitiesPerfSpec
    extends MultiNodeClusterShardingSpec(ClusterShardingRememberEntitiesPerfSpecConfig)
    with ImplicitSender {
  import ClusterShardingRememberEntitiesPerfSpec._
  import ClusterShardingRememberEntitiesPerfSpecConfig._

  def startSharding(): Unit = {
    (1 to NrRegions).foreach { n =>
      startSharding(
        system,
        typeName = s"EntityLatency$n",
        entityProps = Props(new LatencyEntity()),
        extractEntityId = LatencyEntity.extractEntityId,
        extractShardId = LatencyEntity.extractShardId)
    }

  }

  var latencyRegions = Vector.empty[ActorRef]

  val latencyCount = new AtomicInteger(0)

  override protected def atStartup(): Unit = {
    super.atStartup()
    join(first, first)

    startSharding()

    // this will make it run on first
    runOn(first) {
      latencyRegions = (1 to NrRegions).map { n =>
        val region = ClusterSharding(system).shardRegion(s"EntityLatency$n")
        region ! In(0)
        expectMsgType[Out]
        region
      }.toVector
    }
    enterBarrier("allocated-on-first")

    join(second, first)
    join(third, first)

    within(20.seconds) {
      awaitAssert {
        cluster.state.members.size should ===(3)
        cluster.state.members.unsorted.map(_.status) should ===(Set(MemberStatus.Up))
      }
    }

    enterBarrier("all-up")
  }

  "Cluster sharding with remember entities performance" must {

    val percentiles = List(99.9, 99.0, 95.0, 50.0)

    def runBench(name: String)(logic: (Int, ActorRef, Histogram) => Long): Unit = {
      val testRun = latencyCount.getAndIncrement()
      runOn(first) {
        val recording = new FlightRecording(system)
        recording.start()
        val region = latencyRegions(testRun)
        val fullHistogram = new Histogram(10L * 1000L, 2)
        val throughputs = (1 to NrIterations).map { iteration =>
          val histogram: Histogram = new Histogram(10L * 1000L, 2)
          val startTime = System.nanoTime()
          val numberOfMessages = logic(iteration, region, histogram)
          val took = NANOSECONDS.toMillis(System.nanoTime - startTime)
          val throughput = numberOfMessages * 1000.0 / took
          println(
            s"### Test [${name}] stop with $numberOfMessages took $took ms, " +
            f"throughput $throughput%,.0f msg/s")
          //          println("Iteration Latencies: ")
          //          histogram.outputPercentileDistribution(System.out, 1.0)
          fullHistogram.add(histogram)
          throughput
        }
        println(f"Average throughput: ${throughputs.sum / NrIterations}%,.0f msg/s")
        println("Combined latency figures:")
        println(s"total ${fullHistogram.getTotalCount} max ${fullHistogram.getMaxValue} ${percentiles
          .map(p => s"$p% ${fullHistogram.getValueAtPercentile(p)}ms")
          .mkString(" ")}")
        recording.endAndDump(Paths.get("target", s"${name.replace(" ", "-")}.jfr"))
      }
      enterBarrier(s"after-start-stop-${testRun}")
    }

    "test when starting new entity" in {
      val numberOfMessages = 200 * NrOfMessagesFactor
      runBench("start new entities") { (iteration, region, histogram) =>
        (1 to numberOfMessages).foreach { n =>
          region ! In(iteration * 100000 + n)
        }
        for (_ <- 1 to numberOfMessages) {
          histogram.recordValue(expectMsgType[Out].latency)
        }
        numberOfMessages
      }
    }

    "test latency when starting new entity and sending a few messages" in {
      val numberOfMessages = 800 * NrOfMessagesFactor
      runBench("start, few messages") { (iteration, region, histogram) =>
        for (n <- 1 to numberOfMessages / 5; _ <- 1 to 5) {
          region ! In(iteration * 100000 + n)
        }
        for (_ <- 1 to numberOfMessages) {
          histogram.recordValue(expectMsgType[Out].latency)
        }
        numberOfMessages
      }
    }

    "test latency when starting new entity and sending a few messages to it and stopping" in {
      val numberOfMessages = 800 * NrOfMessagesFactor
      // 160 entities, and an extra one for the intialization
      // all but the first one are not removed
      runBench("start, few messages, stop") { (iteration, region, histogram) =>
        for (n <- 1 to numberOfMessages / 5; m <- 1 to 5) {
          val id = iteration * 100000 + n
          region ! In(id)
          if (m == 5) {
            region ! Stop(id)
          }
        }
        for (_ <- 1 to numberOfMessages) {
          try {
            histogram.recordValue(expectMsgType[Out].latency)
          } catch {
            case e: AssertionError =>
              log.error(s"Received ${histogram.getTotalCount} out of $numberOfMessages")
              throw e
          }
        }

        awaitAssert({
          val probe = TestProbe()
          region.tell(GetShardRegionState, probe.ref)
          val stats = probe.expectMsgType[CurrentShardRegionState]
          stats.shards.head.shardId shouldEqual "0"
          stats.shards.head.entityIds.toList.sorted shouldEqual List("0") // the init entity
        }, 2.seconds)

        numberOfMessages
      }
    }

    "test latency when starting, few messages, stopping, few messages" in {
      val numberOfMessages = 800 * NrOfMessagesFactor
      runBench("start, few messages, stop, few messages") { (iteration, region, histogram) =>
        for (n <- 1 to numberOfMessages / 5; m <- 1 to 5) {
          val id = iteration * 100000 + n
          region ! In(id)
          if (m == 2) {
            region ! Stop(id)
          }
        }
        for (_ <- 1 to numberOfMessages) {
          try {
            histogram.recordValue(expectMsgType[Out].latency)
          } catch {
            case e: AssertionError =>
              log.error(s"Received ${histogram.getTotalCount} out of $numberOfMessages")
              throw e
          }
        }
        numberOfMessages
      }
    }

    "test when starting some new entities mixed with sending to started" in {
      runBench("starting mixed with sending to started") { (iteration, region, histogram) =>
        val numberOfMessages = 1600 * NrOfMessagesFactor
        (1 to numberOfMessages).foreach { n =>
          val msg =
            if (n % 20 == 0)
              -(iteration * 100000 + n) // unique, will start new entity
            else
              iteration * 100000 + (n % 10) // these will go to same 10 started entities
          region ! In(msg)

          if (n == 10) {
            for (_ <- 1 to 10) {
              histogram.recordValue(expectMsgType[Out].latency)
            }
          }
        }
        for (_ <- 1 to numberOfMessages - 10) {
          histogram.recordValue(expectMsgType[Out].latency)
        }
        numberOfMessages
      }
    }

    "test sending to started" in {
      runBench("sending to started") { (iteration, region, histogram) =>
        val numberOfMessages = 1600 * NrOfMessagesFactor
        (1 to numberOfMessages).foreach { n =>
          region ! In(iteration * 100000 + (n % 10)) // these will go to same 10 started entities

          if (n == 10) {
            for (_ <- 1 to 10) {
              histogram.recordValue(expectMsgType[Out].latency)
            }
          }
        }
        for (_ <- 1 to numberOfMessages - 10) {
          histogram.recordValue(expectMsgType[Out].latency)
        }
        numberOfMessages
      }
    }
  }
}
