/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils

import akka.actor.{ Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props }
import akka.cluster.{ Cluster, MemberStatus }
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.testkit.{ AkkaSpec, DeadLettersFilter, TestProbe, WithLogCapturing }
import akka.testkit.TestEvent.Mute

object ShardRegionSpec {
  val host = "127.0.0.1"
  val tempConfig =
    s"""
       akka.remote.artery.canonical.hostname = "$host"
       """

  val config =
    ConfigFactory
      .parseString(tempConfig)
      .withFallback(ConfigFactory.parseString("""
        akka.loglevel = DEBUG
        akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
        akka.actor.provider = "cluster"
        akka.remote.artery.canonical.port = 0
        akka.test.single-expect-default = 5 s
        akka.cluster.sharding.distributed-data.durable.lmdb {
            dir = "target/ShardRegionSpec/sharding-ddata"
            map-size = 10 MiB
        }
        akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
        akka.cluster.jmx.enabled = off
        akka.cluster.sharding.verbose-debug-logging = on
        akka.cluster.sharding.fail-on-invalid-entity-state-transition = on
        """))

  val shardTypeName = "Caat"

  val numberOfShards = 3

  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg: Int => (msg.toString, msg)
    case _        => throw new IllegalArgumentException()
  }

  val extractShardId: ShardRegion.ExtractShardId = {
    case msg: Int                    => (msg % 10).toString
    case ShardRegion.StartEntity(id) => (id.toLong % numberOfShards).toString
    case _                           => throw new IllegalArgumentException()
  }

  class EntityActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case msg => sender() ! msg
    }
  }
}
class ShardRegionSpec extends AkkaSpec(ShardRegionSpec.config) with WithLogCapturing {

  import scala.concurrent.duration._

  import ShardRegionSpec._

  val storageLocation = List(
    new File(system.settings.config.getString("akka.cluster.sharding.distributed-data.durable.lmdb.dir")).getParentFile)

  // mute logging of deadLetters
  system.eventStream.publish(Mute(DeadLettersFilter[Any]))

  private val sysA = system
  private val sysB = ActorSystem(system.name, system.settings.config)

  private val p1 = TestProbe()(sysA)
  private val p2 = TestProbe()(sysB)

  private val region1 = startShard(sysA)
  private val region2 = startShard(sysB)

  override protected def atStartup(): Unit = {
    storageLocation.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  override def beforeTermination(): Unit = {
    shutdown(sysB)
  }

  override protected def afterTermination(): Unit = {
    storageLocation.foreach(dir => if (dir.exists) FileUtils.deleteQuietly(dir))
  }

  def startShard(sys: ActorSystem): ActorRef =
    ClusterSharding(sys).start(
      shardTypeName,
      Props[EntityActor](),
      ClusterShardingSettings(system).withRememberEntities(true),
      extractEntityId,
      extractShardId)

  def startProxy(sys: ActorSystem): ActorRef =
    ClusterSharding(sys).startProxy(shardTypeName, None, extractEntityId, extractShardId)

  "ClusterSharding" must {

    "initialize cluster and allocate sharded actors" in {

      Cluster(sysA).join(Cluster(sysA).selfAddress) // coordinator on A
      awaitAssert(Cluster(sysA).selfMember.status shouldEqual MemberStatus.Up, 1.second)

      Cluster(sysB).join(Cluster(sysA).selfAddress)

      within(10.seconds) {
        awaitAssert {
          Set(sysA, sysB).foreach { s =>
            Cluster(s).sendCurrentClusterState(testActor)
            expectMsgType[CurrentClusterState].members.size shouldEqual 2
          }
        }
      }

      region1.tell(1, p1.ref)
      p1.expectMsg(1)

      region2.tell(2, p2.ref)
      p2.expectMsg(2)

      region2.tell(3, p2.ref)
      p2.expectMsg(3)
    }

    "only deliver buffered RestartShard to the local region" in {

      def statesFor(region: ActorRef, probe: TestProbe, expect: Int) = {
        region.tell(ShardRegion.GetShardRegionState, probe.ref)
        probe
          .receiveWhile(messages = expect) {
            case e: ShardRegion.CurrentShardRegionState =>
              e.failed.isEmpty shouldEqual true
              e.shards.map(_.shardId)
          }
          .flatten
      }

      def awaitRebalance(region: ActorRef, msg: Int, probe: TestProbe): Boolean = {
        region.tell(msg, probe.ref)
        probe.expectMsgPF(2.seconds) {
          case id => if (id == msg) true else awaitRebalance(region, msg, probe)
        }
      }

      val region1Shards = statesFor(region1, p1, expect = 2)
      val region2Shards = statesFor(region2, p2, expect = 1)
      region1Shards shouldEqual Seq("1", "3")
      region2Shards shouldEqual Seq("2")
      val allShards = region1Shards ++ region2Shards

      region2 ! PoisonPill
      awaitAssert(region2.isTerminated)

      // Difficult to raise the RestartShard in conjunction with the rebalance for mode=ddata
      awaitAssert(awaitRebalance(region1, 2, p1))

      val rebalancedOnRegion1 = statesFor(region1, p1, expect = numberOfShards)
      awaitAssert(rebalancedOnRegion1.size shouldEqual numberOfShards, 5.seconds)
      rebalancedOnRegion1 shouldEqual allShards
    }
  }

}
