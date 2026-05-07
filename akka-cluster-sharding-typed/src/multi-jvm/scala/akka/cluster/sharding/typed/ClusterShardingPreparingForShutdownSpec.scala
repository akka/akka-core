/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.MemberStatus
import akka.cluster.MemberStatus.Removed
import akka.cluster.sharding.typed.ClusterShardingPreparingForShutdownSpec.Pinger.Command
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Leave
import akka.cluster.typed.MultiNodeTypedClusterSpec
import akka.cluster.typed.PrepareForFullClusterShutdown
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.serialization.jackson.CborSerializable

object ClusterShardingPreparingForShutdownSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = DEBUG 
    akka.actor.provider = "cluster"
    akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
    akka.cluster.testkit.auto-down-unreachable-after = off
    akka.cluster.leader-actions-interval = 100ms
    """))

  object Pinger {
    sealed trait Command extends CborSerializable
    case class Ping(id: Int, ref: ActorRef[Pong]) extends Command
    case class Pong(id: Int) extends CborSerializable

    def apply(): Behavior[Command] = Behaviors.setup { _ =>
      Behaviors.receiveMessage[Command] {
        case Ping(id: Int, ref) =>
          ref ! Pong(id)
          Behaviors.same
      }
    }

  }

  val typeKey = EntityTypeKey[Command]("ping")
}

class ClusterShardingPreparingForShutdownMultiJvmNode1 extends ClusterShardingPreparingForShutdownSpec
class ClusterShardingPreparingForShutdownMultiJvmNode2 extends ClusterShardingPreparingForShutdownSpec
class ClusterShardingPreparingForShutdownMultiJvmNode3 extends ClusterShardingPreparingForShutdownSpec

class ClusterShardingPreparingForShutdownSpec
    extends MultiNodeSpec(ClusterShardingPreparingForShutdownSpec)
    with MultiNodeTypedClusterSpec {
  import ClusterShardingPreparingForShutdownSpec._
  import ClusterShardingPreparingForShutdownSpec.Pinger._

  override def initialParticipants = roles.size

  private val sharding = ClusterSharding(typedSystem)

  "Preparing for shut down ClusterSharding" must {

    "form cluster" in {
      formCluster(first, second, third)
    }

    "not rebalance but should still work preparing for shutdown" in {

      val shardRegion: ActorRef[ShardingEnvelope[Command]] =
        sharding.init(Entity(typeKey)(_ => Pinger()))

      val probe = TestProbe[Pong]()
      shardRegion ! ShardingEnvelope("id1", Pinger.Ping(1, probe.ref))
      probe.expectMessage(Pong(1))

      runOn(second) {
        cluster.manager ! PrepareForFullClusterShutdown
      }
      awaitAssert({
        withClue("members: " + cluster.state.members) {
          cluster.selfMember.status shouldEqual MemberStatus.ReadyForShutdown
          cluster.state.members.unsorted.map(_.status) shouldEqual Set(MemberStatus.ReadyForShutdown)
        }
      }, 10.seconds)
      enterBarrier("preparation-complete")

      shardRegion ! ShardingEnvelope("id2", Pinger.Ping(2, probe.ref))
      probe.expectMessage(Pong(2))

      runOn(second) {
        cluster.manager ! Leave(address(second))
      }
      awaitAssert({
        runOn(first, third) {
          withClue("members: " + cluster.state.members) {
            cluster.state.members.size shouldEqual 2
          }
        }
        runOn(second) {
          withClue("self member: " + cluster.selfMember) {
            cluster.selfMember.status shouldEqual MemberStatus.Removed
          }
        }
      }, 5.seconds) // keep this lower than coordinated shutdown timeout

      // trigger creation of a new shard should be fine even though one node left
      runOn(first, third) {

        awaitAssert({
          shardRegion ! ShardingEnvelope("id3", Pinger.Ping(3, probe.ref))
          probe.expectMessage(Pong(3))
        }, 10.seconds)
      }
      enterBarrier("new-shards-verified")

      runOn(third) {
        cluster.manager ! Leave(address(first))
        cluster.manager ! Leave(address(third))
      }
      awaitAssert({
        withClue("self member: " + cluster.selfMember) {
          cluster.selfMember.status shouldEqual Removed
        }
      }, 15.seconds)
      enterBarrier("done")
    }
  }
}
