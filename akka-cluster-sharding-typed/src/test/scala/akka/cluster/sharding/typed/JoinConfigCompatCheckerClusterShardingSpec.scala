/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.collection.{ immutable => im }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.CoordinatedShutdown
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.ActorSystem
import akka.cluster.{ Cluster => ClassicCluster }
import akka.testkit.LongRunningTest

object JoinConfigCompatCheckerClusterShardingSpec {

  val Shards = 2

  val Key = "akka.cluster.sharding.number-of-shards"

  val baseConfig: Config =
    ConfigFactory.parseString("""
      akka.actor.provider = "cluster"
      akka.cluster.sharding.state-store-mode = "persistence"
      akka.cluster.configuration-compatibility-check.enforce-on-join = on
      akka.cluster.jmx.enabled = off
      akka.remote.artery.canonical.port = 0
    """)

  def clusterConfig: Config =
    joinConfig(Shards)

  def joinConfig(configured: Int): Config =
    ConfigFactory.parseString(s"$Key = $configured").withFallback(baseConfig)
}

class JoinConfigCompatCheckerClusterShardingSpec
    extends ScalaTestWithActorTestKit(JoinConfigCompatCheckerClusterShardingSpec.clusterConfig)
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  import CoordinatedShutdown.IncompatibleConfigurationDetectedReason
  import JoinConfigCompatCheckerClusterShardingSpec._

  private val clusterWaitDuration = 5.seconds

  private def configured(system: ActorSystem[_]): Int =
    system.settings.config.getInt(Key)

  private def join(sys: ActorSystem[_]): ClassicCluster = {
    if (sys eq system) {
      configured(system) should ===(Shards)
      val seedNode = ClassicCluster(system)
      seedNode.join(seedNode.selfAddress)
      val probe = createTestProbe()
      probe.awaitAssert(seedNode.readView.isSingletonCluster should ===(true), clusterWaitDuration)
      seedNode
    } else {
      val joiningNode = ClassicCluster(sys)
      joiningNode.joinSeedNodes(im.Seq(ClassicCluster(system).selfAddress))
      joiningNode
    }
  }

  "A Joining Node" must {

    s"not be allowed to join a cluster with different '${Key}'" taggedAs LongRunningTest in {
      join(system)
      val joining = ActorTestKit(system.name, joinConfig(Shards + 1)) // different
      configured(joining.system) should ===(configured(system) + 1)

      val joiningNode = join(joining.system)
      val probe = createTestProbe()
      probe.awaitAssert(joiningNode.readView.isTerminated should ===(true), clusterWaitDuration)
      CoordinatedShutdown(joining.system).shutdownReason() should ===(Some(IncompatibleConfigurationDetectedReason))

      joining.shutdownTestKit()
    }
  }
}
