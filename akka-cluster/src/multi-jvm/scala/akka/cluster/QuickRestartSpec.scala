/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.{ ActorSystem, Address }
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }
import akka.testkit._

// This test was a reproducer for issue #20639
object QuickRestartMultiJvmSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(
    debugConfig(on = false)
      .withFallback(ConfigFactory.parseString("""
      akka.cluster.testkit.auto-down-unreachable-after = off
      akka.cluster.allow-weakly-up-members = off
      """))
      .withFallback(MultiNodeClusterSpec.clusterConfig))

}

class QuickRestartMultiJvmNode1 extends QuickRestartSpec
class QuickRestartMultiJvmNode2 extends QuickRestartSpec
class QuickRestartMultiJvmNode3 extends QuickRestartSpec

abstract class QuickRestartSpec extends MultiNodeClusterSpec(QuickRestartMultiJvmSpec) with ImplicitSender {

  import QuickRestartMultiJvmSpec._

  def seedNodes: immutable.IndexedSeq[Address] = Vector(first, second, third)

  val rounds = 3

  override def verifySystemShutdown: Boolean = true
  override def expectedTestDuration: FiniteDuration = 45.seconds * rounds

  "Quickly restarting node" must {
    "setup stable nodes" taggedAs LongRunningTest in within(15.seconds) {
      cluster.joinSeedNodes(seedNodes)
      awaitMembersUp(roles.size)
      enterBarrier("stable")
    }

    "join and restart" taggedAs LongRunningTest in {
      val totalNumberOfNodes = roles.size + 1
      var restartingSystem: ActorSystem = null // only used on second
      for (n <- 1 to rounds) {
        log.info("round-" + n)
        runOn(second) {
          restartingSystem = if (restartingSystem == null) {
            ActorSystem(
              system.name,
              MultiNodeSpec.configureNextPortIfFixed(
                ConfigFactory.parseString(s"akka.cluster.roles = [round-$n]").withFallback(system.settings.config)))
          } else {
            ActorSystem(
              system.name,
              // use the same port
              ConfigFactory.parseString(s"""
                       akka.cluster.roles = [round-$n]
                       akka.remote.artery.canonical.port = ${Cluster(restartingSystem).selfAddress.port.get}
                     """).withFallback(system.settings.config))
          }
          log.info("Restarting node has address: {}", Cluster(restartingSystem).selfUniqueAddress)
          Cluster(restartingSystem).joinSeedNodes(seedNodes)
          within(20.seconds) {
            awaitAssert {
              Cluster(restartingSystem).state.members.size should ===(totalNumberOfNodes)
              Cluster(restartingSystem).state.members.map(_.status == MemberStatus.Up)
            }
          }
        }

        enterBarrier("joined-" + n)
        within(20.seconds) {
          awaitAssert {
            Cluster(system).state.members.size should ===(totalNumberOfNodes)
            Cluster(system).state.members.map(_.status == MemberStatus.Up)
            // use the role to test that it is the new incarnation that joined, sneaky
            Cluster(system).state.members.flatMap(_.roles) should ===(
              Set(s"round-$n", ClusterSettings.DcRolePrefix + "default"))
          }
        }
        enterBarrier("members-up-" + n)

        // gating occurred after a while
        if (n > 1)
          Thread.sleep(ThreadLocalRandom.current().nextInt(15) * 1000)

        Cluster(system).state.members.size should ===(totalNumberOfNodes)
        Cluster(system).state.members.map(_.status == MemberStatus.Up)
        Cluster(system).state.unreachable should ===(Set())

        enterBarrier("before-terminate-" + n)
        runOn(second) {
          restartingSystem.terminate().await
        }
        // don't wait for it to be removed, new incarnation will join in next round
        enterBarrier("terminated-" + n)
        log.info("end of round-" + n)
      }
    }

  }
}
