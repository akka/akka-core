/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.collection.{ immutable => im }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.ActorSystem
import akka.cluster.{ Cluster, ClusterReadView }
import akka.testkit.{ AkkaSpec, LongRunningTest }
import akka.testkit.WithLogCapturing

class JoinConfigCompatCheckShardingSpec extends AkkaSpec() with WithLogCapturing {

  def initCluster(system: ActorSystem): ClusterReadView = {
    val cluster = Cluster(system)
    cluster.join(cluster.selfAddress)
    val clusterView = cluster.readView
    awaitCond(clusterView.isSingletonCluster)
    clusterView
  }

  val baseConfig: Config =
    ConfigFactory.parseString("""
     akka.actor.provider = "cluster"
     akka.loglevel = DEBUG
     akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
     akka.coordinated-shutdown.terminate-actor-system = on
     akka.remote.artery.canonical.port = 0
     akka.cluster.sharding.verbose-debug-logging = on
     """)

  "A Joining Node" must {

    /** This test verifies the built-in JoinConfigCompatCheckerSharding */
    "NOT be allowed to join a cluster using a different value for akka.cluster.sharding.state-store-mode" taggedAs LongRunningTest in {

      val joinNodeConfig =
        ConfigFactory.parseString("""
              akka.cluster {

                # use 'persistence' for state store
                sharding.state-store-mode = "persistence"

                configuration-compatibility-check {
                  enforce-on-join = on
                }
              }
            """)

      val seedNode = ActorSystem(system.name, baseConfig)
      val joiningNode = ActorSystem(system.name, joinNodeConfig.withFallback(baseConfig))

      val clusterView = initCluster(seedNode)
      val joiningNodeCluster = Cluster(joiningNode)

      try {
        // join with compatible node
        joiningNodeCluster.joinSeedNodes(im.Seq(clusterView.selfAddress))

        // node will shutdown after unsuccessful join attempt
        within(5.seconds) {
          awaitCond(joiningNodeCluster.readView.isTerminated)
        }

      } finally {
        shutdown(seedNode)
        shutdown(joiningNode)
      }

    }
  }

}
