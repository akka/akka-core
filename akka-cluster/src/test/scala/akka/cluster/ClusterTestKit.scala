/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.util.Random

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.ActorSystem
import akka.testkit.{ AkkaSpec, TestKitBase }

/**
 * Builds on TestKitBase to provide some extra utilities to run cluster test.
 *
 * All functionality is provided through ClusterTest stateful class.
 */
trait ClusterTestKit extends TestKitBase {

  /**
   * Cluster test util to help manage ActorSystem creation and cluster formation.
   * Useful for writing single jvm, but multi ActorSystem tests.
   *
   * NOTE: This class is stateful and not thread safe. A new instance must be created per test method.
   */
  class ClusterTestUtil(val name: String) {

    private var actorSystems: List[ActorSystem] = List.empty

    /**
     * Register an [[ActorSystem]].
     */
    def register(actorSystem: ActorSystem) = {
      actorSystems = actorSystems :+ actorSystem
      actorSystem
    }

    /**
     * Register an [[ActorSystem]].
     *
     * The [[ActorSystem]] will be prepended to list and be considered the first node
     */
    def registerAsFirst(actorSystem: ActorSystem) = {
      actorSystems = actorSystem +: actorSystems
      actorSystem
    }

    /**
     * Creates a new [[ActorSystem]] using the passed [[Config]] and register it.
     */
    def newActorSystem(config: Config): ActorSystem =
      register(ActorSystem(name, config))

    /**
     * Creates a new [[ActorSystem]] using the passed [[Config]] and register it.
     *
     * This newly created [[ActorSystem]] will be prepended to list and be considered the first node
     */
    def newActorSystemAsFirst(config: Config): ActorSystem =
      registerAsFirst(ActorSystem(name, config))

    /**
     * Create a cluster using the registered [[ActorSystem]]s.
     *
     * The head of the list is considered the first node and will first create a cluster with itself.
     * Other nodes will join the cluster after the first one.
     */
    def formCluster(): Unit = {
      require(actorSystems.nonEmpty, "Can't form a cluster with an empty list of ActorSystems")

      val firstCluster = Cluster(actorSystems.head)
      firstCluster.join(firstCluster.selfAddress)

      val firstNode = firstCluster.readView
      awaitCond(firstNode.isSingletonCluster)

      // let the others join
      actorSystems
        .drop(1) // <- safe tail access
        .foreach(joinCluster)
    }

    /**
     * Makes the passed [[ActorSystem]] joins the cluster.
     * The passed system must have been previously registered on this [[ClusterTestUtil]].
     */
    def joinCluster(actorSystem: ActorSystem): Unit = {
      require(isRegistered(actorSystem), "Unknown actor system")

      val addresses = actorSystems.map(s => Cluster(s).selfAddress)

      val joiningNodeCluster = Cluster(actorSystem)
      joiningNodeCluster.joinSeedNodes(addresses)
    }

    private def isRegistered(actorSystem: ActorSystem): Boolean =
      actorSystems.contains(actorSystem)

    /** Shuts down all registered [[ActorSystem]]s */
    def shutdownAll(): Unit = actorSystems.foreach(sys => shutdown(sys, 10.seconds, verifySystemShutdown = true))

    /**
     * Force the passed [[ActorSystem]] to quit the cluster and shutdown.
     * Once original system is removed, a new [[ActorSystem]] is started using the same address.
     */
    def quitAndRestart(actorSystem: ActorSystem, config: Config): ActorSystem = {
      require(isRegistered(actorSystem), "Unknown actor system")

      // is this first seed node?
      val firstSeedNode = actorSystems.headOption.contains(actorSystem)

      val cluster = Cluster(actorSystem)
      val port = cluster.selfAddress.port.get

      // remove old before starting the new one
      cluster.leave(cluster.selfAddress)
      awaitCond(cluster.isTerminated, message = s"awaiting node [${cluster.selfAddress}] to leave and be terminated")

      awaitAssert {
        actorSystems.foreach { sys =>
          if (sys != actorSystem && Cluster(sys).selfMember.status == MemberStatus.Up) {
            // check that it's removed from members
            if (Cluster(sys).state.members.exists(_.uniqueAddress == cluster.selfUniqueAddress))
              throw new AssertionError(
                s"awaiting node [${cluster.selfAddress}] to be removed, " +
                s"still member in [${Cluster(sys).selfAddress}]: ${Cluster(sys).state}")
          }
        }
      }

      shutdown(actorSystem, 10.seconds, verifySystemShutdown = true)

      // remove from internal list
      actorSystems = actorSystems.filterNot(_ == actorSystem)

      val newConfig = ConfigFactory.parseString(s"""
          akka.remote.artery.canonical.port = $port
          """).withFallback(config)

      if (firstSeedNode) newActorSystemAsFirst(newConfig)
      else newActorSystem(newConfig)
    }

    /**
     * Returns true if the cluster instance for the provided [[ActorSystem]] is [[MemberStatus.Up]].
     */
    def isMemberUp(system: ActorSystem): Boolean = Cluster(system).selfMember.status == MemberStatus.Up

    /**
     * Returns true if the cluster instance for the provided [[ActorSystem]] has be shutdown.
     */
    def isTerminated(system: ActorSystem): Boolean = Cluster(system).isTerminated

  }
}

abstract class RollingUpgradeClusterSpec(config: Config) extends AkkaSpec(config) with ClusterTestKit {

  /**
   * Starts `size`
   * Note that the two versions of config are validated against each other and have to
   * be valid both ways: v1 => v2, v2 => v1. Uses a timeout of 20 seconds and
   * defaults to `akka.cluster.configuration-compatibility-check.enforce-on-join = on`.
   *
   * @param clusterSize the cluster size - number of nodes to create for the cluster
   * @param v1Config    the version of config to base validation against
   * @param v2Config    the upgraded version of config being validated
   */
  def upgradeCluster(clusterSize: Int, v1Config: Config, v2Config: Config): Unit = {
    val timeout = 20.seconds
    val awaitAll = timeout * clusterSize
    upgradeCluster(clusterSize, v1Config, v2Config, timeout, awaitAll, true, true)
  }

  /**
   * Starts the given `size` number of nodes and forms a cluster. Shuffles the order
   * of nodes randomly and restarts the tail using the `upgradeConfig`.
   *
   * Note that the two versions of config are validated against each other and have to
   * be valid both ways: v1 => v2, v2 => v1.
   *
   * @param clusterSize   the cluster size - number of nodes to create for the cluster
   * @param baseConfig    the version of config to base validation against
   * @param upgradeConfig the upgraded version of config being validated
   * @param timeout       the duration to wait for each member to be [[MemberStatus.Up]] on re-join
   * @param awaitAll      the duration to wait for all members to be [[MemberStatus.Up]] on initial join,
   *                      and for the one node not upgraded to register member size  as `clusterSize` on upgrade
   * @param enforced      toggle `akka.cluster.configuration-compatibility-check.enforce-on-join` on or off
   * @param shouldRejoin  the condition being tested on attempted re-join: members up or terminated
   */
  def upgradeCluster(
      clusterSize: Int,
      baseConfig: Config,
      upgradeConfig: Config,
      timeout: FiniteDuration,
      awaitAll: FiniteDuration,
      enforced: Boolean,
      shouldRejoin: Boolean): Unit = {
    require(clusterSize > 1, s"'clusterSize' must be > 1 but was $clusterSize")

    val util = new ClusterTestUtil(system.name)

    val config = (version: Config) => if (enforced) version else unenforced(version)

    try {
      val nodes = for (_ <- 0 until clusterSize) yield {
        val system = util.newActorSystem(config(baseConfig))
        util.joinCluster(system)
        system
      }
      awaitCond(nodes.forall(util.isMemberUp), awaitAll)

      val rolling = Random.shuffle(nodes)

      for (restarting <- rolling.tail) {
        within(timeout) {
          val restarted = util.quitAndRestart(restarting, config(upgradeConfig))
          util.joinCluster(restarted)
          awaitCond(if (shouldRejoin) util.isMemberUp(restarted) else util.isTerminated(restarted))
        }
      }
      awaitCond(Cluster(rolling.head).readView.members.size == (if (shouldRejoin) rolling.size else 1), awaitAll)

    } finally util.shutdownAll()
  }

  def unenforced(config: Config): Config =
    ConfigFactory
      .parseString("""akka.cluster.configuration-compatibility-check.enforce-on-join = off""")
      .withFallback(config)
}
