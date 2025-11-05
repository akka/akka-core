/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.actor.ExtendedActorSystem
import akka.testkit.AkkaSpec

class JoinConfigCompatCheckClusterSpec extends AkkaSpec {

  private val extSystem = system.asInstanceOf[ExtendedActorSystem]
  private val clusterSettings = new ClusterSettings(system.settings.config, system.name)
  private val joinConfigCompatChecker: JoinConfigCompatChecker =
    JoinConfigCompatChecker.load(extSystem, clusterSettings)

  // Corresponding to the check of InitJoin
  def checkInitJoin(oldConfig: Config, newConfig: Config): ConfigValidation = {
    // joiningNodeConfig only contains the keys that are required according to JoinConfigCompatChecker on this node
    val joiningNodeConfig = {
      val requiredNonSensitiveKeys =
        JoinConfigCompatChecker.removeSensitiveKeys(joinConfigCompatChecker.requiredKeys, clusterSettings)
      JoinConfigCompatChecker.filterWithKeys(requiredNonSensitiveKeys, newConfig)
    }

    val configWithoutSensitiveKeys = {
      val allowedConfigPaths =
        JoinConfigCompatChecker.removeSensitiveKeys(oldConfig, clusterSettings)
      // build a stripped down config instead where sensitive config paths are removed
      // we don't want any check to happen on those keys
      JoinConfigCompatChecker.filterWithKeys(allowedConfigPaths, oldConfig)
    }

    joinConfigCompatChecker.check(joiningNodeConfig, configWithoutSensitiveKeys)
  }

  // Corresponding to the check of InitJoinAck in SeedNodeProcess
  def checkInitJoinAck(oldConfig: Config, newConfig: Config): ConfigValidation = {
    // validates config coming from cluster against this node config
    val configCheckReply = {
      val nonSensitiveKeys = JoinConfigCompatChecker.removeSensitiveKeys(newConfig, clusterSettings)
      // Send back to joining node a subset of current configuration
      // containing the keys initially sent by the joining node minus
      // any sensitive keys as defined by this node configuration
      JoinConfigCompatChecker.filterWithKeys(nonSensitiveKeys, oldConfig)
    }
    joinConfigCompatChecker.check(configCheckReply, newConfig)
  }

  "JoinConfigCompatCheckCluster" must {
    "be valid when no downing-provider" in {
      val oldConfig = ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = ""
        """).withFallback(system.settings.config)
      val newConfig = ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = ""
        """).withFallback(system.settings.config)
      checkInitJoin(oldConfig, newConfig) should ===(Valid)
    }

    "be valid when same downing-provider" in {
      val oldConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        """).withFallback(system.settings.config)
      val newConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        """).withFallback(system.settings.config)
      checkInitJoin(oldConfig, newConfig) should ===(Valid)
    }

    "be valid when updating from Lightbend sbr" in {
      val oldConfig =
        ConfigFactory
          .parseString("""
        akka.cluster.downing-provider-class = "com.lightbend.akka.sbr.SplitBrainResolverProvider"
        """)
          .withFallback(system.settings.config)
      val newConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        """).withFallback(system.settings.config)
      checkInitJoin(oldConfig, newConfig) should ===(Valid)
    }

    "be invalid when different downing-provider" in {
      val oldConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.testkit.AutoDowning"
        """).withFallback(system.settings.config)
      val newConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        """).withFallback(system.settings.config)
      checkInitJoin(oldConfig, newConfig).getClass should ===(classOf[Invalid])
    }

    "be invalid when different sbr strategy" in {
      val oldConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        akka.cluster.split-brain-resolver.active-strategy = keep-majority
        """).withFallback(system.settings.config)
      val newConfig =
        ConfigFactory.parseString("""
        akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
        akka.cluster.split-brain-resolver.active-strategy = keep-oldest
        """).withFallback(system.settings.config)
      checkInitJoin(oldConfig, newConfig).getClass should ===(classOf[Invalid])
      checkInitJoinAck(oldConfig, newConfig).getClass should ===(classOf[Invalid])
    }
  }
}
