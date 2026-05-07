/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.{ immutable => im }
import scala.concurrent.duration._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.testkit.GHExcludeAeronTest
import akka.testkit.LongRunningTest

object JoinConfigCompatCheckerRollingUpdateSpec {

  val baseConfig = ConfigFactory.parseString("""
      akka.log-dead-letters = off
      akka.log-dead-letters-during-shutdown = off
      akka.cluster.downing-provider-class = akka.cluster.testkit.AutoDowning
      akka.cluster.testkit.auto-down-unreachable-after = 0s
      akka.cluster {
        jmx.enabled                         = off
        gossip-interval                     = 200 ms
        leader-actions-interval             = 200 ms
        unreachable-nodes-reaper-interval   = 500 ms
        periodic-tasks-initial-delay        = 300 ms
        publish-stats-interval              = 0 s # always, when it happens
      }
    """).withFallback(JoinConfigCompatCheckerSpec.baseConfig)

  val v1Config: Config = baseConfig.withFallback(JoinConfigCompatCheckerSpec.configWithChecker)

  private val v2 = ConfigFactory.parseString("""
      akka.cluster.new-configuration = "v2"
      akka.cluster.configuration-compatibility-check.checkers {
        rolling-upgrade-test = "akka.cluster.JoinConfigCompatRollingUpdateChecker"
      }
    """)

  val v2Config: Config = v2.withFallback(v1Config)

  val v2ConfigIncompatible: Config = v2.withFallback(baseConfig)

}

class JoinConfigCompatCheckerRollingUpdateSpec
    extends RollingUpgradeClusterSpec(JoinConfigCompatCheckerRollingUpdateSpec.v1Config) {

  import JoinConfigCompatCheckerRollingUpdateSpec._

  "A Node" must {
    val timeout = 20.seconds
    "NOT be allowed to re-join a cluster if it has a new, additional configuration the others do not have and not the old"
      .taggedAs(LongRunningTest, GHExcludeAeronTest) in {
      // confirms the 2 attempted re-joins fail with both nodes being terminated
      upgradeCluster(3, v1Config, v2ConfigIncompatible, timeout, timeout, enforced = true, shouldRejoin = false)
    }
    "be allowed to re-join a cluster if it has a new, additional property and checker the others do not have".taggedAs(
      LongRunningTest,
      GHExcludeAeronTest) in {
      upgradeCluster(3, v1Config, v2Config, timeout, timeout * 3, enforced = true, shouldRejoin = true)
    }
    "be allowed to re-join a cluster if it has a new, additional configuration the others do not have and configured to NOT enforce it"
      .taggedAs(LongRunningTest, GHExcludeAeronTest) in {
      upgradeCluster(3, v1Config, v2Config, timeout, timeout * 3, enforced = false, shouldRejoin = true)
    }
  }
}

class JoinConfigCompatRollingUpdateChecker extends JoinConfigCompatChecker {
  override def requiredKeys: im.Seq[String] = im.Seq("akka.cluster.new-configuration")
  override def check(toCheck: Config, actualConfig: Config): ConfigValidation = {
    if (toCheck.hasPath(requiredKeys.head))
      JoinConfigCompatChecker.fullMatch(requiredKeys, toCheck, actualConfig)
    else Valid
  }
}
