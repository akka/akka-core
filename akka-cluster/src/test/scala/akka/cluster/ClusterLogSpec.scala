/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.{ Address, ExtendedActorSystem }
import akka.testkit.{ AkkaSpec, EventFilter, ImplicitSender }

object ClusterLogSpec {
  val config = """
    akka.cluster {
      downing-provider-class = akka.cluster.testkit.AutoDowning
      testkit.auto-down-unreachable-after = 0s
      publish-stats-interval = 0 s # always, when it happens
      failure-detector.implementation-class = akka.cluster.FailureDetectorPuppet
    }
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.loglevel = "INFO"
    akka.loggers = ["akka.testkit.TestEventListener"]
    """

}

abstract class ClusterLogSpec(config: Config) extends AkkaSpec(config) with ImplicitSender {

  def this(s: String) = this(ConfigFactory.parseString(s))

  protected val selfAddress: Address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

  protected val upLogMessage = "event MemberUp"

  protected val downLogMessage = "event MemberDowned"

  protected val cluster = Cluster(system)

  protected def clusterView: ClusterReadView = cluster.readView

  protected def awaitUp(): Unit = {
    awaitCond(clusterView.isSingletonCluster)
    clusterView.self.address should ===(selfAddress)
    clusterView.members.map(_.address) should ===(Set(selfAddress))
    awaitAssert(clusterView.status should ===(MemberStatus.Up))
  }

  /** The expected log info pattern to intercept after a `cluster.join`. */
  protected def join(expected: String): Unit =
    EventFilter.info(occurrences = 1, pattern = expected).intercept(cluster.join(selfAddress))

  /** The expected log info pattern to intercept after a `cluster.down`. */
  protected def down(expected: String): Unit =
    EventFilter.info(occurrences = 1, pattern = expected).intercept(cluster.down(selfAddress))
}

class ClusterLogDefaultSpec extends ClusterLogSpec(ClusterLogSpec.config) {

  "A Cluster" must {

    "Log a message when becoming and stopping being a leader" in {
      cluster.settings.LogInfo should ===(true)
      cluster.settings.LogInfoVerbose should ===(false)
      join("is the new leader")
      awaitUp()
      down("is no longer leader")
    }
  }
}

class ClusterLogVerboseDefaultSpec extends ClusterLogSpec(ConfigFactory.parseString(ClusterLogSpec.config)) {

  "A Cluster" must {

    "not log verbose cluster events by default" in {
      cluster.settings.LogInfoVerbose should ===(false)
      intercept[AssertionError](join(upLogMessage))
      awaitUp()
      intercept[AssertionError](down(downLogMessage))
    }
  }
}

class ClusterLogVerboseEnabledSpec
    extends ClusterLogSpec(
      ConfigFactory
        .parseString("akka.cluster.log-info-verbose = on")
        .withFallback(ConfigFactory.parseString(ClusterLogSpec.config))) {

  "A Cluster" must {

    "log verbose cluster events when 'log-info-verbose = on'" in {
      cluster.settings.LogInfoVerbose should ===(true)
      join(upLogMessage)
      awaitUp()
      down(downLogMessage)
    }
  }
}
