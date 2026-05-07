/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import com.typesafe.config.Config

import akka.actor.OneForOneStrategy
import akka.util.Helpers.ConfigOps

/**
 * Default [[ClusterMetricsSupervisor]] strategy:
 * A configurable [[akka.actor.OneForOneStrategy]] with restart-on-throwable decider.
 */
class ClusterMetricsStrategy(config: Config)
    extends OneForOneStrategy(
      maxNrOfRetries = config.getInt("maxNrOfRetries"),
      withinTimeRange = config.getMillisDuration("withinTimeRange"),
      loggingEnabled = config.getBoolean("loggingEnabled"))(ClusterMetricsStrategy.metricsDecider)

/**
 * Provide custom metrics strategy resources.
 */
object ClusterMetricsStrategy {
  import akka.actor._
  import akka.actor.SupervisorStrategy._

  /**
   * [[akka.actor.SupervisorStrategy]] `Decider` which allows to survive intermittent Sigar native method calls failures.
   */
  val metricsDecider: SupervisorStrategy.Decider = {
    case _: ActorInitializationException => Stop
    case _: ActorKilledException         => Stop
    case _: DeathPactException           => Stop
    case _: Throwable                    => Restart
  }

}
