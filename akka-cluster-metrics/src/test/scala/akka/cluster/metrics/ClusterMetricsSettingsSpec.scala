/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.dispatch.Dispatchers
import akka.testkit.AkkaSpec

class ClusterMetricsSettingsSpec extends AkkaSpec {

  "ClusterMetricsSettings" must {

    "be able to parse generic metrics config elements" in {
      val settings = new ClusterMetricsSettings(system.settings.config)
      import settings._

      // Extension.
      MetricsDispatcher should ===(Dispatchers.DefaultDispatcherId)
      PeriodicTasksInitialDelay should ===(1 second)
      NativeLibraryExtractFolder should ===(System.getProperty("user.dir") + "/native")

      // Supervisor.
      SupervisorName should ===("cluster-metrics")
      SupervisorStrategyProvider should ===(classOf[ClusterMetricsStrategy].getName)
      SupervisorStrategyConfiguration should ===(
        ConfigFactory.parseString("loggingEnabled=true,maxNrOfRetries=3,withinTimeRange=3s"))

      // Collector.
      CollectorEnabled should ===(true)
      CollectorProvider should ===("")
      CollectorSampleInterval should ===(3 seconds)
      CollectorGossipInterval should ===(3 seconds)
      CollectorMovingAverageHalfLife should ===(12 seconds)
    }
  }
}
