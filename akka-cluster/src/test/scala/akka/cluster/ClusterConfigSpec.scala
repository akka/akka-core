/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import language.postfixOps

import akka.actor.Address
import akka.dispatch.Dispatchers
import akka.remote.PhiAccrualFailureDetector
import akka.testkit.AkkaSpec
import akka.util.Helpers.ConfigOps
import akka.util.Version

class ClusterConfigSpec extends AkkaSpec {

  "Clustering" must {

    "be able to parse generic cluster config elements" in {
      val settings = new ClusterSettings(system.settings.config, system.name)
      import settings._
      LogInfo should ===(true)
      FailureDetectorConfig.getDouble("threshold") should ===(8.0 +- 0.0001)
      FailureDetectorConfig.getInt("max-sample-size") should ===(1000)
      FailureDetectorConfig.getMillisDuration("min-std-deviation") should ===(100 millis)
      FailureDetectorConfig.getMillisDuration("acceptable-heartbeat-pause") should ===(3 seconds)
      FailureDetectorImplementationClass should ===(classOf[PhiAccrualFailureDetector].getName)
      SeedNodes should ===(Vector.empty[Address])
      SeedNodeTimeout should ===(5 seconds)
      RetryUnsuccessfulJoinAfter should ===(10 seconds)
      ShutdownAfterUnsuccessfulJoinSeedNodes should ===(Duration.Undefined)
      PeriodicTasksInitialDelay should ===(1 seconds)
      GossipInterval should ===(1 second)
      GossipTimeToLive should ===(2 seconds)
      HeartbeatInterval should ===(1 second)
      MonitoredByNrOfMembers should ===(9)
      HeartbeatExpectedResponseAfter should ===(1 seconds)
      LeaderActionsInterval should ===(1 second)
      UnreachableNodesReaperInterval should ===(1 second)
      PublishStatsInterval should ===(Duration.Undefined)
      DownRemovalMargin should ===(Duration.Zero)
      MinNrOfMembers should ===(1)
      MinNrOfMembersOfRole should ===(Map.empty[String, Int])
      SelfDataCenter should ===("default")
      Roles should ===(Set(ClusterSettings.DcRolePrefix + "default"))
      AppVersion should ===(Version.Zero)
      JmxEnabled should ===(true)
      UseDispatcher should ===(Dispatchers.InternalDispatcherId)
      GossipDifferentViewProbability should ===(0.8 +- 0.0001)
      ReduceGossipDifferentViewProbability should ===(400)
      SchedulerTickDuration should ===(33 millis)
      SchedulerTicksPerWheel should ===(512)
    }

    "be able to parse non-default cluster config elements" in {
      val settings = new ClusterSettings(
        ConfigFactory.parseString("""
          |akka {
          |  cluster {
          |    roles = [ "hamlet" ]
          |    multi-data-center.self-data-center = "blue"
          |  }
          |}
        """.stripMargin).withFallback(ConfigFactory.load()),
        system.name)
      import settings._
      Roles should ===(Set("hamlet", ClusterSettings.DcRolePrefix + "blue"))
      SelfDataCenter should ===("blue")
    }
  }
}
