/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.testkit.AkkaSpec
import akka.util.Helpers.ConfigOps

class RemoteConfigSpec extends AkkaSpec("""
    akka.actor.provider = remote
  """) {

  "Remoting" should {

    "contain correct configuration values in reference.conf" in {
      val remoteSettings = RARP(system).provider.remoteSettings
      import remoteSettings._

      WatchFailureDetectorImplementationClass should ===(classOf[PhiAccrualFailureDetector].getName)
      WatchHeartBeatInterval should ===(1 seconds)
      WatchHeartbeatExpectedResponseAfter should ===(1 seconds)
      WatchUnreachableReaperInterval should ===(1 second)
      WatchFailureDetectorConfig.getDouble("threshold") should ===(10.0 +- 0.0001)
      WatchFailureDetectorConfig.getInt("max-sample-size") should ===(200)
      WatchFailureDetectorConfig.getMillisDuration("acceptable-heartbeat-pause") should ===(10 seconds)
      WatchFailureDetectorConfig.getMillisDuration("min-std-deviation") should ===(100 millis)
    }

  }
}
