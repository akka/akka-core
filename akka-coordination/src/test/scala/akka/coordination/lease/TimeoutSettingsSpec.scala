/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TimeoutSettingsSpec extends AnyWordSpec with Matchers {
  private def conf(overrides: String): TimeoutSettings = {
    val c = ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load())
    TimeoutSettings(c)
  }
  "TimeoutSettings" should {
    "default heartbeat-interval to heartbeat-timeout / 10" in {
      conf("""
          heartbeat-timeout=100s
          heartbeat-interval=""
          lease-operation-timeout=5s
        """).heartbeatInterval shouldEqual 10.second
    }

    "have a min of 5s for heartbeat-interval" in {
      conf("""
          heartbeat-timeout=40s
          heartbeat-interval=""
          lease-operation-timeout=5s
        """).heartbeatInterval shouldEqual 5.second
    }

    "allow overriding of heartbeat-interval" in {
      conf("""
          heartbeat-timeout=100s
          heartbeat-interval=20s
          lease-operation-timeout=5s
        """).heartbeatInterval shouldEqual 20.second
    }

    "not allow heartbeat-interval to be greater or equal to half the heartbeat-timeout" in {
      intercept[IllegalArgumentException] {
        conf("""
          heartbeat-timeout=100s
          heartbeat-interval=50s
          lease-operation-timeout=5s
        """)
      }.getMessage shouldEqual "requirement failed: heartbeat-interval must be less than half heartbeat-timeout"

    }
  }

}
