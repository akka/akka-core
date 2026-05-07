/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.akka.cluster.typed

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.cluster.typed.ClusterSingleton
import akka.cluster.typed.ClusterSingletonSettings
import akka.cluster.typed.SingletonActor
import akka.coordination.lease.LeaseUsageSettings
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.annotation.nowarn
import scala.concurrent.duration.DurationInt

object LeaseDocSpec {

  def config = ConfigFactory.parseString("""
      // #singleton-config
      my.app.my-singleton-lease {
        use-lease = "akka.coordination.lease.kubernetes"
        lease-retry-interval = 5s
        lease-name = "my-pingpong-singleton-lease"
      }
      // #singleton-config
      """).withFallback(ConfigFactory.load()).resolve()
}

@nowarn("msg=never used")
class LeaseDocSpec extends ScalaTestWithActorTestKit(LeaseDocSpec.config) with AnyWordSpecLike {
  import akka.cluster.typed.ClusterSingletonApiSpec._
  "docs" should {
    "show per singleton config" in {
      // #singleton-load-config
      val settings =
        ClusterSingletonSettings(system).withLeaseSettings(
          LeaseUsageSettings(system.settings.config.getConfig("my.app.my-singleton-lease")))
      val singletonActor = SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings)
      // #singleton-load-config
      if (false) {
        // #singleton-load-config
        ClusterSingleton(system).init(singletonActor)
        // #singleton-load-config
      }
      settings.leaseSettings.get.leaseName should ===("my-pingpong-singleton-lease")
    }

    "show per singleton settings" in {
      // #singleton-settings
      val settings = ClusterSingletonSettings(system).withLeaseSettings(
        LeaseUsageSettings("akka.coordination.lease.kubernetes", 5.seconds, "my-pingpong-singleton-lease"))
      val singletonActor = SingletonActor(pingPong, "ping-pong").withStopMessage(Perish).withSettings(settings)
      // #singleton-settings
      settings.leaseSettings.get.leaseName should ===("my-pingpong-singleton-lease")
    }
  }

}
