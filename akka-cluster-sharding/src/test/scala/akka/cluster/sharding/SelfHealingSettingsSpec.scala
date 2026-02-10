/*
 * Copyright (C) 2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import akka.cluster.sharding.ClusterShardingSettings.SelfHealingSettings
import akka.testkit.{ AkkaSpec, TestKit }

class SelfHealingSettingsSpec extends AnyWordSpec with Matchers {

  def settings(conf: String): ClusterShardingSettings = {
    val config = ConfigFactory.parseString(conf).withFallback(AkkaSpec.testConf)
    val system = ActorSystem("SelfHealingSettingsSpec", config)
    val clusterShardingSettings = ClusterShardingSettings(system)
    TestKit.shutdownActorSystem(system)
    clusterShardingSettings
  }

  val defaultSettings: ClusterShardingSettings = settings(conf = "")

  "SelfHealingSettings" must {

    // === Default Values Tests ===

    "be disabled by default" in {
      defaultSettings.selfHealingSettings.enabled shouldBe false
    }

    "have correct default values when disabled" in {
      val sh = defaultSettings.selfHealingSettings
      sh.staleRegionTimeout shouldBe 30.seconds
      sh.checkInterval shouldBe 5.seconds
      sh.startupGracePeriod shouldBe 60.seconds
      sh.dryRun shouldBe false
    }

    // === HOCON Configuration Tests ===

    "allow enabling self-healing via config" in {
      settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
        }
      """).selfHealingSettings.enabled shouldBe true
    }

    "allow configuring stale-region-timeout" in {
      settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
          stale-region-timeout = 45s
        }
      """).selfHealingSettings.staleRegionTimeout shouldBe 45.seconds
    }

    "allow configuring check-interval" in {
      settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
          check-interval = 10s
        }
      """).selfHealingSettings.checkInterval shouldBe 10.seconds
    }

    "allow configuring startup-grace-period" in {
      settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
          startup-grace-period = 120s
        }
      """).selfHealingSettings.startupGracePeriod shouldBe 120.seconds
    }

    "allow configuring dry-run mode" in {
      settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
          dry-run = on
        }
      """).selfHealingSettings.dryRun shouldBe true
    }

    "allow configuring all settings together" in {
      val sh = settings("""
        akka.cluster.sharding.self-healing {
          enabled = on
          stale-region-timeout = 60s
          check-interval = 10s
          startup-grace-period = 90s
          dry-run = on
        }
      """).selfHealingSettings

      sh.enabled shouldBe true
      sh.staleRegionTimeout shouldBe 60.seconds
      sh.checkInterval shouldBe 10.seconds
      sh.startupGracePeriod shouldBe 90.seconds
      sh.dryRun shouldBe true
    }

    // === Programmatic Configuration Tests ===

    "provide a disabled instance via SelfHealingSettings.disabled" in {
      val sh = SelfHealingSettings.disabled
      sh.enabled shouldBe false
      sh.staleRegionTimeout shouldBe 30.seconds
      sh.checkInterval shouldBe 5.seconds
      sh.startupGracePeriod shouldBe 60.seconds
      sh.dryRun shouldBe false
    }

    "support programmatic configuration via withEnabled" in {
      val sh = SelfHealingSettings.disabled.withEnabled(true)
      sh.enabled shouldBe true
    }

    "support programmatic configuration via withStaleRegionTimeout" in {
      val sh = SelfHealingSettings.disabled.withStaleRegionTimeout(45.seconds)
      sh.staleRegionTimeout shouldBe 45.seconds
    }

    "support programmatic configuration via withCheckInterval" in {
      val sh = SelfHealingSettings.disabled.withCheckInterval(15.seconds)
      sh.checkInterval shouldBe 15.seconds
    }

    "support programmatic configuration via withStartupGracePeriod" in {
      val sh = SelfHealingSettings.disabled.withStartupGracePeriod(90.seconds)
      sh.startupGracePeriod shouldBe 90.seconds
    }

    "support programmatic configuration via withDryRun" in {
      val sh = SelfHealingSettings.disabled.withDryRun(true)
      sh.dryRun shouldBe true
    }

    "support chaining multiple programmatic configurations" in {
      val sh = SelfHealingSettings.disabled
        .withEnabled(true)
        .withStaleRegionTimeout(45.seconds)
        .withCheckInterval(10.seconds)
        .withStartupGracePeriod(120.seconds)
        .withDryRun(true)

      sh.enabled shouldBe true
      sh.staleRegionTimeout shouldBe 45.seconds
      sh.checkInterval shouldBe 10.seconds
      sh.startupGracePeriod shouldBe 120.seconds
      sh.dryRun shouldBe true
    }

    // === Java Duration Support Tests ===

    "support Java Duration for programmatic configuration" in {
      val sh = SelfHealingSettings.disabled
        .withStaleRegionTimeout(java.time.Duration.ofSeconds(45))
        .withCheckInterval(java.time.Duration.ofSeconds(10))
        .withStartupGracePeriod(java.time.Duration.ofMinutes(2))

      sh.staleRegionTimeout shouldBe 45.seconds
      sh.checkInterval shouldBe 10.seconds
      sh.startupGracePeriod shouldBe 2.minutes
    }

    "support Java Duration for stale-region-timeout" in {
      val sh = SelfHealingSettings.disabled.withStaleRegionTimeout(java.time.Duration.ofMinutes(1))
      sh.staleRegionTimeout shouldBe 1.minute
    }

    "support Java Duration for check-interval" in {
      val sh = SelfHealingSettings.disabled.withCheckInterval(java.time.Duration.ofMillis(500))
      sh.checkInterval shouldBe 500.millis
    }

    "support Java Duration for startup-grace-period" in {
      val sh = SelfHealingSettings.disabled.withStartupGracePeriod(java.time.Duration.ofSeconds(30))
      sh.startupGracePeriod shouldBe 30.seconds
    }

    // === Validation Tests ===

    "reject stale-region-timeout <= 0" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        SelfHealingSettings.disabled.withStaleRegionTimeout(Duration.Zero)
      }
    }

    "reject negative stale-region-timeout" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        SelfHealingSettings.disabled.withStaleRegionTimeout(-1.second)
      }
    }

    "reject check-interval <= 0" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        SelfHealingSettings.disabled.withCheckInterval(Duration.Zero)
      }
    }

    "reject negative check-interval" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        SelfHealingSettings.disabled.withCheckInterval(-1.second)
      }
    }

    "reject startup-grace-period < 0" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        SelfHealingSettings.disabled.withStartupGracePeriod(-1.second)
      }
    }

    "allow startup-grace-period = 0" in {
      val sh = SelfHealingSettings.disabled.withStartupGracePeriod(Duration.Zero)
      sh.startupGracePeriod shouldBe Duration.Zero
    }

    // === toString Tests ===

    "provide meaningful toString" in {
      val sh = SelfHealingSettings.disabled
      sh.toString should include("enabled=false")
      sh.toString should include("staleRegionTimeout=")
      sh.toString should include("dryRun=false")
    }

    "include all fields in toString for enabled settings" in {
      val sh = SelfHealingSettings.disabled
        .withEnabled(true)
        .withStaleRegionTimeout(45.seconds)
        .withCheckInterval(10.seconds)
        .withStartupGracePeriod(90.seconds)
        .withDryRun(true)

      val str = sh.toString
      str should include("enabled=true")
      str should include("staleRegionTimeout=")
      str should include("checkInterval=")
      str should include("startupGracePeriod=")
      str should include("dryRun=true")
    }

    // === ClusterShardingSettings Integration Tests ===

    "be accessible from ClusterShardingSettings via withSelfHealingSettings" in {
      val customSelfHealing = SelfHealingSettings.disabled.withEnabled(true).withStaleRegionTimeout(45.seconds)

      val shardingSettings = defaultSettings.withSelfHealingSettings(customSelfHealing)
      shardingSettings.selfHealingSettings.enabled shouldBe true
      shardingSettings.selfHealingSettings.staleRegionTimeout shouldBe 45.seconds
    }

    "preserve other settings when updating selfHealingSettings" in {
      val customSelfHealing = SelfHealingSettings.disabled.withEnabled(true)
      val originalSettings = defaultSettings
      val updatedSettings = originalSettings.withSelfHealingSettings(customSelfHealing)

      // Self-healing should be updated
      updatedSettings.selfHealingSettings.enabled shouldBe true

      // Other settings should remain unchanged
      updatedSettings.role shouldBe originalSettings.role
      updatedSettings.rememberEntities shouldBe originalSettings.rememberEntities
    }

    "return new instance when using withSelfHealingSettings" in {
      val customSelfHealing = SelfHealingSettings.disabled.withEnabled(true)
      val original = defaultSettings
      val updated = original.withSelfHealingSettings(customSelfHealing)

      original.selfHealingSettings.enabled shouldBe false
      updated.selfHealingSettings.enabled shouldBe true
      original should not be theSameInstanceAs(updated)
    }

    "return new instance when using programmatic with* methods" in {
      val original = SelfHealingSettings.disabled
      val updated = original.withEnabled(true)

      original.enabled shouldBe false
      updated.enabled shouldBe true
      original should not be theSameInstanceAs(updated)
    }
  }
}
