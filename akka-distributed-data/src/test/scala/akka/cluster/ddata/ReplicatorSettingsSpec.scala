/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import akka.testkit.AkkaSpec

object ReplicatorSettingsSpec {

  val config = ConfigFactory.parseString("""
    akka.actor.provider = "cluster"
    akka.remote.artery.canonical.port = 0
    akka.remote.artery.canonical.hostname = 127.0.0.1
    """)
}

class ReplicatorSettingsSpec
    extends AkkaSpec(ReplicatorSettingsSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterAll {

  "DistributedData" must {
    "have the default replicator name" in {
      ReplicatorSettings.name(system, None) should ===("ddataReplicator")
    }
    "have the prefixed replicator name" in {
      ReplicatorSettings.name(system, Some("other")) should ===("otherDdataReplicator")
    }

    "be able to configure expiry for certain keys" in {
      val settings = ReplicatorSettings(ConfigFactory.parseString("""
        expire-keys-after-inactivity {
          "key-1" = 10 minutes
          "cache-*" = 2 minutes
        }
        """).withFallback(system.settings.config.getConfig("akka.cluster.distributed-data")))
      settings.expiryKeys("key-1") should ===(10.minutes)
      settings.expiryKeys("cache-*") should ===(2.minutes)
      settings.expiryKeys.size should ===(2)
    }
  }
}
