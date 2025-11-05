/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.cluster.sharding.ShardRegion

object MostRecentlyUsedSpec {

  val config: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = mru
        mru {
          active-entity-limit = 10
          replacement.policy = most-recently-used
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val idleConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = mru-idle
        mru-idle {
          active-entity-limit = 3
          replacement.policy = most-recently-used
          idle-entity.timeout = 1s
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class MostRecentlyUsedSpec extends AbstractEntityPassivationSpec(MostRecentlyUsedSpec.config, expectedEntities = 40) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.ManuallyPassivate
  import EntityPassivationSpec.Entity.Stop

  "Passivation of most recently used entities" must {
    "passivate the most recently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first, most recently used entities passivated once the limit is reached
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id, message = "A")
        if (id > 10) expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "B")
      expectReceived(id = 21, message = "B")
      for (id <- Seq(20, 9, 8, 7, 6)) {
        expectReceived(id, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 5), 2 -> Set(21))

      // shards now have a limit of 5 entities
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        if (id > 5) expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 20), 2 -> Set(21))

      // shards now have a limit of 5 entities
      for (id <- 21 to 24) {
        region ! Envelope(shard = 2, id = id, message = "D")
        expectReceived(id = id, message = "D")
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 20), 2 -> Set(21, 22, 23, 24))

      // activating a third shard will divide the per-shard limit in three, passivating entities over the new limits
      region ! Envelope(shard = 3, id = 31, message = "E")
      expectReceived(id = 31, message = "E")
      for (id <- Seq(24, 20, 4)) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3), 2 -> Set(21, 22, 23), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 24 to 30) {
        region ! Envelope(shard = 2, id = id, message = "F")
        expectReceived(id = id, message = "F")
        expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3), 2 -> Set(21, 22, 30), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 31 to 40) {
        region ! Envelope(shard = 3, id = id, message = "G")
        expectReceived(id = id, message = "G")
        if (id > 33) expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3), 2 -> Set(21, 22, 30), 3 -> Set(31, 32, 40))

      // manually passivate some entities
      region ! Envelope(shard = 1, id = 2, message = ManuallyPassivate)
      region ! Envelope(shard = 2, id = 22, message = ManuallyPassivate)
      region ! Envelope(shard = 3, id = 32, message = ManuallyPassivate)
      expectReceived(id = 2, message = ManuallyPassivate)
      expectReceived(id = 22, message = ManuallyPassivate)
      expectReceived(id = 32, message = ManuallyPassivate)
      expectReceived(id = 2, message = Stop)
      expectReceived(id = 22, message = Stop)
      expectReceived(id = 32, message = Stop)

      expectState(region)(1 -> Set(1, 3), 2 -> Set(21, 30), 3 -> Set(31, 40))

      for (i <- 1 to 3) {
        region ! Envelope(shard = 1, id = 11 + i, message = "H")
        region ! Envelope(shard = 2, id = 22 + i, message = "H")
        region ! Envelope(shard = 3, id = 33 + i, message = "H")
        expectReceived(id = 11 + i, message = "H")
        expectReceived(id = 22 + i, message = "H")
        expectReceived(id = 33 + i, message = "H")
        if (i == 2) {
          expectReceived(id = 12, message = Stop)
          expectReceived(id = 23, message = Stop)
          expectReceived(id = 34, message = Stop)
        } else if (i == 3) {
          expectReceived(id = 13, message = Stop)
          expectReceived(id = 24, message = Stop)
          expectReceived(id = 35, message = Stop)
        }
      }

      expectState(region)(1 -> Set(1, 3, 14), 2 -> Set(21, 30, 25), 3 -> Set(31, 40, 36))
    }
  }
}

class MostRecentlyUsedWithIdleSpec
    extends AbstractEntityPassivationSpec(MostRecentlyUsedSpec.idleConfig, expectedEntities = 3) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of idle entities with most recently used strategy" must {
    "passivate entities when they haven't seen messages for the configured timeout" in {
      val region = start()

      val lastSendNanoTime1 = clock.currentTime()
      region ! Envelope(shard = 1, id = 1, message = "A")
      region ! Envelope(shard = 1, id = 2, message = "B")

      // keep entity 3 active to prevent idle passivation
      region ! Envelope(shard = 1, id = 3, message = "C")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      region ! Envelope(shard = 1, id = 3, message = "D")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      region ! Envelope(shard = 1, id = 3, message = "E")
      Thread.sleep((configuredIdleTimeout / 2).toMillis)
      val lastSendNanoTime2 = clock.currentTime()
      region ! Envelope(shard = 1, id = 3, message = "F")

      expectReceived(id = 1, message = "A")
      expectReceived(id = 2, message = "B")
      expectReceived(id = 3, message = "C")
      expectReceived(id = 3, message = "D")
      expectReceived(id = 3, message = "E")
      expectReceived(id = 3, message = "F")
      val passivate1 = expectReceived(id = 1, message = Stop)
      val passivate2 = expectReceived(id = 2, message = Stop)
      val passivate3 = expectReceived(id = 3, message = Stop, within = configuredIdleTimeout * 2)

      // note: touched timestamps are when the shard receives the message, not the entity itself
      // so look at the time from before sending the last message until receiving the passivate message
      (passivate1.nanoTime - lastSendNanoTime1).nanos should be > configuredIdleTimeout
      (passivate2.nanoTime - lastSendNanoTime1).nanos should be > configuredIdleTimeout
      (passivate3.nanoTime - lastSendNanoTime2).nanos should be > configuredIdleTimeout
    }
  }
}

class MostRecentlyUsedLimitAdjustmentSpec
    extends AbstractEntityPassivationSpec(MostRecentlyUsedSpec.config, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of most recently used entities" must {
    "adjust per-shard entity limits when the per-region limit is dynamically adjusted" in {
      val region = start()

      // only one active shard at first, initial per-shard limit of 10
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 10) expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 5, 6, 7, 8, 9, 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "B")
      expectReceived(id = 21, message = "B")
      for (id <- Seq(20, 9, 8, 7, 6)) {
        expectReceived(id, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3, 4, 5), 2 -> Set(21))

      // reduce the per-region limit from 10 to 6, per-shard limit becomes 3
      region ! ShardRegion.SetActiveEntityLimit(6)
      for (id <- Seq(5, 4)) { // passivate entities over new limit
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 3), 2 -> Set(21))

      for (id <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        if (id > 5) expectReceived(id = id - 1, message = Stop)
      }

      expectState(region)(1 -> Set(1, 2, 10), 2 -> Set(21))

      // increase the per-region limit from 6 to 12, per-shard limit becomes 6
      region ! ShardRegion.SetActiveEntityLimit(12)

      for (id <- 11 to 20) {
        region ! Envelope(shard = 1, id = id, message = "D")
        expectReceived(id = id, message = "D")
        if (id > 13) { // start passivating at new higher limit of 6
          expectReceived(id = id - 1, message = Stop)
        }
      }

      expectState(region)(1 -> Set(1, 2, 10, 11, 12, 20), 2 -> Set(21))
    }
  }
}
