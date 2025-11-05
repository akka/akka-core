/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.cluster.sharding.ShardRegion

object LeastRecentlyUsedSpec {

  val config: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lru
        lru {
          active-entity-limit = 10
          replacement.policy = least-recently-used
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val segmentedConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = slru
        slru {
          active-entity-limit = 10
          replacement {
            policy = least-recently-used
            least-recently-used {
              segmented {
                levels = 2
                proportions = [0.2, 0.8]
              }
            }
          }
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val segmentedInitialLimitConfig: Config =
    ConfigFactory.parseString("""
      akka.cluster.sharding.passivation.slru.active-entity-limit = 20
    """).withFallback(segmentedConfig)

  val idleConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lru-idle
        lru-idle {
          active-entity-limit = 3
          replacement.policy = least-recently-used
          idle-entity.timeout = 1s
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class LeastRecentlyUsedSpec extends AbstractEntityPassivationSpec(LeastRecentlyUsedSpec.config, expectedEntities = 40) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.ManuallyPassivate
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least recently used entities" must {
    "passivate the least recently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first, least recently used entities passivated once the limit is reached
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 10) expectReceived(id = id - 10, message = Stop)
      }

      expectState(region)(1 -> (11 to 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "B")
      expectReceived(id = 21, message = "B")
      for (id <- 11 to 15) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> (16 to 20), 2 -> Set(21))

      // shards now have a limit of 5 entities
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        val passivatedId = if (id <= 5) id + 15 else id - 5
        expectReceived(id = passivatedId, message = Stop)
      }

      expectState(region)(1 -> (16 to 20), 2 -> Set(21))

      // shards now have a limit of 5 entities
      for (id <- 21 to 24) {
        region ! Envelope(shard = 2, id = id, message = "D")
        expectReceived(id = id, message = "D")
      }

      expectState(region)(1 -> (16 to 20), 2 -> (21 to 24))

      // activating a third shard will divide the per-shard limit in three, passivating entities over the new limits
      region ! Envelope(shard = 3, id = 31, message = "E")
      expectReceived(id = 31, message = "E")
      for (id <- Seq(16, 17, 21)) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> Set(18, 19, 20), 2 -> Set(22, 23, 24), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 25 to 30) {
        region ! Envelope(shard = 2, id = id, message = "F")
        expectReceived(id = id, message = "F")
        expectReceived(id = id - 3, message = Stop)
      }

      expectState(region)(1 -> Set(18, 19, 20), 2 -> Set(28, 29, 30), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 31 to 40) {
        region ! Envelope(shard = 3, id = id, message = "G")
        expectReceived(id = id, message = "G")
        if (id > 33) expectReceived(id = id - 3, message = Stop)
      }

      expectState(region)(1 -> Set(18, 19, 20), 2 -> Set(28, 29, 30), 3 -> Set(38, 39, 40))

      // manually passivate some entities
      region ! Envelope(shard = 1, id = 19, message = ManuallyPassivate)
      region ! Envelope(shard = 2, id = 29, message = ManuallyPassivate)
      region ! Envelope(shard = 3, id = 39, message = ManuallyPassivate)
      expectReceived(id = 19, message = ManuallyPassivate)
      expectReceived(id = 29, message = ManuallyPassivate)
      expectReceived(id = 39, message = ManuallyPassivate)
      expectReceived(id = 19, message = Stop)
      expectReceived(id = 29, message = Stop)
      expectReceived(id = 39, message = Stop)

      expectState(region)(1 -> Set(18, 20), 2 -> Set(28, 30), 3 -> Set(38, 40))

      for (i <- 1 to 3) {
        region ! Envelope(shard = 1, id = 10 + i, message = "H")
        region ! Envelope(shard = 2, id = 20 + i, message = "H")
        region ! Envelope(shard = 3, id = 30 + i, message = "H")
        expectReceived(id = 10 + i, message = "H")
        expectReceived(id = 20 + i, message = "H")
        expectReceived(id = 30 + i, message = "H")
        if (i == 2) {
          expectReceived(id = 18, message = Stop)
          expectReceived(id = 28, message = Stop)
          expectReceived(id = 38, message = Stop)
        } else if (i == 3) {
          expectReceived(id = 20, message = Stop)
          expectReceived(id = 30, message = Stop)
          expectReceived(id = 40, message = Stop)
        }
      }

      expectState(region)(1 -> Set(11, 12, 13), 2 -> Set(21, 22, 23), 3 -> Set(31, 32, 33))
    }
  }
}

class SegmentedLeastRecentlyUsedSpec
    extends AbstractEntityPassivationSpec(LeastRecentlyUsedSpec.segmentedConfig, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of segmented least recently used entities" must {
    "passivate the (segmented) least recently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first
      // entities are only accessed once (so all in lowest segment)
      // least recently used entities passivated once the total limit is reached
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 10) expectReceived(id = id - 10, message = Stop)
      }

      // shard 1: level 0: 11-20, level 1: empty
      expectState(region)(1 -> (11 to 20))

      // accessing entities a second time moves them to the higher "protected" segment
      // when limit for higher segment is reached, entities are demoted to lower "probationary" segment
      for (id <- 11 to 20) {
        region ! Envelope(shard = 1, id = id, message = "B")
        expectReceived(id = id, message = "B")
      }

      // shard 1: level 0: 11-12, level 1: 13-20
      expectState(region)(1 -> (11 to 20))

      // newly activated entities will just cycle through the lower segment
      for (id <- 1 to 5) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        val passivatedId = if (id < 3) id + 10 else id - 2
        expectReceived(passivatedId, message = Stop)
      }

      // shard 1: level 0: 4-5, level 1: 13-20
      expectState(region)(1 -> ((4 to 5) ++ (13 to 20)))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "D")
      expectReceived(id = 21, message = "D")
      for (id <- List(4, 5, 13, 14, 15)) {
        expectReceived(id = id, message = Stop)
      }

      // shard 1: level 0: 16, level 1: 17-20
      // shard 2: level 0: 21, level 1: empty
      expectState(region)(1 -> (16 to 20), 2 -> Set(21))

      // entities in higher "protected" segment accessed again move to most recent position
      // entities in lower "probationary" segment accessed again are promoted
      // lower segment only has space for one entity
      for (id <- (11 to 20).reverse) {
        region ! Envelope(shard = 1, id = id, message = "E")
        expectReceived(id = id, message = "E")
        if (id == 15) expectReceived(id = 20, message = Stop)
        else if (id < 15) expectReceived(id = id + 1, message = Stop)
      }

      // shard 1: level 0: 11, level 1: 19, 18, 17, 16
      // shard 2: level 0: 21, level 1: empty
      expectState(region)(1 -> Set(11, 19, 18, 17, 16), 2 -> Set(21))
    }
  }
}

class LeastRecentlyUsedWithIdleSpec
    extends AbstractEntityPassivationSpec(LeastRecentlyUsedSpec.idleConfig, expectedEntities = 3) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of idle entities with least recently used strategy" must {
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

class LeastRecentlyUsedLimitAdjustmentSpec
    extends AbstractEntityPassivationSpec(LeastRecentlyUsedSpec.config, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least recently used entities" must {
    "adjust per-shard entity limits when the per-region limit is dynamically adjusted" in {
      val region = start()

      // only one active shard at first, initial per-shard limit of 10
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 10) expectReceived(id = id - 10, message = Stop)
      }

      expectState(region)(1 -> (11 to 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "B")
      expectReceived(id = 21, message = "B")
      for (id <- 11 to 15) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> (16 to 20), 2 -> Set(21))

      // reduce the per-region limit from 10 to 6, per-shard limit becomes 3
      region ! ShardRegion.SetActiveEntityLimit(6)
      for (id <- 16 to 17) { // passivate entities over new limit
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> (18 to 20), 2 -> Set(21))

      for (id <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        val passivated = if (id < 4) id + 17 else id - 3
        expectReceived(id = passivated, message = Stop)
      }

      expectState(region)(1 -> (8 to 10), 2 -> Set(21))

      // increase the per-region limit from 6 to 12, per-shard limit becomes 6
      region ! ShardRegion.SetActiveEntityLimit(12)

      for (id <- 11 to 20) {
        region ! Envelope(shard = 1, id = id, message = "D")
        expectReceived(id = id, message = "D")
        if (id > 13) { // start passivating at new higher limit of 6
          expectReceived(id = id - 6, message = Stop)
        }
      }

      expectState(region)(1 -> (15 to 20), 2 -> Set(21))
    }
  }
}

class SegmentedLeastRecentlyUsedLimitAdjustmentSpec
    extends AbstractEntityPassivationSpec(LeastRecentlyUsedSpec.segmentedInitialLimitConfig, expectedEntities = 31) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of segmented least recently used entities" must {
    "adjust per-shard entity limits when the per-region limit is dynamically adjusted" in {
      val region = start()

      // only one active shard at first, initial per-shard limit of 20
      for (id <- 1 to 30) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 20) expectReceived(id = id - 20, message = Stop)
      }

      expectState(region)(1 -> (11 to 30))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 31, message = "B")
      expectReceived(id = 31, message = "B")
      for (id <- 11 to 20) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> (21 to 30), 2 -> Set(31))

      // reduce the per-region limit from 20 to 10, per-shard limit becomes 5
      region ! ShardRegion.SetActiveEntityLimit(10)
      for (id <- 21 to 25) { // passivate entities over new limit
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> (26 to 30), 2 -> Set(31))

      for (id <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        val passivated = if (id < 6) id + 25 else id - 5
        expectReceived(id = passivated, message = Stop)
      }

      expectState(region)(1 -> (6 to 10), 2 -> Set(31))

      // increase the per-region limit from 10 to 30, per-shard limit becomes 15
      region ! ShardRegion.SetActiveEntityLimit(30)

      for (id <- 11 to 30) {
        region ! Envelope(shard = 1, id = id, message = "D")
        expectReceived(id = id, message = "D")
        if (id > 20) { // start passivating at new higher limit of 15
          expectReceived(id = id - 15, message = Stop)
        }
      }

      expectState(region)(1 -> (16 to 30), 2 -> Set(31))
    }
  }
}
