/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.cluster.sharding.ShardRegion

object LeastFrequentlyUsedSpec {

  val config: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lfu
        lfu {
          active-entity-limit = 10
          replacement.policy = least-frequently-used
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val dynamicAgingConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lfuda
        lfuda {
          active-entity-limit = 10
          replacement {
            policy = least-frequently-used
            least-frequently-used {
              dynamic-aging = on
            }
          }
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val idleConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lfu-idle
        lfu-idle {
          active-entity-limit = 3
          replacement.policy = least-frequently-used
          idle-entity.timeout = 1s
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class LeastFrequentlyUsedSpec
    extends AbstractEntityPassivationSpec(LeastFrequentlyUsedSpec.config, expectedEntities = 40) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.ManuallyPassivate
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least frequently used entities" must {
    "passivate the least frequently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first, least frequently used entities passivated once the limit is reached
      for (id <- 1 to 20) {
        val accesses = (id - 1) % 4 + 1 // vary frequency between 1 and 4 accesses
        for (x <- 1 to accesses) {
          region ! Envelope(shard = 1, id = id, message = s"A$x")
          expectReceived(id = id, message = s"A$x")
        }
        val expectPassivated =
          Map(11 -> 1, 12 -> 5, 13 -> 9, 14 -> 13, 15 -> 2, 16 -> 6, 17 -> 10, 18 -> 17, 19 -> 14, 20 -> 18)
        expectPassivated.get(id).foreach(passivated => expectReceived(id = passivated, message = Stop))
      }

      // shard 1: frequency 3 = (3, 7, 11, 15, 19), frequency 4 = (4, 8, 12, 16, 20)
      expectState(region)(1 -> Set(3, 7, 11, 15, 19, 4, 8, 12, 16, 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "B")
      expectReceived(id = 21, message = "B")
      for (id <- Seq(3, 7, 11, 15, 19)) {
        expectReceived(id = id, message = Stop)
      }

      // shard 1: frequency 4 = (4, 8, 12, 16, 20)
      // shard 2: frequency 1 = (21)
      expectState(region)(1 -> Set(4, 8, 12, 16, 20), 2 -> Set(21))

      // shards now have a limit of 5 entities
      // note: newly added entities are not counted as least frequent for passivation
      for (id <- 1 to 20) {
        val accesses = (id - 1) % 4 + 1 // vary frequency between 1 and 4 accesses
        for (x <- 1 to accesses) {
          region ! Envelope(shard = 1, id = id, message = s"C$x")
          expectReceived(id = id, message = s"C$x")
        }
        val passivated = if (accesses == 1) id + 3 else id - 1
        expectReceived(id = passivated, message = Stop)
      }

      // shard 1: frequency 4 = (4, 8, 12, 16, 20)
      // shard 2: frequency 1 = (21)
      expectState(region)(1 -> Set(4, 8, 12, 16, 20), 2 -> Set(21))

      // shards now have a limit of 5 entities
      for (id <- 21 to 24) {
        region ! Envelope(shard = 2, id = id, message = "D")
        expectReceived(id = id, message = "D")
      }

      // shard 1: frequency 4 = (4, 8, 12, 16, 20)
      // shard 2: frequency 1 = (22, 23, 24), frequency 2 = (21)
      expectState(region)(1 -> Set(4, 8, 12, 16, 20), 2 -> Set(22, 23, 24, 21))

      // activating a third shard will divide the per-shard limit in three, passivating entities over the new limits
      region ! Envelope(shard = 3, id = 31, message = "E")
      expectReceived(id = 31, message = "E")
      for (id <- Seq(4, 8, 22)) {
        expectReceived(id = id, message = Stop)
      }

      // shard 1: frequency 4 = (12, 16, 20)
      // shard 2: frequency 1 = (23, 24), frequency 2 = (21)
      // shard 3: frequency 1 = (31)
      expectState(region)(1 -> Set(12, 16, 20), 2 -> Set(23, 24, 21), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 21 to 30) {
        val accesses = (id - 1) % 4 + 1 // vary frequency between 1 and 4 accesses
        for (x <- 1 to accesses) {
          region ! Envelope(shard = 2, id = id, message = s"F$x")
          expectReceived(id = id, message = s"F$x")
        }
        val expectPassivated =
          Map(22 -> 23, 23 -> 24, 24 -> 22, 25 -> 21, 26 -> 25, 27 -> 26, 28 -> 23, 29 -> 27, 30 -> 29)
        expectPassivated.get(id).foreach(passivated => expectReceived(id = passivated, message = Stop))
      }

      // shard 1: frequency 4 = (12, 16, 20)
      // shard 2: frequency 2 = (30), frequency 4 = (24, 28)
      // shard 3: frequency 1 = (31)
      expectState(region)(1 -> Set(12, 16, 20), 2 -> Set(30, 24, 28), 3 -> Set(31))

      // shards now have a limit of 3 entities
      for (id <- 31 to 40) {
        val accesses = 4 - (id - 1) % 4 // vary frequency between 1 and 4 accesses
        for (x <- 1 to accesses) {
          region ! Envelope(shard = 3, id = id, message = s"G$x")
          expectReceived(id = id, message = s"G$x")
        }
        val expectPassivated = Map(34 -> 32, 35 -> 31, 36 -> 35, 37 -> 36, 38 -> 34, 39 -> 38, 40 -> 39)
        expectPassivated.get(id).foreach(passivated => expectReceived(id = passivated, message = Stop))
      }

      // shard 1: frequency 4 = (12, 16, 20)
      // shard 2: frequency 2 = (30), frequency 4 = (24, 28)
      // shard 3: frequency 1 = (40), frequency 4 = (33, 37)
      expectState(region)(1 -> Set(12, 16, 20), 2 -> Set(30, 24, 28), 3 -> Set(40, 33, 37))

      // manually passivate some entities
      region ! Envelope(shard = 1, id = 16, message = ManuallyPassivate)
      region ! Envelope(shard = 2, id = 24, message = ManuallyPassivate)
      region ! Envelope(shard = 3, id = 33, message = ManuallyPassivate)
      expectReceived(id = 16, message = ManuallyPassivate)
      expectReceived(id = 24, message = ManuallyPassivate)
      expectReceived(id = 33, message = ManuallyPassivate)
      expectReceived(id = 16, message = Stop)
      expectReceived(id = 24, message = Stop)
      expectReceived(id = 33, message = Stop)

      // shard 1: frequency 4 = (12, 20)
      // shard 2: frequency 2 = (30), frequency 4 = (28)
      // shard 3: frequency 1 = (40), frequency 4 = (37)
      expectState(region)(1 -> Set(12, 20), 2 -> Set(30, 28), 3 -> Set(40, 37))

      for (i <- 1 to 3) {
        for (x <- 1 to i) {
          region ! Envelope(shard = 1, id = 10 + i, message = s"H$x")
          region ! Envelope(shard = 2, id = 20 + i, message = s"H$x")
          region ! Envelope(shard = 3, id = 30 + i, message = s"H$x")
          expectReceived(id = 10 + i, message = s"H$x")
          expectReceived(id = 20 + i, message = s"H$x")
          expectReceived(id = 30 + i, message = s"H$x")
        }
        if (i == 2) {
          expectReceived(id = 21, message = Stop)
          expectReceived(id = 40, message = Stop)
        } else if (i == 3) {
          expectReceived(id = 11, message = Stop)
          expectReceived(id = 30, message = Stop)
          expectReceived(id = 31, message = Stop)
        }
      }

      // shard 1: frequency 3 = (13), frequency 4 = (20), frequency 6 = (12)
      // shard 2: frequency 2 = (22), frequency 3 = (23), frequency 4 = (28)
      // shard 3: frequency 2 = (32), frequency 3 = (33), frequency 4 = (37)
      expectState(region)(1 -> Set(13, 20, 12), 2 -> Set(22, 23, 28), 3 -> Set(32, 33, 37))
    }
  }
}

class LeastFrequentlyUsedWithDynamicAgingSpec
    extends AbstractEntityPassivationSpec(LeastFrequentlyUsedSpec.dynamicAgingConfig, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least frequently used entities with dynamic aging" must {
    "passivate the least frequently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first
      // ids 1 and 2 are quite popular initially
      for (id <- 1 to 2) {
        for (x <- 1 to 5) {
          region ! Envelope(shard = 1, id = id, message = s"A$x")
          expectReceived(id = id, message = s"A$x")
        }
      }
      // ids 3, 4, and 5 are very popular initially
      for (id <- 3 to 5) {
        for (x <- 1 to 10) {
          region ! Envelope(shard = 1, id = id, message = s"A$x")
          expectReceived(id = id, message = s"A$x")
        }
      }

      // shard 1: age = 0, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
      expectState(region)(1 -> Set(1, 2, 3, 4, 5))

      for (id <- 6 to 20) {
        region ! Envelope(shard = 1, id = id, message = s"B")
        expectReceived(id = id, message = s"B")
        if (id > 10) expectReceived(id = id - 5, message = Stop)
      }

      // shard 1: age = 2, @3 (1 + 2) = (16, 17, 18, 19, 20), @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
      expectState(region)(1 -> Set(1, 2, 3, 4, 5, 16, 17, 18, 19, 20))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "C")
      expectReceived(id = 21, message = "C")
      for (id <- 16 to 20) expectReceived(id, message = Stop)

      // shard 1: age = 3, @5 (5 + 0) = (1, 2), @10 (10 + 0) = (3, 4, 5)
      // shard 2: age = 0, @1 (1 + 0) = (21)
      expectState(region)(1 -> Set(1, 2, 3, 4, 5), 2 -> Set(21))

      for (id <- 6 to 10) {
        region ! Envelope(shard = 1, id = id, message = s"D")
        expectReceived(id = id, message = s"D")
      }

      // shard 1: age = 7, @7 (1 + 6) = (9), @8 (1 + 7) = (10), @10 (10 + 0) = (3, 4, 5)
      // shard 2: age = 0, @1 (1 + 0) = (21)
      expectState(region)(1 -> Set(9, 10, 3, 4, 5), 2 -> Set(21))

      for (id <- 11 to 15) {
        region ! Envelope(shard = 1, id = id, message = s"E")
        expectReceived(id = id, message = s"E")
      }

      // shard 1: age = 9, @10 (10 + 0) = (3, 4, 5), @10 (1 + 9) = (14, 15)
      // shard 2: age = 0, @1 (1 + 0) = (21)
      expectState(region)(1 -> Set(3, 4, 5, 14, 15), 2 -> Set(21))

      for (id <- 16 to 20) {
        region ! Envelope(shard = 1, id = id, message = s"F")
        expectReceived(id = id, message = s"F")
      }

      // shard 1: age = 10, @11 (1 + 10) = (16, 17, 18, 19, 20)
      // shard 2: age = 0, @1 (1 + 0) = (21)
      expectState(region)(1 -> Set(16, 17, 18, 19, 20), 2 -> Set(21))
    }
  }
}

class LeastFrequentlyUsedWithIdleSpec
    extends AbstractEntityPassivationSpec(LeastFrequentlyUsedSpec.idleConfig, expectedEntities = 3) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of idle entities with least frequently used strategy" must {
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

class LeastFrequentlyUsedLimitAdjustmentSpec
    extends AbstractEntityPassivationSpec(LeastFrequentlyUsedSpec.config, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least frequently used entities" must {
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
