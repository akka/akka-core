/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.cluster.sharding.ShardRegion

object CompositeSpec {

  val admissionWindowAndFilterConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lru-fs-slru
        lru-fs-slru {
          active-entity-limit = 20
          admission {
            window {
              policy = least-recently-used
              proportion = 0.2
              optimizer = none
            }
            filter = frequency-sketch
          }
          replacement {
            policy = least-recently-used
            least-recently-used {
              segmented {
                levels = 2
                proportions = [0.25, 0.75]
              }
            }
          }
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val admissionFilterNoWindowConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = fs-lru
        fs-lru {
          active-entity-limit = 10
          admission {
            window.policy = none
            filter = frequency-sketch
          }
          replacement.policy = least-recently-used
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val adaptiveWindowConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = lru-fs-lru-hc
        lru-fs-lru-hc {
          active-entity-limit = 10
          admission {
            window {
              policy = least-recently-used
              proportion = 0.1
              minimum-proportion = 0.1
              maximum-proportion = 1.0
              optimizer = hill-climbing
              hill-climbing {
                adjust-multiplier = 2
                initial-step = 0.4 # big step for testing
                restart-threshold = 0.8 # high threshold for testing
                step-decay = 0.5 # fast decay for testing
              }
            }
            filter = frequency-sketch
            frequency-sketch {
              reset-multiplier = 100
            }
          }
          replacement.policy = least-recently-used
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)

  val idleConfig: Config = ConfigFactory.parseString("""
    akka.cluster.sharding {
      passivation {
        strategy = default-strategy
        default-strategy {
          active-entity-limit = 3
          idle-entity.timeout = 1s
        }
      }
    }
    """).withFallback(EntityPassivationSpec.config)
}

class AdmissionWindowAndFilterSpec
    extends AbstractEntityPassivationSpec(CompositeSpec.admissionWindowAndFilterConfig, expectedEntities = 41) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of entities with composite LRU/FS/SLRU strategy" must {
    "passivate the least recently or frequently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first
      // entities move through the LRU window before moving to main
      // candidate entities from window passivated due to admission filter (and same frequency)
      // entities are only accessed once (so all in lowest segment)
      for (id <- 1 to 40) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 20) expectReceived(id = id - 4, message = Stop)
      }

      // shard 1: window: 37-40, main level 0: 1-16, main level 1: empty
      expectState(region)(1 -> ((1 to 16) ++ (37 to 40)))

      // increase the frequency of the entities in the main area
      // accessing entities a second time moves them to the higher "protected" segment in main area
      // when the limit for higher segment is reached, entities are demoted to lower "probationary" segment
      // any activated entities will passivate candidates from the window (which have lower frequency)
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "B")
        expectReceived(id = id, message = "B")
        if (id > 16) expectReceived(id = id + 20, message = Stop)
      }

      // shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
      expectState(region)(1 -> (1 to 20))

      // cycle through the entities in the window to increase their counts in the frequency sketch
      for (id <- 17 to 20; _ <- 1 to 5) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
      }

      // shard 1: window: 17-20, main level 0: 1-4, level 1: 5-16
      expectState(region)(1 -> (1 to 20))

      // activating new entities will promote candidates from the window to main (higher frequencies now)
      for (id <- 21 to 24) {
        region ! Envelope(shard = 1, id = id, message = "D")
        expectReceived(id = id, message = "D")
        expectReceived(id = id - 20, message = Stop)
      }

      // shard 1: window: 21-24, main level 0: 17-20, level 1: 5-16
      expectState(region)(1 -> (5 to 24))

      // activating more new entities will passivate candidates from the window (lower frequencies than main)
      for (id <- 25 to 28) {
        region ! Envelope(shard = 1, id = id, message = "E")
        expectReceived(id = id, message = "E")
        expectReceived(id = id - 4, message = Stop)
      }

      // shard 1: window: 25-28, main level 0: 17-20, level 1: 5-16
      expectState(region)(1 -> ((5 to 20) ++ (25 to 28)))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 41, message = "F")
      expectReceived(id = 41, message = "F")
      for (id <- List(25, 26, 17, 18, 19, 20, 5, 6, 7, 8)) {
        expectReceived(id = id, message = Stop)
      }

      // shard 1: window: 27-28, main level 0: 9-10, level 1: 11-16
      // shard 2: window: 41, main level 0: empty, level 1: empty
      expectState(region)(1 -> ((9 to 16) ++ (27 to 28)), 2 -> Set(41))

      // ids 17 and 18 still have higher frequencies, will be promoted through to main from window
      for (id <- 17 to 20) {
        region ! Envelope(shard = 1, id = id, message = "G")
        expectReceived(id = id, message = "G")
        if (id == 17) expectReceived(id = 27, message = Stop)
        if (id == 18) expectReceived(id = 28, message = Stop)
        if (id == 19) expectReceived(id = 9, message = Stop)
        if (id == 20) expectReceived(id = 10, message = Stop)
      }

      // shard 1: window: 19-20, main level 0: 17-18, level 1: 11-16
      // shard 2: window: 41, main level 0: empty, level 1: empty
      expectState(region)(1 -> (11 to 20), 2 -> Set(41))
    }
  }
}

class AdmissionFilterNoWindowSpec
    extends AbstractEntityPassivationSpec(CompositeSpec.admissionFilterNoWindowConfig, expectedEntities = 21) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of entities with composite (no window) FS/LRU strategy" must {
    "passivate the least recently or frequently used entities when the per-shard entity limit is reached" in {
      val region = start()

      // only one active shard at first
      // as entities have same frequency, when limit is reached the candidate entities (11-20) are not admitted to main
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 10) expectReceived(id = id, message = Stop)
      }

      // shard 1: 1-10
      expectState(region)(1 -> (1 to 10))

      // entities 11-20 on second access, so admitted to main, replacing current entities
      // cycle through multiple times to increase frequency counts
      for (id <- 11 to 20; i <- 1 to 5) {
        region ! Envelope(shard = 1, id = id, message = "B")
        expectReceived(id = id, message = "B")
        if (i == 1) expectReceived(id = id - 10, message = Stop)
      }

      // shard 1: 11-20
      expectState(region)(1 -> (11 to 20))

      // entities 1-10 on second and third access, but not admitted to main as lower frequency
      for (id <- 1 to 10; _ <- 1 to 2) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        expectReceived(id = id, message = Stop)
      }

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 21, message = "D")
      expectReceived(id = 21, message = "D")
      for (id <- 11 to 15) {
        expectReceived(id = id, message = Stop)
      }

      // shard 1: 16-20, shard 2: 21
      expectState(region)(1 -> (16 to 20), 2 -> Set(21))

      // frequency sketch is recreated on limit changes, so all frequencies are 0 again
      // candidate entities will replace old entities, but then retain their position
      for (id <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = "E")
        expectReceived(id = id, message = "E")
        if (id <= 5) expectReceived(id = id + 15, message = Stop)
      }

      // shard 1: 16-20, shard 2: 21
      expectState(region)(1 -> (1 to 5), 2 -> Set(21))
    }
  }
}

class AdaptiveAdmissionWindowSpec
    extends AbstractEntityPassivationSpec(CompositeSpec.adaptiveWindowConfig, expectedEntities = 70) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of entities with composite LRU/FS/LRU/HC strategy" must {
    "adjust the admission window size when optimizing with hill-climbing algorithm" in {
      val region = start()

      // fill admission window and main areas
      // increase frequency counts for entities 1-10
      for (id <- 1 to 10; i <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = s"A$i")
        expectReceived(id = id, message = s"A$i")
      }

      expectState(region)(1 -> (1 to 10))

      // window = [10], main = [1, 2, 3, 4, 5, 6, 7, 8, 9]
      // window limit is currently 1 (window proportion of 0.1)
      // these entities will be passivated as they have lower frequencies
      // given drop in active rate the adjustment direction flips to increased (for next step)
      // so that window proportion is increased by step of 0.4 (to window limit of 5) on next cycle
      for (i <- 1 to 4; id <- 11 to 20) {
        region ! Envelope(shard = 1, id = id, message = s"B$i")
        expectReceived(id = id, message = s"B$i")
        val passivated = if (id == 11 && i > 1) 20 else id - 1
        expectReceived(id = passivated, message = Stop)
      }

      expectState(region)(1 -> ((1 to 9) ++ Set(20)))

      // window = [20, 1, 2, 3, 4], main = [5, 6, 7, 8, 9]
      // window limit is currently 5 (window proportion of 0.5), direction is increasing
      // continue to maintain positive active rate delta to increase window by decayed step of 0.2
      for (id <- 21 to 30; i <- 1 to 2) {
        region ! Envelope(shard = 1, id = id, message = s"C$i")
        expectReceived(id = id, message = s"C$i")
        if (i == 1) {
          val passivated = id match {
            case 21           => 20 // in window from previous loop
            case x if x <= 25 => x - 21 // demoted from main
            case x            => x - 5 // window size
          }
          expectReceived(id = passivated, message = Stop)
        }
      }

      expectState(region)(1 -> ((5 to 9) ++ (26 to 30)))

      // window = [26, 27, 28, 29, 30, 5, 6], main = [7, 8, 9]
      // window proportion is currently 0.7, direction is increasing
      // continue to maintain positive active rate delta to increase window by decayed step of 0.1
      // increase to high active rate to trigger subsequent restart on delta over threshold
      for (id <- 21 to 24; i <- 1 to 5) {
        region ! Envelope(shard = 1, id = id, message = s"D$i")
        expectReceived(id = id, message = s"D$i")
        if (i == 1) expectReceived(id = id + 5, message = Stop)
      }

      expectState(region)(1 -> ((5 to 9) ++ (21 to 24) ++ Set(30)))

      // window = [30, 5, 6, 21, 22, 23, 24, 7], main = [8, 9]
      // window proportion is currently 0.8, direction is increasing
      // drop the active rate to zero to trigger a direction change (decrease of 0.05 to window of 7)
      // and hill-climbing restart triggered for next step (change is over restart threshold)
      for (i <- 1 to 2; id <- 31 to 40) {
        region ! Envelope(shard = 1, id = id, message = s"E$i")
        expectReceived(id = id, message = s"E$i")
        val previousWindow = IndexedSeq(30, 5, 6, 21, 22, 23, 24, 7)
        val passivated = {
          if (i == 1 && id <= 38) previousWindow(id - 31)
          else if (id <= 38) id + 2
          else id - 8 // window size
        }
        expectReceived(id = passivated, message = Stop)
      }

      expectState(region)(1 -> ((8 to 9) ++ (33 to 40)))

      // window = [34, 35, 36, 37, 38, 39, 40], main = [8, 9, 33]
      // window proportion is currently 0.7, direction is decreasing and restarted at initial step
      // stable or increasing active rate will keep decreasing window now (step of -0.4)
      for (id <- 41 to 50; i <- 1 to 2) {
        region ! Envelope(shard = 1, id = id, message = s"F$i")
        expectReceived(id = id, message = s"F$i")
        if (i == 1) expectReceived(id = id - 7 /* window size */, message = Stop)
      }

      expectState(region)(1 -> ((8 to 9) ++ Set(33) ++ (44 to 50)))

      // window = [48, 49, 50], main = [8, 9, 33, 44, 45, 46, 47]
      // window proportion is currently 0.3, direction is decreasing, with next step decayed to -0.2
      for (id <- 51 to 60; i <- 1 to 2) {
        region ! Envelope(shard = 1, id = id, message = s"G$i")
        expectReceived(id = id, message = s"G$i")
        if (i == 1) expectReceived(id = id - 3 /* window size */, message = Stop)
      }

      expectState(region)(1 -> (Set(60) ++ Set(8, 9, 33, 44, 45, 46, 47, 58, 59)))

      // window = [60], main = [8, 9, 33, 44, 45, 46, 47, 58, 59]
      // window proportion is currently 0.1, direction is decreasing, with next step decayed to -0.1
      for (id <- 61 to 70) {
        region ! Envelope(shard = 1, id = id, message = s"H")
        expectReceived(id = id, message = s"H")
        expectReceived(id = id - 1 /* window size */, message = Stop)
      }

      expectState(region)(1 -> (Set(70) ++ Set(8, 9, 33, 44, 45, 46, 47, 58, 59)))
    }
  }
}

class CompositeWithIdleSpec extends AbstractEntityPassivationSpec(CompositeSpec.idleConfig, expectedEntities = 3) {

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

class CompositeLimitAdjustmentSpec
    extends AbstractEntityPassivationSpec(CompositeSpec.admissionWindowAndFilterConfig, expectedEntities = 41) {

  import EntityPassivationSpec.Entity.Envelope
  import EntityPassivationSpec.Entity.Stop

  "Passivation of least recently used entities" must {
    "adjust per-shard entity limits when the per-region limit is dynamically adjusted" in {
      val region = start()

      // only one active shard at first, initial per-shard limit of 20
      // entities move through the LRU window before moving to main
      // candidate entities from window passivated due to admission filter (and same frequency)
      for (id <- 1 to 40) {
        region ! Envelope(shard = 1, id = id, message = "A")
        expectReceived(id = id, message = "A")
        if (id > 20) expectReceived(id = id - 4, message = Stop)
      }

      expectState(region)(1 -> ((1 to 16) ++ (37 to 40)))

      // activating a second shard will divide the per-shard limit in two, passivating half of the first shard
      region ! Envelope(shard = 2, id = 41, message = "B")
      expectReceived(id = 41, message = "B")
      for (id <- List(37, 38) ++ (1 to 8)) {
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> ((9 to 16) ++ (39 to 40)), 2 -> Set(41))

      // reduce the per-region limit from 20 to 10, per-shard limit becomes 5
      region ! ShardRegion.SetActiveEntityLimit(10)
      for (id <- 39 +: (9 to 12)) { // passivate entities over new limit
        expectReceived(id = id, message = Stop)
      }

      expectState(region)(1 -> ((13 to 16) ++ Set(40)), 2 -> Set(41))

      // note: frequency sketch is recreated when limit changes, all frequency counts are 0 again
      for (id <- 1 to 10) {
        region ! Envelope(shard = 1, id = id, message = "C")
        expectReceived(id = id, message = "C")
        val passivated = if (id == 1) 40 else if (id <= 5) id + 11 else id - 1
        expectReceived(id = passivated, message = Stop)
      }

      expectState(region)(1 -> ((1 to 4) ++ Set(10)), 2 -> Set(41))

      // increase the per-region limit from 10 to 30, per-shard limit becomes 15
      region ! ShardRegion.SetActiveEntityLimit(30)

      // note: frequency sketch is recreated when limit changes, all frequency counts are 0 again
      for (id <- 1 to 20) {
        region ! Envelope(shard = 1, id = id, message = "D")
        expectReceived(id = id, message = "D")
        if (id > 15) { // start passivating window candidates at new higher limit of 15
          expectReceived(id = id - 3 /* new window size */, message = Stop)
        }
      }

      expectState(region)(1 -> ((1 to 12) ++ (18 to 20)), 2 -> Set(41))
    }
  }
}
