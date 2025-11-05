/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import akka.testkit.{ AkkaSpec, TestKit }

class ClusterShardingSettingsSpec extends AnyWordSpec with Matchers {

  def settings(conf: String): ClusterShardingSettings = {
    val config = ConfigFactory.parseString(conf).withFallback(AkkaSpec.testConf)
    val system = ActorSystem("ClusterShardingSettingsSpec", config)
    val clusterShardingSettings = ClusterShardingSettings(system)
    TestKit.shutdownActorSystem(system)
    clusterShardingSettings
  }

  val defaultSettings: ClusterShardingSettings = settings(conf = "")

  "ClusterShardingSettings" must {

    "have default passivation strategy (passivate idle entities after 120 seconds)" in {
      defaultSettings.passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 120.seconds,
        interval = 60.seconds)
    }

    "allow timeout for (default) idle passivation strategy to be configured (via config)" in {
      settings("""
        #passivation-idle-timeout
        akka.cluster.sharding.passivation {
          default-idle-strategy.idle-entity.timeout = 3 minutes
        }
        #passivation-idle-timeout
      """).passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 3.minutes,
        interval = 90.seconds)
    }

    "allow timeout for (default) idle passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults.withIdleEntityPassivation(timeout = 42.seconds))
        .passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 42.seconds,
        interval = 21.seconds)
    }

    "allow timeout and interval for (default) idle passivation strategy to be configured (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          default-idle-strategy {
            idle-entity {
              timeout = 3 minutes
              interval = 1 minute
            }
          }
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 3.minutes,
        interval = 1.minute)
    }

    "allow timeout and interval for (default) idle passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(ClusterShardingSettings.PassivationStrategySettings.defaults
          .withIdleEntityPassivation(timeout = 42.seconds, interval = 42.millis))
        .passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 42.seconds,
        interval = 42.millis)
    }

    "allow new default passivation strategy to be enabled (via config)" in {
      settings("""
        #passivation-new-default-strategy
        akka.cluster.sharding.passivation {
          strategy = default-strategy
        }
        #passivation-new-default-strategy
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 100000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = None)
    }

    "allow new default passivation strategy limit to be configured (via config)" in {
      settings("""
        #passivation-new-default-strategy-configured
        akka.cluster.sharding.passivation {
          strategy = default-strategy
          default-strategy {
            active-entity-limit = 1000000
          }
        }
        #passivation-new-default-strategy-configured
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = None)
    }

    "allow new default passivation strategy with idle timeout to be configured (via config)" in {
      settings("""
        #passivation-new-default-strategy-with-idle
        akka.cluster.sharding.passivation {
          strategy = default-strategy
          default-strategy {
            idle-entity.timeout = 30.minutes
          }
        }
        #passivation-new-default-strategy-with-idle
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 100000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 30.minutes, interval = 15.minutes)))
    }

    "allow least recently used passivation strategy to be configured (via config)" in {
      settings("""
        #custom-passivation-strategy
        #lru-policy
        akka.cluster.sharding.passivation {
          strategy = custom-lru-strategy
          custom-lru-strategy {
            active-entity-limit = 1000000
            replacement.policy = least-recently-used
          }
        }
        #lru-policy
        #custom-passivation-strategy
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 1000000,
        segmented = Nil,
        idle = None)
    }

    "allow least recently used passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withLeastRecentlyUsedReplacement())
        .passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 42000,
        segmented = Nil,
        idle = None)
    }

    "allow segmented least recently used passivation strategy to be configured (via config)" in {
      settings("""
        #slru-policy
        akka.cluster.sharding.passivation {
          strategy = custom-slru-strategy
          custom-slru-strategy {
            active-entity-limit = 1000000
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
        #slru-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 1000000,
        segmented = List(0.2, 0.8),
        idle = None)
    }

    "allow 4-level segmented least recently used passivation strategy to be configured (via config)" in {
      settings("""
        #s4lru-policy
        akka.cluster.sharding.passivation {
          strategy = custom-s4lru-strategy
          custom-s4lru-strategy {
            active-entity-limit = 1000000
            replacement {
              policy = least-recently-used
              least-recently-used {
                segmented.levels = 4
              }
            }
          }
        }
        #s4lru-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 1000000,
        segmented = List(0.25, 0.25, 0.25, 0.25),
        idle = None)
    }

    "allow segmented least recently used passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(ClusterShardingSettings.PassivationStrategySettings.defaults
          .withActiveEntityLimit(42000)
          .withReplacementPolicy(ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults
            .withSegmented(proportions = List(0.4, 0.3, 0.2, 0.1))))
        .passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 42000,
        segmented = List(0.4, 0.3, 0.2, 0.1),
        idle = None)
    }

    "allow least recently used passivation strategy with idle timeout to be configured (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          strategy = custom-lru-with-idle
          custom-lru-with-idle {
            active-entity-limit = 1000000
            replacement.policy = least-recently-used
            idle-entity.timeout = 30.minutes
          }
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 1000000,
        segmented = Nil,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 30.minutes, interval = 15.minutes)))
    }

    "allow least recently used passivation strategy with idle timeout to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withLeastRecentlyUsedReplacement()
            .withIdleEntityPassivation(timeout = 42.minutes))
        .passivationStrategy shouldBe ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(
        limit = 42000,
        segmented = Nil,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 42.minutes, interval = 21.minutes)))
    }

    "allow most recently used passivation strategy to be configured (via config)" in {
      settings("""
        #mru-policy
        akka.cluster.sharding.passivation {
          strategy = custom-mru-strategy
          custom-mru-strategy {
            active-entity-limit = 1000000
            replacement.policy = most-recently-used
          }
        }
        #mru-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(
        limit = 1000000,
        idle = None)
    }

    "allow most recently used passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withMostRecentlyUsedReplacement())
        .passivationStrategy shouldBe ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(
        limit = 42000,
        idle = None)
    }

    "allow most recently used passivation strategy with idle timeout to be configured (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          strategy = custom-mru-with-idle
          custom-mru-with-idle {
            active-entity-limit = 1000000
            replacement.policy = most-recently-used
            idle-entity.timeout = 30.minutes
          }
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(
        limit = 1000000,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 30.minutes, interval = 15.minutes)))
    }

    "allow most recently used passivation strategy with idle timeout to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withMostRecentlyUsedReplacement()
            .withIdleEntityPassivation(timeout = 42.minutes))
        .passivationStrategy shouldBe ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(
        limit = 42000,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 42.minutes, interval = 21.minutes)))
    }

    "allow least frequently used passivation strategy to be configured (via config)" in {
      settings("""
        #lfu-policy
        akka.cluster.sharding.passivation {
          strategy = custom-lfu-strategy
          custom-lfu-strategy {
            active-entity-limit = 1000000
            replacement.policy = least-frequently-used
          }
        }
        #lfu-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 1000000,
        dynamicAging = false,
        idle = None)
    }

    "allow least frequently used passivation strategy to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withLeastFrequentlyUsedReplacement())
        .passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 42000,
        dynamicAging = false,
        idle = None)
    }

    "allow least frequently used passivation strategy with idle timeout to be configured (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          strategy = custom-lfu-with-idle
          custom-lfu-with-idle {
            active-entity-limit = 1000000
            replacement.policy = least-frequently-used
            idle-entity.timeout = 30.minutes
          }
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 1000000,
        dynamicAging = false,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 30.minutes, interval = 15.minutes)))
    }

    "allow least frequently used passivation strategy with idle timeout to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withLeastFrequentlyUsedReplacement()
            .withIdleEntityPassivation(timeout = 42.minutes))
        .passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 42000,
        dynamicAging = false,
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 42.minutes, interval = 21.minutes)))
    }

    "allow least frequently used passivation strategy with dynamic aging to be configured (via config)" in {
      settings("""
        #lfuda-policy
        akka.cluster.sharding.passivation {
          strategy = custom-lfu-with-dynamic-aging
          custom-lfu-with-dynamic-aging {
            active-entity-limit = 1000
            replacement {
              policy = least-frequently-used
              least-frequently-used {
                dynamic-aging = on
              }
            }
          }
        }
        #lfuda-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 1000,
        dynamicAging = true,
        idle = None)
    }

    "allow least frequently used passivation strategy with dynamic aging to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withReplacementPolicy(
              ClusterShardingSettings.PassivationStrategySettings.LeastFrequentlyUsedSettings.defaults
                .withDynamicAging()))
        .passivationStrategy shouldBe ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(
        limit = 42000,
        dynamicAging = true,
        idle = None)
    }

    "allow passivation strategy admission window policy to be configured (via config)" in {
      settings("""
        #admission-window-policy
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission-window
          custom-strategy-with-admission-window {
            active-entity-limit = 1000000
            admission.window.policy = least-recently-used
            replacement.policy = least-frequently-used
          }
        }
        #admission-window-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy =
          ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(limit = 0, dynamicAging = false, idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.NoAdmissionOptimizer,
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy.AlwaysAdmissionFilter,
        idle = None)
    }

    "allow passivation strategy admission window proportion to be configured (via config)" in {
      settings("""
        #admission-window-proportion
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission-window
          custom-strategy-with-admission-window {
            active-entity-limit = 1000000
            admission.window {
              policy = least-recently-used
              proportion = 0.1 # 10%
            }
            replacement.policy = least-frequently-used
          }
        }
        #admission-window-proportion
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy =
          ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(limit = 0, dynamicAging = false, idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.1,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.NoAdmissionOptimizer,
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy.AlwaysAdmissionFilter,
        idle = None)
    }

    "allow passivation strategy admission window optimizer to be configured (via config)" in {
      settings("""
        #admission-window-optimizer
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission-window
          custom-strategy-with-admission-window {
            active-entity-limit = 1000000
            admission.window {
              policy = least-recently-used
              optimizer = hill-climbing
            }
            replacement.policy = least-frequently-used
          }
        }
        #admission-window-optimizer
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy =
          ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(limit = 0, dynamicAging = false, idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy.AlwaysAdmissionFilter,
        idle = None)
    }

    "allow passivation strategy admission to be configured (via config)" in {
      settings("""
        #admission-policy
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission
          custom-strategy-with-admission {
            active-entity-limit = 1000000
            admission {
              window {
                policy = least-recently-used
                optimizer = hill-climbing
              }
              filter = frequency-sketch
            }
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
        #admission-policy
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = None)
    }

    "allow passivation strategy admission parameters to be tuned (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission
          custom-strategy-with-admission {
            active-entity-limit = 1000000
            admission {
              window {
                policy = least-recently-used
                proportion = 0.1
                minimum-proportion = 0.05
                maximum-proportion = 0.50
                optimizer = hill-climbing
                hill-climbing {
                  adjust-multiplier = 5
                  initial-step = 0.05
                  restart-threshold = 0.025
                  step-decay = 0.95
                }
              }
              filter = frequency-sketch
              frequency-sketch {
                depth = 3
                counter-bits = 8
                width-multiplier = 2
                reset-multiplier = 50.0
              }
            }
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
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.1,
        minimumWindowProportion = 0.05,
        maximumWindowProportion = 0.5,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 5.0,
          initialStep = 0.05,
          restartThreshold = 0.025,
          stepDecay = 0.95),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 2, resetMultiplier = 50.0, depth = 3, counterBits = 8),
        idle = None)
    }

    "allow passivation strategy admission to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withAdmission(ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.defaults
              .withWindow(ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.WindowSettings.defaults
                .withPolicy(ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults)
                .withOptimizer(
                  ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.HillClimbingSettings.defaults))
              .withFilter(
                ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings.defaults))
            .withReplacementPolicy(
              ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults
                .withSegmented(proportions = List(0.2, 0.8))))
        .passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 42000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = None)
    }

    "allow passivation strategy admission settings to be tuned (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withAdmission(ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.defaults
              .withWindow(
                ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.WindowSettings.defaults
                  .withPolicy(ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults)
                  .withInitialProportion(0.1)
                  .withMinimumProportion(0.05)
                  .withMaximumProportion(0.5)
                  .withOptimizer(
                    ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.HillClimbingSettings.defaults
                      .withAdjustMultiplier(5)
                      .withInitialStep(0.05)
                      .withRestartThreshold(0.025)
                      .withStepDecay(0.95)))
              .withFilter(
                ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings.defaults
                  .withDepth(3)
                  .withCounterBits(8)
                  .withWidthMultiplier(2)
                  .withResetMultiplier(50)))
            .withReplacementPolicy(
              ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults
                .withSegmented(proportions = List(0.2, 0.8))))
        .passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 42000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.1,
        minimumWindowProportion = 0.05,
        maximumWindowProportion = 0.5,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 5.0,
          initialStep = 0.05,
          restartThreshold = 0.025,
          stepDecay = 0.95),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 2, resetMultiplier = 50.0, depth = 3, counterBits = 8),
        idle = None)
    }

    "allow passivation strategy with admission and idle timeout to be configured (via config)" in {
      settings("""
        akka.cluster.sharding.passivation {
          strategy = custom-strategy-with-admission
          custom-strategy-with-admission {
            active-entity-limit = 1000000
            admission {
              window {
                policy = least-recently-used
                optimizer = hill-climbing
              }
              filter = frequency-sketch
            }
            replacement {
              policy = least-recently-used
              least-recently-used {
                segmented {
                  levels = 2
                  proportions = [0.2, 0.8]
                }
              }
            }
            idle-entity.timeout = 30.minutes
          }
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 1000000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 30.minutes, interval = 15.minutes)))
    }

    "allow passivation strategy with admission and idle timeout to be configured (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(
          ClusterShardingSettings.PassivationStrategySettings.defaults
            .withActiveEntityLimit(42000)
            .withAdmission(ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.defaults
              .withWindow(ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.WindowSettings.defaults
                .withPolicy(ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults)
                .withOptimizer(
                  ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.HillClimbingSettings.defaults))
              .withFilter(
                ClusterShardingSettings.PassivationStrategySettings.AdmissionSettings.FrequencySketchSettings.defaults))
            .withReplacementPolicy(
              ClusterShardingSettings.PassivationStrategySettings.LeastRecentlyUsedSettings.defaults.withSegmented(
                proportions = List(0.2, 0.8)))
            .withIdleEntityPassivation(timeout = 42.minutes))
        .passivationStrategy shouldBe ClusterShardingSettings.CompositePassivationStrategy(
        limit = 42000,
        mainStrategy = ClusterShardingSettings
          .LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = List(0.2, 0.8), idle = None),
        windowStrategy =
          ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit = 0, segmented = Nil, idle = None),
        initialWindowProportion = 0.01,
        minimumWindowProportion = 0.01,
        maximumWindowProportion = 1.0,
        windowOptimizer = ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98),
        admissionFilter = ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier = 4, resetMultiplier = 10.0, depth = 4, counterBits = 4),
        idle = Some(ClusterShardingSettings.IdlePassivationStrategy(timeout = 42.minutes, interval = 21.minutes)))
    }

    "disable automatic passivation if `remember-entities` is enabled (via config)" in {
      settings("""
        akka.cluster.sharding.remember-entities = on
      """).passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
    }

    "disable automatic passivation if `remember-entities` is enabled (via factory method)" in {
      defaultSettings
        .withRememberEntities(true)
        .passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
    }

    "disable automatic passivation if idle timeout is set to zero (via config)" in {
      settings("""
        akka.cluster.sharding.passivation.default-idle-strategy.idle-entity.timeout = 0
      """).passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
    }

    "disable automatic passivation if idle timeout is set to zero (via factory method)" in {
      defaultSettings
        .withPassivationStrategy(ClusterShardingSettings.PassivationStrategySettings.defaults.withIdleEntityPassivation(
          timeout = Duration.Zero))
        .passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
    }

    "disable automatic passivation if disabled (via factory method)" in {
      defaultSettings
        .withNoPassivationStrategy()
        .passivationStrategy shouldBe ClusterShardingSettings.NoPassivationStrategy
    }

    "support old `passivate-idle-entity-after` setting (overriding new strategy settings)" in {
      settings("""
        akka.cluster.sharding {
          passivate-idle-entity-after = 5 minutes
          passivation.strategy = default-strategy
        }
      """).passivationStrategy shouldBe ClusterShardingSettings.IdlePassivationStrategy(
        timeout = 5.minutes,
        interval = 2.5.minutes)
    }

  }
}
