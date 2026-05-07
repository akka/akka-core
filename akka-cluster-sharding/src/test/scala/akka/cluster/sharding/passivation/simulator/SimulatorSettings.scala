/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation.simulator

import java.util.Locale

import scala.collection.immutable

import com.typesafe.config.Config

import akka.japi.Util.immutableSeq
import scala.jdk.CollectionConverters._

final case class SimulatorSettings(runs: immutable.Seq[SimulatorSettings.RunSettings], printDetailedStats: Boolean)

object SimulatorSettings {
  def apply(testConfig: Config): SimulatorSettings = {
    val simulatorConfig = testConfig.getConfig("akka.cluster.sharding.passivation.simulator")
    val runDefaults = simulatorConfig.getConfig("run-defaults")
    val runs = immutableSeq(simulatorConfig.getConfigList("runs")).map { runConfig =>
      RunSettings(simulatorConfig, runConfig.withFallback(runDefaults))
    }
    val printDetailedStats = simulatorConfig.getBoolean("print-detailed-stats")
    SimulatorSettings(runs, printDetailedStats)
  }

  final case class RunSettings(
      name: String,
      shards: Int,
      regions: Int,
      strategy: StrategySettings,
      pattern: PatternSettings)

  object RunSettings {
    def apply(simulatorConfig: Config, runConfig: Config): RunSettings = {
      val name = runConfig.getString("name")
      val shards = runConfig.getInt("shards")
      val regions = runConfig.getInt("regions")
      val strategy = StrategySettings(simulatorConfig, runConfig.getString("strategy"))
      val pattern = PatternSettings(simulatorConfig, runConfig.getString("pattern"))
      RunSettings(name, shards, regions, strategy, pattern)
    }
  }

  sealed trait StrategySettings

  object StrategySettings {
    final case class Optimal(perRegionLimit: Int) extends StrategySettings
    final case class LeastRecentlyUsed(perRegionLimit: Int, segmented: immutable.Seq[Double]) extends StrategySettings
    final case class MostRecentlyUsed(perRegionLimit: Int) extends StrategySettings
    final case class LeastFrequentlyUsed(perRegionLimit: Int, dynamicAging: Boolean) extends StrategySettings
    case object NoStrategy extends StrategySettings

    final case class Composite(
        perRegionLimit: Int,
        main: StrategySettings,
        window: StrategySettings,
        initialWindowProportion: Double,
        minimumWindowProportion: Double,
        maximumWindowProportion: Double,
        filter: AdmissionFilterSettings,
        optimizer: AdmissionOptimizerSettings)
        extends StrategySettings

    def apply(simulatorConfig: Config, strategy: String): StrategySettings = {
      val config = simulatorConfig.getConfig(strategy)
      val fallbackConfig = simulatorConfig.getConfig("strategy-defaults")
      lowerCase(config.getString("strategy")) match {
        case "composite" =>
          val compositeConfig = config.getConfig("composite").withFallback(fallbackConfig.getConfig("composite"))
          Composite(
            compositeConfig.getInt("per-region-limit"),
            settings(compositeConfig.getConfig("main"), fallbackConfig),
            settings(compositeConfig.getConfig("admission.window"), fallbackConfig),
            compositeConfig.getDouble("admission.window.proportion"),
            compositeConfig.getDouble("admission.window.minimum-proportion"),
            compositeConfig.getDouble("admission.window.maximum-proportion"),
            AdmissionFilterSettings(compositeConfig),
            AdmissionOptimizerSettings(compositeConfig))
        case _ => settings(config, fallbackConfig)
      }
    }

    private def settings(strategyConfig: Config, fallbackConfig: Config): StrategySettings = {
      val config = strategyConfig.withFallback(fallbackConfig)
      lowerCase(config.getString("strategy")) match {
        case "optimal" => Optimal(config.getInt("optimal.per-region-limit"))
        case "least-recently-used" =>
          val limit = config.getInt("least-recently-used.per-region-limit")
          val segmented = lowerCase(config.getString("least-recently-used.segmented.levels")) match {
            case "off" | "none" => Nil
            case _ =>
              val levels = config.getInt("least-recently-used.segmented.levels")
              val proportions =
                config.getDoubleList("least-recently-used.segmented.proportions").asScala.map(_.toDouble).toList
              if (proportions.isEmpty) List.fill(levels)(1.0 / levels) else proportions
          }
          LeastRecentlyUsed(limit, segmented)
        case "most-recently-used" => MostRecentlyUsed(config.getInt("most-recently-used.per-region-limit"))
        case "least-frequently-used" =>
          LeastFrequentlyUsed(
            config.getInt("least-frequently-used.per-region-limit"),
            config.getBoolean("least-frequently-used.dynamic-aging"))
        case _ => NoStrategy
      }
    }

    sealed trait AdmissionFilterSettings

    object AdmissionFilterSettings {
      object NoFilter extends AdmissionFilterSettings
      final case class FrequencySketchFilter(
          widthMultiplier: Int,
          resetMultiplier: Double,
          depth: Int,
          counterBits: Int)
          extends AdmissionFilterSettings

      def apply(config: Config): AdmissionFilterSettings = {
        lowerCase(config.getString("admission.filter")) match {
          case "frequency-sketch" =>
            FrequencySketchFilter(
              config.getInt("admission.frequency-sketch.width-multiplier"),
              config.getDouble("admission.frequency-sketch.reset-multiplier"),
              config.getInt("admission.frequency-sketch.depth"),
              config.getInt("admission.frequency-sketch.counter-bits"))
          case _ => NoFilter
        }
      }
    }

    sealed trait AdmissionOptimizerSettings

    object AdmissionOptimizerSettings {
      object NoOptimizer extends AdmissionOptimizerSettings
      final case class HillClimbingOptimizer(
          adjustMultiplier: Double,
          initialStep: Double,
          restartThreshold: Double,
          stepDecay: Double)
          extends AdmissionOptimizerSettings

      def apply(config: Config): AdmissionOptimizerSettings = {
        lowerCase(config.getString("admission.optimizer")) match {
          case "hill-climbing" =>
            HillClimbingOptimizer(
              config.getDouble("admission.hill-climbing.adjust-multiplier"),
              config.getDouble("admission.hill-climbing.initial-step"),
              config.getDouble("admission.hill-climbing.restart-threshold"),
              config.getDouble("admission.hill-climbing.step-decay"))
          case _ => NoOptimizer
        }
      }
    }
  }

  sealed trait PatternSettings

  object PatternSettings {
    final case class Synthetic(generator: Synthetic.Generator, events: Int) extends PatternSettings

    object Synthetic {
      sealed trait Generator
      final case class Sequence(start: Long) extends Generator
      final case class Loop(start: Long, end: Long) extends Generator
      final case class Uniform(min: Long, max: Long) extends Generator
      final case class Exponential(mean: Double) extends Generator
      final case class Hotspot(min: Long, max: Long, hot: Double, rate: Double) extends Generator
      final case class Zipfian(min: Long, max: Long, constant: Double, scrambled: Boolean) extends Generator
      final case class ShiftingZipfian(min: Long, max: Long, constant: Double, shifts: Int, scrambled: Boolean)
          extends Generator

      def apply(patternConfig: Config): Synthetic = {
        val config = patternConfig.getConfig("synthetic")
        val generator = lowerCase(config.getString("generator")) match {
          case "sequence" =>
            val start = config.getLong("sequence.start")
            Sequence(start)
          case "loop" =>
            val start = config.getLong("loop.start")
            val end = config.getLong("loop.end")
            Loop(start, end)
          case "uniform" =>
            val min = config.getLong("uniform.min")
            val max = config.getLong("uniform.max")
            Uniform(min, max)
          case "exponential" =>
            val mean = config.getDouble("exponential.mean")
            Exponential(mean)
          case "hotspot" =>
            val min = config.getLong("hotspot.min")
            val max = config.getLong("hotspot.max")
            val hot = config.getDouble("hotspot.hot")
            val rate = config.getDouble("hotspot.rate")
            Hotspot(min, max, hot, rate)
          case "zipfian" =>
            val min = config.getLong("zipfian.min")
            val max = config.getLong("zipfian.max")
            val constant = config.getDouble("zipfian.constant")
            val scrambled = config.getBoolean("zipfian.scrambled")
            if (lowerCase(config.getString("zipfian.shifts")) != "off") {
              val shifts = config.getInt("zipfian.shifts")
              ShiftingZipfian(min, max, constant, shifts, scrambled)
            } else {
              Zipfian(min, max, constant, scrambled)
            }
        }
        Synthetic(generator, config.getInt("events"))
      }
    }

    final case class Trace(path: String, format: String) extends PatternSettings

    object Trace {
      def apply(patternConfig: Config): Trace = {
        val config = patternConfig.getConfig("trace")
        val path = config.getString("path")
        val format = lowerCase(config.getString("format"))
        Trace(path, format)
      }
    }

    final case class Joined(patterns: Seq[PatternSettings]) extends PatternSettings

    object Joined {
      def apply(simulatorConfig: Config, patternConfig: Config): Joined = {
        val patterns = patternConfig.getStringList("joined").asScala.toSeq
        Joined(patterns.map(pattern => PatternSettings(simulatorConfig, pattern)))
      }
    }

    def apply(simulatorConfig: Config, pattern: String): PatternSettings = {
      val config = simulatorConfig.getConfig(pattern).withFallback(simulatorConfig.getConfig("pattern-defaults"))
      lowerCase(config.getString("pattern")) match {
        case "synthetic" => Synthetic(config)
        case "trace"     => Trace(config)
        case "joined"    => Joined(simulatorConfig, config)
        case _           => sys.error(s"Unknown pattern for [$pattern]")
      }
    }
  }

  private def lowerCase(s: String): String = s.toLowerCase(Locale.ROOT)
}
