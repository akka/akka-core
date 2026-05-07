/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.passivation.simulator

import scala.collection.immutable
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import com.typesafe.config.ConfigFactory

import akka.NotUsed
import akka.actor.ActorSystem
import akka.cluster.sharding.internal.ActiveEntities
import akka.cluster.sharding.internal.AdmissionFilter
import akka.cluster.sharding.internal.AdmissionOptimizer
import akka.cluster.sharding.internal.AlwaysAdmissionFilter
import akka.cluster.sharding.internal.CompositeEntityPassivationStrategy
import akka.cluster.sharding.internal.DisabledEntityPassivationStrategy
import akka.cluster.sharding.internal.EntityPassivationStrategy
import akka.cluster.sharding.internal.FrequencySketchAdmissionFilter
import akka.cluster.sharding.internal.HillClimbingAdmissionOptimizer
import akka.cluster.sharding.internal.LeastFrequentlyUsedEntityPassivationStrategy
import akka.cluster.sharding.internal.LeastFrequentlyUsedReplacementPolicy
import akka.cluster.sharding.internal.LeastRecentlyUsedEntityPassivationStrategy
import akka.cluster.sharding.internal.LeastRecentlyUsedReplacementPolicy
import akka.cluster.sharding.internal.MostRecentlyUsedEntityPassivationStrategy
import akka.cluster.sharding.internal.MostRecentlyUsedReplacementPolicy
import akka.cluster.sharding.internal.NoActiveEntities
import akka.cluster.sharding.internal.NoAdmissionOptimizer
import akka.cluster.sharding.internal.SegmentedLeastRecentlyUsedEntityPassivationStrategy
import akka.cluster.sharding.internal.SegmentedLeastRecentlyUsedReplacementPolicy
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import akka.util.Clock
import akka.util.OptionVal

/**
 * Simulator for testing the efficiency of passivation strategies.
 * For access pattern, can read cache replacement policy traces or generate synthetic events.
 */
object Simulator {
  def main(args: Array[String]): Unit = {
    val configName = args.headOption
    println(s"Running passivation simulator with [${configName.getOrElse("default application")}] config")
    val config = configName.fold(ConfigFactory.load)(ConfigFactory.load)
    run(SimulatorSettings(config))
  }

  final case class Results(name: String, stats: ShardingStats)

  def run(settings: SimulatorSettings): Unit = {
    implicit val system: ActorSystem = ActorSystem("simulator")
    val simulations = settings.runs.map(s => Simulation(s, () => Clock(system)))
    implicit val ec: ExecutionContext = system.dispatcher
    Source(simulations)
      .runFoldAsync(Seq.empty[Results]) { (results, simulation) =>
        println(s"Running [${simulation.name}] ...")
        simulate(simulation).map(stats => results :+ Results(simulation.name, stats))
      }
      .onComplete {
        case Success(allResults) =>
          val summary = allResults.map { results =>
            if (settings.printDetailedStats) {
              println(results.name)
              PrintData(DataTable(results.stats))
              println()
            }
            results.name +: DataTable.row(results.stats.totals)
          }
          PrintData(DataTable(DataTable.Headers.RunStats, summary))
          system.terminate()
        case Failure(exception) =>
          println(s"Failed to run simulations: $exception")
          system.terminate()
      }
  }

  final case class Simulation(
      name: String,
      numberOfShards: Int,
      numberOfRegions: Int,
      accessPattern: AccessPattern,
      strategyCreator: StrategyCreator)

  object Simulation {
    def apply(runSettings: SimulatorSettings.RunSettings, clock: () => Clock): Simulation =
      Simulation(
        name = runSettings.name,
        numberOfShards = runSettings.shards,
        numberOfRegions = runSettings.regions,
        accessPattern = accessPattern(runSettings.pattern),
        strategyCreator = strategyCreator(runSettings, clock))

    def accessPattern(patternSettings: SimulatorSettings.PatternSettings): AccessPattern = patternSettings match {
      case SimulatorSettings.PatternSettings.Synthetic(generator, events) =>
        generator match {
          case SimulatorSettings.PatternSettings.Synthetic.Sequence(start) =>
            new SyntheticGenerator.Sequence(start, events)
          case SimulatorSettings.PatternSettings.Synthetic.Loop(start, end) =>
            new SyntheticGenerator.Loop(start, end, events)
          case SimulatorSettings.PatternSettings.Synthetic.Uniform(min, max) =>
            new SyntheticGenerator.Uniform(min, max, events)
          case SimulatorSettings.PatternSettings.Synthetic.Exponential(mean) =>
            new SyntheticGenerator.Exponential(mean, events)
          case SimulatorSettings.PatternSettings.Synthetic.Hotspot(min, max, hot, rate) =>
            new SyntheticGenerator.Hotspot(min, max, hot, rate, events)
          case SimulatorSettings.PatternSettings.Synthetic.Zipfian(min, max, constant, scrambled) =>
            new SyntheticGenerator.Zipfian(min, max, constant, scrambled, events)
          case SimulatorSettings.PatternSettings.Synthetic.ShiftingZipfian(min, max, constant, shifts, scrambled) =>
            new SyntheticGenerator.ShiftingZipfian(min, max, constant, shifts, scrambled, events)
        }
      case SimulatorSettings.PatternSettings.Trace(path, format) =>
        format match {
          case "arc"       => new TraceFileReader.Arc(path)
          case "corda"     => new TraceFileReader.Corda(path)
          case "lirs"      => new TraceFileReader.Lirs(path)
          case "lirs2"     => new TraceFileReader.Lirs2(path)
          case "simple"    => new TraceFileReader.Simple(path)
          case "text"      => new TraceFileReader.Text(path)
          case "wikipedia" => new TraceFileReader.Wikipedia(path)
          case _           => sys.error(s"Unknown trace file format [$format]")
        }
      case SimulatorSettings.PatternSettings.Joined(patterns) =>
        new JoinedAccessPatterns(patterns.map(accessPattern))
    }

    def strategyCreator(runSettings: SimulatorSettings.RunSettings, clock: () => Clock): StrategyCreator =
      runSettings.strategy match {
        case SimulatorSettings.StrategySettings.Optimal(perRegionLimit) =>
          new ClairvoyantStrategyCreator(perRegionLimit)
        case SimulatorSettings.StrategySettings.LeastRecentlyUsed(perRegionLimit, segmented) =>
          new LeastRecentlyUsedStrategyCreator(perRegionLimit, segmented, clock)
        case SimulatorSettings.StrategySettings.MostRecentlyUsed(perRegionLimit) =>
          new MostRecentlyUsedStrategyCreator(perRegionLimit, clock())
        case SimulatorSettings.StrategySettings.LeastFrequentlyUsed(perRegionLimit, dynamicAging) =>
          new LeastFrequentlyUsedStrategyCreator(perRegionLimit, dynamicAging, clock)
        case settings: SimulatorSettings.StrategySettings.Composite =>
          new CompositeStrategyCreator(settings, clock)
        case SimulatorSettings.StrategySettings.NoStrategy =>
          DisabledStrategyCreator
      }
  }

  object Id {
    def hashed(id: String, n: Int): String =
      padded(math.abs(id.hashCode % n), (n - 1) max 1)

    def padded(id: Int, max: Int): String = {
      val maxDigits = math.floor(math.log10(max)).toInt + 1
      s"%0${maxDigits}d".format(id)
    }
  }

  type RegionId = String
  type ShardId = String
  type EntityId = String

  final case class Access(regionId: RegionId, shardId: ShardId, entityId: EntityId)

  sealed trait Event
  final case class Accessed(regionId: RegionId, shardId: ShardId, entityId: EntityId) extends Event
  final case class Activated(regionId: RegionId, shardId: ShardId, entityId: EntityId) extends Event
  final case class Passivated(regionId: RegionId, shardId: ShardId, entityIds: immutable.Seq[EntityId]) extends Event

  def simulate(simulation: Simulation)(implicit system: ActorSystem): Future[ShardingStats] =
    if (simulation.strategyCreator.requiresPreprocessing) runWithPreprocessing(simulation) else run(simulation)

  def run(simulation: Simulation)(implicit system: ActorSystem): Future[ShardingStats] =
    simulation.accessPattern.entityIds
      .via(ShardAllocation(simulation.numberOfShards, simulation.numberOfRegions))
      .via(ShardingState(simulation.strategyCreator))
      .runWith(SimulatorStats())

  // note: may need a lot of extra memory, if accesses from the simulation are collected during preprocessing
  def runWithPreprocessing(simulation: Simulation)(implicit system: ActorSystem): Future[ShardingStats] =
    simulation.accessPattern.entityIds
      .via(ShardAllocation(simulation.numberOfShards, simulation.numberOfRegions))
      .map(simulation.strategyCreator.preprocess) // note: mutable state in strategy creator
      .fold(immutable.Queue.empty[Access])((collected, access) =>
        if (simulation.accessPattern.isSynthetic) collected.enqueue(access) else collected)
      .flatMapConcat(
        collectedAccesses =>
          if (simulation.accessPattern.isSynthetic)
            Source(collectedAccesses) // use the exact same randomly generated accesses
          else
            simulation.accessPattern.entityIds.via( // re-read the access pattern
              ShardAllocation(simulation.numberOfShards, simulation.numberOfRegions)))
      .via(ShardingState(simulation.strategyCreator))
      .runWith(SimulatorStats())

  object ShardAllocation {
    def apply(numberOfShards: Int, numberOfRegions: Int): Flow[EntityId, Access, NotUsed] =
      Flow[EntityId].statefulMapConcat(() => {
        val allocation = new ShardAllocation(numberOfShards, numberOfRegions)
        entityId => List(allocation.access(entityId))
      })
  }

  final class ShardAllocation(numberOfShards: Int, numberOfRegions: Int) {
    private var allocationMap: Map[RegionId, Set[ShardId]] = (1 to numberOfRegions).map { id =>
      Id.padded(id, numberOfRegions) -> Set.empty[ShardId]
    }.toMap

    def access(entityId: EntityId): Access = {
      val shardId = extractShardId(entityId)
      val regionId = currentRegion(shardId).getOrElse(allocateShard(shardId))
      Access(regionId, shardId, entityId)
    }

    // simulate default shard id extractor
    private def extractShardId(entityId: EntityId): ShardId =
      Id.hashed(entityId, numberOfShards)

    private def currentRegion(shardId: ShardId): Option[RegionId] =
      allocationMap.collectFirst { case (regionId, allocated) if allocated.contains(shardId) => regionId }

    // simulate default least shard allocation
    private def allocateShard(shardId: ShardId): RegionId = {
      val regionId = allocationMap.toSeq.sortBy(_._1).minBy(_._2)(Ordering.by(_.size))._1
      allocationMap = allocationMap.updated(regionId, allocationMap(regionId) + shardId)
      regionId
    }
  }

  object ShardingState {
    def apply(strategyCreator: StrategyCreator): Flow[Access, Event, NotUsed] =
      Flow[Access].statefulMapConcat(() => {
        val state = new ShardingState(strategyCreator)
        access => state.process(access)
      })
  }

  final class ShardingState(strategyCreator: StrategyCreator) {
    private val active = mutable.Map.empty[RegionId, mutable.Map[ShardId, ShardState]]

    private def createShardState(regionId: RegionId, shardId: ShardId): ShardState =
      new ShardState(regionId, shardId, strategyCreator.create(shardId))

    def process(access: Access): Seq[Event] = {
      val regionId = access.regionId
      val shardId = access.shardId
      val region = active.getOrElseUpdate(regionId, mutable.Map.empty)
      val alreadyActive = region.contains(shardId)
      val shard = region.getOrElseUpdate(shardId, createShardState(regionId, shardId))
      val passivated = if (!alreadyActive) region.values.toSeq.flatMap(_.updated(region.size)) else Nil
      passivated ++ shard.accessed(access.entityId)
    }
  }

  final class ShardState(regionId: RegionId, shardId: ShardId, strategy: SimulatedStrategy) {
    private val activeEntities = mutable.Set.empty[EntityId]

    def updated(activeShards: Int): immutable.Seq[Event] = {
      val passivateEntities = strategy.shardsUpdated(activeShards)
      passivateEntities.foreach(activeEntities.remove)
      if (passivateEntities.isEmpty) Nil
      else List(Passivated(regionId, shardId, passivateEntities))
    }

    def accessed(entityId: EntityId): immutable.Seq[Event] = {
      val changes = if (activeEntities.contains(entityId)) {
        strategy.entityTouched(entityId)
        Nil
      } else {
        activeEntities += entityId
        val passivateEntities = strategy.entityTouched(entityId)
        passivateEntities.foreach(activeEntities.remove)
        val passivated =
          if (passivateEntities.isEmpty) Nil
          else List(Passivated(regionId, shardId, passivateEntities))
        passivated :+ Activated(regionId, shardId, entityId)
      }
      changes :+ Accessed(regionId, shardId, entityId)
    }
  }

  sealed trait SimulatedStrategy {
    def shardsUpdated(activeShards: Int): immutable.Seq[EntityId]
    def entityTouched(id: EntityId): immutable.Seq[EntityId]
  }

  final class PassivationStrategy(strategy: EntityPassivationStrategy) extends SimulatedStrategy {
    override def shardsUpdated(activeShards: Int): immutable.Seq[EntityId] = strategy.shardsUpdated(activeShards)
    override def entityTouched(id: EntityId): immutable.Seq[EntityId] = strategy.entityTouched(id)
  }

  sealed trait StrategyCreator {
    def create(shardId: ShardId): SimulatedStrategy
    def requiresPreprocessing: Boolean
    def preprocess(access: Access): Access
  }

  sealed abstract class PassivationStrategyCreator extends StrategyCreator {
    override val requiresPreprocessing = false
    override def preprocess(access: Access): Access = access
  }

  final class LeastRecentlyUsedStrategyCreator(
      perRegionLimit: Int,
      segmented: immutable.Seq[Double],
      clock: () => Clock)
      extends PassivationStrategyCreator {
    override def create(shardId: ShardId): SimulatedStrategy =
      new PassivationStrategy(
        if (segmented.nonEmpty)
          new SegmentedLeastRecentlyUsedEntityPassivationStrategy(perRegionLimit, segmented, idleCheck = None, clock)
        else new LeastRecentlyUsedEntityPassivationStrategy(perRegionLimit, idleCheck = None, clock()))
  }

  final class MostRecentlyUsedStrategyCreator(perRegionLimit: Int, clock: Clock) extends PassivationStrategyCreator {
    override def create(shardId: ShardId): SimulatedStrategy =
      new PassivationStrategy(new MostRecentlyUsedEntityPassivationStrategy(perRegionLimit, idleCheck = None, clock))
  }

  final class LeastFrequentlyUsedStrategyCreator(perRegionLimit: Int, dynamicAging: Boolean, clock: () => Clock)
      extends PassivationStrategyCreator {
    override def create(shardId: ShardId): SimulatedStrategy =
      new PassivationStrategy(
        new LeastFrequentlyUsedEntityPassivationStrategy(perRegionLimit, dynamicAging, idleCheck = None, clock))
  }

  final class CompositeStrategyCreator(settings: SimulatorSettings.StrategySettings.Composite, clock: () => Clock)
      extends PassivationStrategyCreator {
    override def create(shardId: ShardId): SimulatedStrategy = {
      val main = activeEntities(settings.main, clock)
      val window = activeEntities(settings.window, clock)
      val initialWindowProportion = if (window eq NoActiveEntities) 0.0 else settings.initialWindowProportion
      val minimumWindowProportion = if (window eq NoActiveEntities) 0.0 else settings.minimumWindowProportion
      val maximumWindowProportion = if (window eq NoActiveEntities) 0.0 else settings.maximumWindowProportion
      val windowOptimizer =
        if (window eq NoActiveEntities) NoAdmissionOptimizer
        else admissionOptimizer(settings.perRegionLimit, settings.optimizer)
      val admission = admissionFilter(settings.perRegionLimit, settings.filter)
      new PassivationStrategy(
        new CompositeEntityPassivationStrategy(
          settings.perRegionLimit,
          window,
          initialWindowProportion,
          minimumWindowProportion,
          maximumWindowProportion,
          windowOptimizer,
          admission,
          main,
          idleCheck = None))
    }

    private def activeEntities(
        strategySettings: SimulatorSettings.StrategySettings,
        clock: () => Clock): ActiveEntities =
      strategySettings match {
        case SimulatorSettings.StrategySettings.LeastRecentlyUsed(perRegionLimit, segmented) if segmented.isEmpty =>
          new LeastRecentlyUsedReplacementPolicy(perRegionLimit, clock())
        case SimulatorSettings.StrategySettings.LeastRecentlyUsed(perRegionLimit, segmented) =>
          new SegmentedLeastRecentlyUsedReplacementPolicy(perRegionLimit, segmented, idleEnabled = false, clock)
        case SimulatorSettings.StrategySettings.MostRecentlyUsed(perRegionLimit) =>
          new MostRecentlyUsedReplacementPolicy(perRegionLimit, clock())
        case SimulatorSettings.StrategySettings.LeastFrequentlyUsed(perRegionLimit, dynamicAging) =>
          new LeastFrequentlyUsedReplacementPolicy(perRegionLimit, dynamicAging, idleEnabled = false, clock)
        case _ => NoActiveEntities
      }

    private def admissionOptimizer(
        capacity: Int,
        optimizerSettings: SimulatorSettings.StrategySettings.AdmissionOptimizerSettings): AdmissionOptimizer =
      optimizerSettings match {
        case SimulatorSettings.StrategySettings.AdmissionOptimizerSettings.NoOptimizer => NoAdmissionOptimizer
        case SimulatorSettings.StrategySettings.AdmissionOptimizerSettings
              .HillClimbingOptimizer(adjustMultiplier, initialStep, restartThreshold, stepDecay) =>
          new HillClimbingAdmissionOptimizer(capacity, adjustMultiplier, initialStep, restartThreshold, stepDecay)
      }

    private def admissionFilter(
        capacity: Int,
        filterSettings: SimulatorSettings.StrategySettings.AdmissionFilterSettings): AdmissionFilter =
      filterSettings match {
        case SimulatorSettings.StrategySettings.AdmissionFilterSettings.NoFilter => AlwaysAdmissionFilter
        case SimulatorSettings.StrategySettings.AdmissionFilterSettings
              .FrequencySketchFilter(widthMultiplier, resetMultiplier, depth, counterBits) =>
          FrequencySketchAdmissionFilter(capacity, widthMultiplier, resetMultiplier, depth, counterBits)
      }
  }

  object DisabledStrategyCreator extends PassivationStrategyCreator {
    override def create(shardId: ShardId): SimulatedStrategy =
      new PassivationStrategy(DisabledEntityPassivationStrategy)
  }

  // Clairvoyant passivation strategy using Bélády's algorithm.
  // Record virtual access times per entity id on a first pass through the access pattern,
  // to passivate entities that will not be accessed again for the furthest time in the future.
  // Note: running this requires a lot of extra memory for large workloads.

  final class ClairvoyantAccessRecorder {
    private type AccessTime = Int
    private var tick: AccessTime = 0
    private val futureAccesses = mutable.Map.empty[EntityId, mutable.Queue[AccessTime]]

    def accessed(id: EntityId): Unit = {
      tick += 1
      futureAccesses.getOrElseUpdate(id, mutable.Queue.empty[AccessTime]) += tick
    }

    def nextAccess(id: EntityId): OptionVal[AccessTime] = {
      val accesses = futureAccesses(id)
      if (accesses.nonEmpty) OptionVal.Some(accesses.head)
      else {
        futureAccesses -= id
        OptionVal.none
      }
    }

    def previousAccess(id: EntityId): AccessTime = futureAccesses(id).dequeue()
  }

  final class ClairvoyantStrategyCreator(perRegionLimit: Int) extends StrategyCreator {
    override val requiresPreprocessing = true

    private val recorders = mutable.Map.empty[ShardId, ClairvoyantAccessRecorder]

    override def preprocess(access: Access): Access = {
      recorders.getOrElseUpdate(access.shardId, new ClairvoyantAccessRecorder).accessed(access.entityId)
      access
    }

    override def create(shardId: ShardId): SimulatedStrategy =
      new ClairvoyantPassivationStrategy(perRegionLimit, recorders.getOrElse(shardId, new ClairvoyantAccessRecorder))
  }

  final class ClairvoyantPassivationStrategy(perRegionLimit: Int, recorder: ClairvoyantAccessRecorder)
      extends SimulatedStrategy {

    private type AccessTime = Int
    private var perShardLimit: Int = perRegionLimit
    private val nextAccess = mutable.TreeMap.empty[AccessTime, EntityId]
    private var never: AccessTime = Int.MaxValue

    override def shardsUpdated(activeShards: Int): immutable.Seq[EntityId] = {
      perShardLimit = perRegionLimit / activeShards
      passivateExcessEntities()
    }

    override def entityTouched(id: EntityId): immutable.Seq[EntityId] = {
      nextAccess -= recorder.previousAccess(id)
      recorder.nextAccess(id) match {
        case OptionVal.Some(access) =>
          nextAccess += access -> id
        case _ =>
          never -= 1
          nextAccess += never -> id
      }
      passivateExcessEntities()
    }

    private def passivateExcessEntities(): immutable.Seq[EntityId] = {
      val passivated = mutable.ListBuffer.empty[EntityId]
      while (nextAccess.size > perShardLimit) {
        nextAccess.remove(nextAccess.lastKey).foreach(passivated.+=)
      }
      passivated.result()
    }
  }
}
