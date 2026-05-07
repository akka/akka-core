/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.internal

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.cluster.sharding.ClusterShardingSettings
import akka.cluster.sharding.ShardRegion.EntityId
import akka.util.{ FrequencyList, RecencyList, SegmentedRecencyList }
import akka.util.Clock
import akka.util.FastFrequencySketch
import akka.util.FrequencySketch
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EntityPassivationStrategy {
  type PassivateEntities = immutable.Seq[EntityId]

  object PassivateEntities {
    val none: PassivateEntities = immutable.Seq.empty[EntityId]
  }

  def apply(settings: ClusterShardingSettings, clock: () => Clock): EntityPassivationStrategy = {
    settings.passivationStrategy match {
      case ClusterShardingSettings.IdlePassivationStrategy(timeout, interval) =>
        new IdleEntityPassivationStrategy(new IdleCheck(timeout, interval), clock())
      case ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(limit, segmented, idle) =>
        val idleCheck = idle.map(idle => new IdleCheck(idle.timeout, idle.interval))
        if (segmented.isEmpty) new LeastRecentlyUsedEntityPassivationStrategy(limit, idleCheck, clock())
        else new SegmentedLeastRecentlyUsedEntityPassivationStrategy(limit, segmented, idleCheck, clock)
      case ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(limit, idle) =>
        val idleCheck = idle.map(idle => new IdleCheck(idle.timeout, idle.interval))
        new MostRecentlyUsedEntityPassivationStrategy(limit, idleCheck, clock())
      case ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(limit, dynamicAging, idle) =>
        val idleCheck = idle.map(idle => new IdleCheck(idle.timeout, idle.interval))
        new LeastFrequentlyUsedEntityPassivationStrategy(limit, dynamicAging, idleCheck, clock)
      case composite: ClusterShardingSettings.CompositePassivationStrategy =>
        val main = ActiveEntities(composite.mainStrategy, composite.idle.isDefined, clock)
        if (main eq NoActiveEntities) DisabledEntityPassivationStrategy
        else {
          val initialLimit = composite.limit
          val window = ActiveEntities(composite.windowStrategy, composite.idle.isDefined, clock)
          val initialWindowProportion = if (window eq NoActiveEntities) 0.0 else composite.initialWindowProportion
          val minimumWindowProportion = if (window eq NoActiveEntities) 0.0 else composite.minimumWindowProportion
          val maximumWindowProportion = if (window eq NoActiveEntities) 0.0 else composite.maximumWindowProportion
          val windowOptimizer =
            if (window eq NoActiveEntities) NoAdmissionOptimizer
            else AdmissionOptimizer(composite.limit, composite.windowOptimizer)
          val admissionFilter = AdmissionFilter(composite.limit, composite.admissionFilter)
          val idleCheck = composite.idle.map(idle => new IdleCheck(idle.timeout, idle.interval))
          new CompositeEntityPassivationStrategy(
            initialLimit,
            window,
            initialWindowProportion,
            minimumWindowProportion,
            maximumWindowProportion,
            windowOptimizer,
            admissionFilter,
            main,
            idleCheck)
        }
      case _ => DisabledEntityPassivationStrategy
    }
  }
}

/**
 * INTERNAL API: An entity passivation strategy, which is instantiated per active shard.
 */
@InternalApi
private[akka] sealed abstract class EntityPassivationStrategy {
  import EntityPassivationStrategy.PassivateEntities

  /**
   * The per-region active entity limit has been updated, which can trigger passivation.
   * @param newLimit the new per-region active entity limit
   * @return entities to passivate in the associated shard
   */
  def limitUpdated(newLimit: Int): PassivateEntities

  /**
   * Active shards in this region have been updated, which can trigger passivation.
   * @param activeShards updated number of active shards
   * @return entities to passivate in the associated shard
   */
  def shardsUpdated(activeShards: Int): PassivateEntities

  /**
   * An entity instance has been touched. Recorded before message delivery.
   * @param id entity id for the touched entity instance
   * @return entities to passivate, when active capacity has been reached
   */
  def entityTouched(id: EntityId): PassivateEntities

  /**
   * An entity instance has been terminated and should be removed from active tracking.
   * @param id entity id for the terminated entity instance
   */
  def entityTerminated(id: EntityId): Unit

  /**
   * An optional interval for time-based passivation strategies.
   * @return the scheduled interval to call the `intervalPassed` method
   */
  def scheduledInterval: Option[FiniteDuration]

  /**
   * Called each time the `scheduledInterval` has passed, if defined.
   * @return entities to passivate, if deemed inactive
   */
  def intervalPassed(): PassivateEntities
}

/**
 * INTERNAL API: No-op passivation strategy for when automatic passivation is disabled.
 */
@InternalApi
private[akka] object DisabledEntityPassivationStrategy extends EntityPassivationStrategy {
  import EntityPassivationStrategy.PassivateEntities

  override def limitUpdated(newLimit: Int): PassivateEntities = PassivateEntities.none
  override def shardsUpdated(activeShards: Int): PassivateEntities = PassivateEntities.none
  override def entityTouched(id: EntityId): PassivateEntities = PassivateEntities.none
  override def entityTerminated(id: EntityId): Unit = ()
  override def scheduledInterval: Option[FiniteDuration] = None
  override def intervalPassed(): PassivateEntities = PassivateEntities.none
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class IdleCheck(val timeout: FiniteDuration, val interval: FiniteDuration)

/**
 * INTERNAL API: Passivates entities when they have not received a message for a specified length of time.
 * @param idleCheck passivate idle entities after the given timeout, checking every interval
 */
@InternalApi
private[akka] final class IdleEntityPassivationStrategy(idleCheck: IdleCheck, clock: Clock)
    extends EntityPassivationStrategy {

  import EntityPassivationStrategy.PassivateEntities

  private val recencyList = RecencyList[EntityId](clock)

  override val scheduledInterval: Option[FiniteDuration] = Some(idleCheck.interval)

  override def limitUpdated(newLimit: Int): PassivateEntities = PassivateEntities.none

  override def shardsUpdated(activeShards: Int): PassivateEntities = PassivateEntities.none

  override def entityTouched(id: EntityId): PassivateEntities = {
    recencyList.update(id)
    PassivateEntities.none
  }

  override def entityTerminated(id: EntityId): Unit = recencyList.remove(id)

  override def intervalPassed(): PassivateEntities = recencyList.removeLeastRecentOutside(idleCheck.timeout)
}

/**
 * INTERNAL API: Shared base class for limit-based passivation strategies.
 * @param initialLimit initial active entity capacity for a shard region
 */
@InternalApi
private[akka] abstract class LimitBasedEntityPassivationStrategy(initialLimit: Int) extends EntityPassivationStrategy {
  import EntityPassivationStrategy.PassivateEntities

  protected var activeShards: Int = 1
  protected var perRegionLimit: Int = initialLimit
  protected var perShardLimit: Int = perRegionLimit

  override def limitUpdated(newPerRegionLimit: Int): PassivateEntities = {
    perRegionLimit = newPerRegionLimit
    perShardLimit = perRegionLimit / activeShards
    passivateEntitiesOnLimitUpdate()
  }

  override def shardsUpdated(newActiveShards: Int): PassivateEntities = {
    activeShards = newActiveShards
    perShardLimit = perRegionLimit / activeShards
    passivateEntitiesOnLimitUpdate()
  }

  protected def passivateEntitiesOnLimitUpdate(): PassivateEntities
}

/**
 * INTERNAL API
 *
 * Passivate the least recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard region
 * @param idleCheck optionally passivate idle entities after the given timeout, checking every interval
 */
private[akka] final class LeastRecentlyUsedEntityPassivationStrategy(
    initialLimit: Int,
    idleCheck: Option[IdleCheck],
    clock: Clock)
    extends LimitBasedEntityPassivationStrategy(initialLimit) {

  import EntityPassivationStrategy.PassivateEntities

  val active = new LeastRecentlyUsedReplacementPolicy(initialLimit, clock)

  override protected def passivateEntitiesOnLimitUpdate(): PassivateEntities = active.updateLimit(perShardLimit)

  override def entityTouched(id: EntityId): PassivateEntities = active.update(id)

  override def entityTerminated(id: EntityId): Unit = active.remove(id)

  override def scheduledInterval: Option[FiniteDuration] = idleCheck.map(_.interval)

  override def intervalPassed(): PassivateEntities = idleCheck.fold(PassivateEntities.none) { idle =>
    active.removeIdle(idle.timeout)
  }
}

/**
 * INTERNAL API
 *
 * Passivate the least recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 * Active entities are tracked in multiple recency lists, where entities are promoted to higher-level
 * segments on subsequent accesses, and demoted through levels when segments become full.
 * The proportions of the segmented levels can be configured as fractions of the overall limit.
 *
 * @param initialLimit initial active entity capacity for a shard region
 * @param proportions proportions of the segmented levels
 * @param idleCheck optionally passivate idle entities after the given timeout, checking every interval
 */
@InternalApi
private[akka] final class SegmentedLeastRecentlyUsedEntityPassivationStrategy(
    initialLimit: Int,
    proportions: immutable.Seq[Double],
    idleCheck: Option[IdleCheck],
    clock: () => Clock)
    extends LimitBasedEntityPassivationStrategy(initialLimit) {

  import EntityPassivationStrategy.PassivateEntities

  val active = new SegmentedLeastRecentlyUsedReplacementPolicy(initialLimit, proportions, idleCheck.isDefined, clock)

  override protected def passivateEntitiesOnLimitUpdate(): PassivateEntities = active.updateLimit(perShardLimit)

  override def entityTouched(id: EntityId): PassivateEntities = active.update(id)

  override def entityTerminated(id: EntityId): Unit = active.remove(id)

  override def scheduledInterval: Option[FiniteDuration] = idleCheck.map(_.interval)

  override def intervalPassed(): PassivateEntities = idleCheck.fold(PassivateEntities.none) { idle =>
    active.removeIdle(idle.timeout)
  }
}

/**
 * INTERNAL API
 *
 * Passivate the most recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard region
 * @param idleCheck optionally passivate idle entities after the given timeout, checking every interval
 */
@InternalApi
private[akka] final class MostRecentlyUsedEntityPassivationStrategy(
    initialLimit: Int,
    idleCheck: Option[IdleCheck],
    clock: Clock)
    extends LimitBasedEntityPassivationStrategy(initialLimit) {

  import EntityPassivationStrategy.PassivateEntities

  val active = new MostRecentlyUsedReplacementPolicy(initialLimit, clock)

  override protected def passivateEntitiesOnLimitUpdate(): PassivateEntities = active.updateLimit(perShardLimit)

  override def entityTouched(id: EntityId): PassivateEntities = active.update(id)

  override def entityTerminated(id: EntityId): Unit = active.remove(id)

  override def scheduledInterval: Option[FiniteDuration] = idleCheck.map(_.interval)

  override def intervalPassed(): PassivateEntities = idleCheck.fold(PassivateEntities.none) { idle =>
    active.removeIdle(idle.timeout)
  }
}

/**
 * INTERNAL API
 *
 * Passivate the least frequently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard region
 * @param dynamicAging whether to apply "dynamic aging" as entities are passivated
 * @param idleCheck optionally passivate idle entities after the given timeout, checking every interval
 */
@InternalApi
private[akka] final class LeastFrequentlyUsedEntityPassivationStrategy(
    initialLimit: Int,
    dynamicAging: Boolean,
    idleCheck: Option[IdleCheck],
    clock: () => Clock)
    extends LimitBasedEntityPassivationStrategy(initialLimit) {

  import EntityPassivationStrategy.PassivateEntities

  val active = new LeastFrequentlyUsedReplacementPolicy(initialLimit, dynamicAging, idleCheck.isDefined, clock)

  override protected def passivateEntitiesOnLimitUpdate(): PassivateEntities = active.updateLimit(perShardLimit)

  override def entityTouched(id: EntityId): PassivateEntities = active.update(id)

  override def entityTerminated(id: EntityId): Unit = active.remove(id)

  override def scheduledInterval: Option[FiniteDuration] = idleCheck.map(_.interval)

  override def intervalPassed(): PassivateEntities = idleCheck.fold(PassivateEntities.none) { idle =>
    active.removeIdle(idle.timeout)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ActiveEntities {
  def apply(
      strategy: ClusterShardingSettings.PassivationStrategy,
      idleEnabled: Boolean,
      clock: () => Clock): ActiveEntities =
    strategy match {
      case ClusterShardingSettings.LeastRecentlyUsedPassivationStrategy(_, segmented, _) =>
        if (segmented.isEmpty) new LeastRecentlyUsedReplacementPolicy(initialLimit = 0, clock())
        else new SegmentedLeastRecentlyUsedReplacementPolicy(initialLimit = 0, segmented, idleEnabled, clock)
      case ClusterShardingSettings.MostRecentlyUsedPassivationStrategy(_, _) =>
        new MostRecentlyUsedReplacementPolicy(initialLimit = 0, clock())
      case ClusterShardingSettings.LeastFrequentlyUsedPassivationStrategy(_, dynamicAging, _) =>
        new LeastFrequentlyUsedReplacementPolicy(initialLimit = 0, dynamicAging, idleEnabled, clock)
      case _ => NoActiveEntities
    }
}

/**
 * INTERNAL API
 *
 * Active entity tracking for entity passivation strategies, implemented with a replacement policy.
 */
@InternalApi
private[akka] sealed abstract class ActiveEntities {
  import EntityPassivationStrategy.PassivateEntities

  /**
   * The current number of active entities being tracked.
   * @return size of active entities
   */
  def size: Int

  /**
   * Check whether the entity id is currently tracked as active.
   * @param id the entity id to check
   * @return whether the entity is active
   */
  def isActive(id: EntityId): Boolean

  /**
   * The per-shard active entity limit has been updated, which can trigger passivation.
   * @param newLimit the new per-shard active entity limit
   * @return entities to passivate in the associated shard
   */
  def updateLimit(newLimit: Int): PassivateEntities

  /**
   * An entity instance has been touched. Recorded before message delivery.
   * @param id entity id for the touched entity instance
   * @return entities to passivate, when active capacity has been reached
   */
  def update(id: EntityId): PassivateEntities

  /**
   * Select the entity that would be passivated by the replacement policy, when active capacity has been reached.
   * @return entity that would be passivated
   */
  def select: OptionVal[EntityId]

  /**
   * An entity instance should be removed from active tracking.
   * @param id entity id for the removed entity instance
   */
  def remove(id: EntityId): Unit

  /**
   * Remove entity instances that have not been active for the given timeout.
   * @param timeout the idle timeout for entities
   * @return entities to passivate, if deemed inactive
   */
  def removeIdle(timeout: FiniteDuration): PassivateEntities
}

/**
 * INTERNAL API
 *
 * Disabled ActiveEntities (for no window in composite passivation strategies).
 */
@InternalApi
private[akka] object NoActiveEntities extends ActiveEntities {
  import EntityPassivationStrategy.PassivateEntities

  override def size: Int = 0
  override def isActive(id: EntityId): Boolean = false
  override def updateLimit(newLimit: Int): PassivateEntities = PassivateEntities.none
  override def update(id: EntityId): PassivateEntities = List(id)
  override def select: OptionVal[EntityId] = OptionVal.None
  override def remove(id: EntityId): Unit = ()
  override def removeIdle(timeout: FiniteDuration): PassivateEntities = PassivateEntities.none
}

/**
 * INTERNAL API
 *
 * Passivate the least recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard
 */
@InternalApi
private[akka] final class LeastRecentlyUsedReplacementPolicy(initialLimit: Int, clock: Clock) extends ActiveEntities {
  import EntityPassivationStrategy.PassivateEntities

  private var limit = initialLimit
  private val recencyList = RecencyList[EntityId](clock)

  override def size: Int = recencyList.size

  override def isActive(id: EntityId): Boolean = recencyList.contains(id)

  override def updateLimit(newLimit: Int): PassivateEntities = {
    limit = newLimit
    removeExcess()
  }

  override def update(id: EntityId): PassivateEntities = {
    recencyList.update(id)
    removeExcess()
  }

  override def select: OptionVal[EntityId] = recencyList.leastRecent

  override def remove(id: EntityId): Unit = recencyList.remove(id)

  override def removeIdle(timeout: FiniteDuration): PassivateEntities = recencyList.removeLeastRecentOutside(timeout)

  private def removeExcess(): PassivateEntities = {
    val excess = recencyList.size - limit
    if (excess > 0) recencyList.removeLeastRecent(excess) else PassivateEntities.none
  }
}

/**
 * INTERNAL API
 *
 * Passivate the least recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 * Active entities are tracked in multiple recency lists, where entities are promoted to higher-level
 * segments on subsequent accesses, and demoted through levels when segments become full.
 * The proportions of the segmented levels can be configured as fractions of the overall limit.
 *
 * @param initialLimit initial active entity capacity for a shard
 * @param proportions proportions of the segmented levels
 * @param idleEnabled whether idle entity passivation is enabled
 */
@InternalApi
private[akka] final class SegmentedLeastRecentlyUsedReplacementPolicy(
    initialLimit: Int,
    proportions: immutable.Seq[Double],
    idleEnabled: Boolean,
    clock: () => Clock)
    extends ActiveEntities {

  import EntityPassivationStrategy.PassivateEntities

  private var limit = initialLimit
  private def segmentLimits: immutable.Seq[Int] = {
    // assign the first segment with the leftover, to have an accurate total limit
    val higherLimits = proportions.drop(1).map(p => (p * limit).toInt)
    (limit - higherLimits.sum) +: higherLimits
  }

  private val segmentedRecencyList =
    if (idleEnabled) SegmentedRecencyList.withOverallRecency.empty[EntityId](clock(), segmentLimits)
    else SegmentedRecencyList.empty[EntityId](segmentLimits)

  override def size: Int = segmentedRecencyList.size

  override def isActive(id: EntityId): Boolean = segmentedRecencyList.contains(id)

  override def updateLimit(newLimit: Int): PassivateEntities = {
    limit = newLimit
    segmentedRecencyList.updateLimits(segmentLimits)
    removeExcess()
  }

  override def update(id: EntityId): PassivateEntities = {
    segmentedRecencyList.update(id)
    removeExcess()
  }

  override def select: OptionVal[EntityId] = segmentedRecencyList.leastRecent

  override def remove(id: EntityId): Unit = segmentedRecencyList.remove(id)

  override def removeIdle(timeout: FiniteDuration): PassivateEntities =
    segmentedRecencyList.removeOverallLeastRecentOutside(timeout)

  private def removeExcess(): PassivateEntities = segmentedRecencyList.removeLeastRecentOverLimit()
}

/**
 * INTERNAL API
 *
 * Passivate the most recently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard
 */
@InternalApi
private[akka] final class MostRecentlyUsedReplacementPolicy(initialLimit: Int, clock: Clock) extends ActiveEntities {
  import EntityPassivationStrategy.PassivateEntities

  private var limit = initialLimit
  private val recencyList = RecencyList[EntityId](clock)

  override def size: Int = recencyList.size

  override def isActive(id: EntityId): Boolean = recencyList.contains(id)

  override def updateLimit(newLimit: Int): PassivateEntities = {
    limit = newLimit
    removeExcess()
  }

  override def update(id: EntityId): PassivateEntities = {
    recencyList.update(id)
    removeExcess(skip = 1) // remove the most recent entity before the just touched entity
  }

  override def select: OptionVal[EntityId] = recencyList.mostRecent

  override def remove(id: EntityId): Unit = recencyList.remove(id)

  override def removeIdle(timeout: FiniteDuration): PassivateEntities = recencyList.removeLeastRecentOutside(timeout)

  private def removeExcess(skip: Int = 0): PassivateEntities = {
    val excess = recencyList.size - limit
    if (excess > 0) recencyList.removeMostRecent(excess, skip) else PassivateEntities.none
  }
}

/**
 * INTERNAL API
 *
 * Passivate the least frequently used entities when the number of active entities in a shard region
 * reaches a limit. The per-region limit is divided evenly among the active shards in a region.
 *
 * @param initialLimit initial active entity capacity for a shard
 * @param dynamicAging whether to apply "dynamic aging" as entities are passivated
 * @param idleEnabled whether idle entity passivation is enabled
 */
@InternalApi
private[akka] final class LeastFrequentlyUsedReplacementPolicy(
    initialLimit: Int,
    dynamicAging: Boolean,
    idleEnabled: Boolean,
    clock: () => Clock)
    extends ActiveEntities {

  import EntityPassivationStrategy.PassivateEntities

  private var limit = initialLimit

  private val frequencyList =
    if (idleEnabled) FrequencyList.withOverallRecency.empty[EntityId](clock(), dynamicAging)
    else FrequencyList.empty[EntityId](dynamicAging)

  override def size: Int = frequencyList.size

  override def isActive(id: EntityId): Boolean = frequencyList.contains(id)

  override def updateLimit(newLimit: Int): PassivateEntities = {
    limit = newLimit
    removeExcess()
  }

  override def update(id: EntityId): PassivateEntities = {
    // first remove excess entities so that dynamic aging is updated
    // and the adjusted age is applied to any new entities on update
    // adjust the expected size by 1 if this is a newly activated entity
    val adjustment = if (frequencyList.contains(id)) 0 else 1
    val passivated = removeExcess(adjustment)
    frequencyList.update(id)
    passivated
  }

  override def select: OptionVal[EntityId] = frequencyList.leastFrequent

  override def remove(id: EntityId): Unit = frequencyList.remove(id)

  override def removeIdle(timeout: FiniteDuration): PassivateEntities =
    frequencyList.removeOverallLeastRecentOutside(timeout)

  private def removeExcess(adjustment: Int = 0): PassivateEntities = {
    val excess = frequencyList.size - limit + adjustment
    if (excess > 0) frequencyList.removeLeastFrequent(excess) else PassivateEntities.none
  }
}

/**
 * INTERNAL API
 *
 * Passivate entities using a "composite" strategy, with an admission area before the main area, an admission
 * filter for admitting entities to the main area, and an optimizer for the proportion of the admission window.
 *
 * References:
 *
 * "TinyLFU: A Highly Efficient Cache Admission Policy"
 * Gil Einziger, Roy Friedman, Ben Manes
 *
 * "Adaptive Software Cache Management"
 * Gil Einziger, Ohad Eytan, Roy Friedman, Ben Manes
 *
 * @param initialLimit initial active entity capacity for a shard region
 * @param window the active entities tracking and replacement policy for the admission window
 * @param initialWindowProportion the initial proportion for the admission window
 * @param minimumWindowProportion the minimum proportion for the admission window (if being optimized)
 * @param maximumWindowProportion the maximum proportion for the admission window (if being optimized)
 * @param windowOptimizer the optimizer for the admission window proportion
 * @param admissionFilter the admission filter to apply for the main area
 * @param main the active entities tracking and replacement policy for the main area
 * @param idleCheck optionally passivate idle entities after the given timeout, checking every interval
 */
@InternalApi
private[akka] final class CompositeEntityPassivationStrategy(
    initialLimit: Int,
    window: ActiveEntities,
    initialWindowProportion: Double,
    minimumWindowProportion: Double,
    maximumWindowProportion: Double,
    windowOptimizer: AdmissionOptimizer,
    admissionFilter: AdmissionFilter,
    main: ActiveEntities,
    idleCheck: Option[IdleCheck])
    extends LimitBasedEntityPassivationStrategy(initialLimit) {

  import EntityPassivationStrategy.PassivateEntities

  private var windowProportion = initialWindowProportion
  private var windowLimit = 0
  private var mainLimit = 0

  private def calculateLimits(): Unit = {
    windowLimit = (windowProportion * perShardLimit).toInt
    mainLimit = perShardLimit - windowLimit
  }

  // set initial limits based on initial window proportion
  calculateLimits()
  window.updateLimit(windowLimit)
  main.updateLimit(mainLimit)

  override def entityTouched(id: EntityId): PassivateEntities = {
    admissionFilter.update(id)
    val passivated = if (window.isActive(id)) {
      windowOptimizer.recordActive()
      window.update(id)
    } else if (main.isActive(id)) {
      windowOptimizer.recordActive()
      main.update(id)
    } else {
      windowOptimizer.recordPassive()
      maybeAdmitToMain(window.update(id))
    }
    adaptWindow()
    passivated
  }

  override def entityTerminated(id: EntityId): Unit = {
    window.remove(id)
    main.remove(id)
  }

  override protected def passivateEntitiesOnLimitUpdate(): PassivateEntities = {
    calculateLimits()
    windowOptimizer.updateLimit(perShardLimit)
    admissionFilter.updateCapacity(perShardLimit)
    maybeAdmitToMain(window.updateLimit(windowLimit)) ++ main.updateLimit(mainLimit)
  }

  override def scheduledInterval: Option[FiniteDuration] = idleCheck.map(_.interval)

  override def intervalPassed(): PassivateEntities = idleCheck.fold(PassivateEntities.none) { idle =>
    window.removeIdle(idle.timeout) ++ main.removeIdle(idle.timeout)
  }

  private def maybeAdmitToMain(candidates: PassivateEntities): PassivateEntities = {
    if (candidates.nonEmpty) {
      var passivated: PassivateEntities = PassivateEntities.none
      candidates.foreach { candidate =>
        if (main.size >= mainLimit) {
          main.select match {
            case OptionVal.Some(selected) =>
              if (admissionFilter.admit(candidate, selected))
                passivated ++= main.update(candidate)
              else passivated :+= candidate
            case _ => passivated ++= main.update(candidate)
          }
        } else passivated ++= main.update(candidate)
      }
      passivated
    } else PassivateEntities.none
  }

  private def adaptWindow(): Unit = {
    val adjustment = windowOptimizer.calculateAdjustment()
    if (adjustment != 0.0) {
      windowProportion =
        math.max(minimumWindowProportion, math.min(maximumWindowProportion, windowProportion + adjustment))
      calculateLimits()
      // note: no passivations from adjustments, entities are transferred between window and main
      if (adjustment > 0.0) { // increase window limit
        window.updateLimit(windowLimit)
        main.updateLimit(mainLimit).foreach(window.update)
      } else { // decrease window limit
        main.updateLimit(mainLimit)
        window.updateLimit(windowLimit).foreach(main.update)
      }
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object AdmissionOptimizer {
  def apply(
      initialLimit: Int,
      optimizer: ClusterShardingSettings.CompositePassivationStrategy.AdmissionOptimizer): AdmissionOptimizer =
    optimizer match {
      case ClusterShardingSettings.CompositePassivationStrategy.HillClimbingAdmissionOptimizer(
          adjustMultiplier,
          initialStep,
          restartThreshold,
          stepDecay) =>
        new HillClimbingAdmissionOptimizer(initialLimit, adjustMultiplier, initialStep, restartThreshold, stepDecay)
      case _ => NoAdmissionOptimizer
    }
}

/**
 * INTERNAL API
 *
 * An optimizer for the size of the admission window for a composite passivation strategy.
 */
@InternalApi
private[akka] abstract class AdmissionOptimizer {

  /**
   * An entity was accessed that is already active.
   */
  def recordActive(): Unit

  /**
   * An entity was accessed that was passive (needed to be activated).
   */
  def recordPassive(): Unit

  /**
   * The per-shard limit has been updated.
   * @param newLimit the new per-shard limit
   */
  def updateLimit(newLimit: Int): Unit

  /**
   * Calculate an adjustment to the proportion of the admission window.
   * Can be positive (to grow the window) or negative (to shrink the window).
   * Returns 0.0 if no adjustment should be made.
   * @return the adjustment to make to the admission window proportion
   */
  def calculateAdjustment(): Double
}

/**
 * INTERNAL API
 *
 * Disabled admission window proportion optimizer.
 */
@InternalApi
private[akka] object NoAdmissionOptimizer extends AdmissionOptimizer {
  override def recordActive(): Unit = ()
  override def recordPassive(): Unit = ()
  override def updateLimit(newLimit: Int): Unit = ()
  override def calculateAdjustment(): Double = 0.0
}

/**
 * INTERNAL API
 *
 * Optimizer for the admission window using a simple hill-climbing algorithm.
 */
@InternalApi
private[akka] final class HillClimbingAdmissionOptimizer(
    initialLimit: Int,
    adjustMultiplier: Double,
    initialStep: Double,
    restartThreshold: Double,
    stepDecay: Double)
    extends AdmissionOptimizer {
  private var adjustSize = adjustMultiplier * initialLimit
  private var accesses = 0
  private var activeAccesses = 0
  private var previousActiveRate = 0.0
  private var nextStep = -initialStep // start in decreasing direction

  override def recordActive(): Unit = {
    accesses += 1
    activeAccesses += 1
  }

  override def recordPassive(): Unit = accesses += 1

  override def updateLimit(newLimit: Int): Unit =
    adjustSize = adjustMultiplier * newLimit

  override def calculateAdjustment(): Double = {
    if (accesses >= adjustSize) {
      val activeRate = activeAccesses.toDouble / accesses
      val delta = activeRate - previousActiveRate
      val adjustment = if (delta >= 0) nextStep else -nextStep
      val direction = if (adjustment >= 0) 1 else -1
      val restart = math.abs(delta) >= restartThreshold
      nextStep = if (restart) initialStep * direction else adjustment * stepDecay
      previousActiveRate = activeRate
      accesses = 0
      activeAccesses = 0
      adjustment
    } else 0.0
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object AdmissionFilter {
  def apply(
      initialCapacity: Int,
      filter: ClusterShardingSettings.CompositePassivationStrategy.AdmissionFilter): AdmissionFilter = filter match {
    case ClusterShardingSettings.CompositePassivationStrategy
          .FrequencySketchAdmissionFilter(widthMultiplier, resetMultiplier, depth, counterBits) =>
      FrequencySketchAdmissionFilter(initialCapacity, widthMultiplier, resetMultiplier, depth, counterBits)
    case _ => AlwaysAdmissionFilter
  }
}

/**
 * INTERNAL API
 *
 * An admission filter for the main area for a composite passivation strategy.
 */
@InternalApi
private[akka] abstract class AdmissionFilter {

  /**
   * Update the capacity, the per-shard entity limit.
   * @param newCapacity the new capacity for the filter
   */
  def updateCapacity(newCapacity: Int): Unit

  /**
   * Update the filter when an entity is accessed
   * @param id the entity id that has been accessed
   */
  def update(id: EntityId): Unit

  /**
   * Determine whether an entity should be admitted to the main area.
   * The candidate has been removed from the admission window (according to its replacement policy)
   * and can replace an entity in the main area (selected by its replacement policy).
   * Whichever entity is not admitted or retained will be passivated.
   * @param candidate the candidate from the window that may be admitted to the main area
   * @param selected the entity selected from the main area to possibly be replaced by the candidate
   * @return whether to admit the candidate to the main area
   */
  def admit(candidate: EntityId, selected: EntityId): Boolean
}

/**
 * INTERNAL API
 *
 * Disabled admission filter, always admit candidates to the main area.
 */
@InternalApi
private[akka] object AlwaysAdmissionFilter extends AdmissionFilter {
  override def updateCapacity(newCapacity: Int): Unit = ()
  override def update(id: EntityId): Unit = ()
  override def admit(candidate: EntityId, selected: EntityId): Boolean = true
}

/**
 * INTERNAL API
 *
 * Admission filter based on a frequency sketch.
 */
@InternalApi
private[akka] object FrequencySketchAdmissionFilter {
  def apply(
      initialCapacity: Int,
      widthMultiplier: Int,
      resetMultiplier: Double,
      depth: Int,
      counterBits: Int): AdmissionFilter = {
    if (depth == 4 && counterBits == 4)
      new FastFrequencySketchAdmissionFilter(initialCapacity, widthMultiplier, resetMultiplier)
    else
      new FrequencySketchAdmissionFilter(initialCapacity, widthMultiplier, resetMultiplier, depth, counterBits)
  }
}

/**
 * INTERNAL API
 *
 * Admission filter based on a frequency sketch.
 */
@InternalApi
private[akka] final class FrequencySketchAdmissionFilter(
    initialCapacity: Int,
    widthMultiplier: Int,
    resetMultiplier: Double,
    depth: Int,
    counterBits: Int)
    extends AdmissionFilter {

  private def createSketch(capacity: Int): FrequencySketch[EntityId] =
    FrequencySketch[EntityId](capacity, widthMultiplier, resetMultiplier, depth, counterBits)

  private var frequencySketch = createSketch(initialCapacity)

  override def updateCapacity(newCapacity: Int): Unit = frequencySketch = createSketch(newCapacity)

  override def update(id: EntityId): Unit = frequencySketch.increment(id)

  override def admit(candidate: EntityId, selected: EntityId): Boolean =
    frequencySketch.frequency(candidate) > frequencySketch.frequency(selected)
}

/**
 * INTERNAL API
 *
 * Admission filter based on a frequency sketch (fast version with depth of 4 and 4-bit counters).
 */
@InternalApi
private[akka] final class FastFrequencySketchAdmissionFilter(
    initialCapacity: Int,
    widthMultiplier: Int,
    resetMultiplier: Double)
    extends AdmissionFilter {

  private def createSketch(capacity: Int): FastFrequencySketch[EntityId] =
    FastFrequencySketch[EntityId](capacity, widthMultiplier, resetMultiplier)

  private var frequencySketch = createSketch(initialCapacity)

  override def updateCapacity(newCapacity: Int): Unit = frequencySketch = createSketch(newCapacity)

  override def update(id: EntityId): Unit = frequencySketch.increment(id)

  override def admit(candidate: EntityId, selected: EntityId): Boolean =
    frequencySketch.frequency(candidate) > frequencySketch.frequency(selected)
}
