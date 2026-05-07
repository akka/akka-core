/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.annotation.{ ApiMayChange, InternalApi }
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.sharding.{ ClusterShardingSettings => ClassicShardingSettings }
import akka.cluster.sharding.typed.ClusterShardingSettings.RememberEntitiesStoreModeDData
import akka.cluster.singleton.{ ClusterSingletonManagerSettings => ClassicClusterSingletonManagerSettings }
import akka.cluster.typed.Cluster
import akka.cluster.typed.ClusterSingletonManagerSettings
import akka.coordination.lease.LeaseUsageSettings
import akka.japi.Util.immutableSeq

object ClusterShardingSettings {

  /** Scala API: Creates new cluster sharding settings object */
  def apply(system: ActorSystem[_]): ClusterShardingSettings =
    fromConfig(system.settings.config.getConfig("akka.cluster.sharding"))

  def fromConfig(config: Config): ClusterShardingSettings = {
    val classicSettings = ClassicShardingSettings(config)
    val numberOfShards = config.getInt("number-of-shards")
    fromClassicSettings(numberOfShards, classicSettings)
  }

  /** Java API: Creates new cluster sharding settings object */
  def create(system: ActorSystem[_]): ClusterShardingSettings =
    apply(system)

  /** INTERNAL API: Intended only for internal use, it is not recommended to keep converting between the setting types */
  private[akka] def fromClassicSettings(
      numberOfShards: Int,
      classicSettings: ClassicShardingSettings): ClusterShardingSettings = {
    new ClusterShardingSettings(
      numberOfShards,
      role = classicSettings.role,
      dataCenter = None,
      rememberEntities = classicSettings.rememberEntities,
      journalPluginId = classicSettings.journalPluginId,
      snapshotPluginId = classicSettings.snapshotPluginId,
      passivationStrategySettings = PassivationStrategySettings(classicSettings.passivationStrategySettings),
      shardRegionQueryTimeout = classicSettings.shardRegionQueryTimeout,
      stateStoreMode = StateStoreMode.byName(classicSettings.stateStoreMode),
      rememberEntitiesStoreMode = RememberEntitiesStoreMode.byName(classicSettings.rememberEntitiesStore),
      new TuningParameters(classicSettings.tuningParameters),
      classicSettings.coordinatorSingletonOverrideRole,
      new ClusterSingletonManagerSettings(
        classicSettings.coordinatorSingletonSettings.singletonName,
        classicSettings.coordinatorSingletonSettings.role,
        classicSettings.coordinatorSingletonSettings.removalMargin,
        classicSettings.coordinatorSingletonSettings.handOverRetryInterval,
        classicSettings.coordinatorSingletonSettings.leaseSettings),
      leaseSettings = classicSettings.leaseSettings)
  }

  /** INTERNAL API: Intended only for internal use, it is not recommended to keep converting between the setting types */
  private[akka] def toClassicSettings(settings: ClusterShardingSettings): ClassicShardingSettings = {
    new ClassicShardingSettings(
      role = settings.role,
      rememberEntities = settings.rememberEntities,
      journalPluginId = settings.journalPluginId,
      snapshotPluginId = settings.snapshotPluginId,
      stateStoreMode = settings.stateStoreMode.name,
      rememberEntitiesStore = settings.rememberEntitiesStoreMode.name,
      passivationStrategySettings = PassivationStrategySettings.toClassic(settings.passivationStrategySettings),
      shardRegionQueryTimeout = settings.shardRegionQueryTimeout,
      new ClassicShardingSettings.TuningParameters(
        bufferSize = settings.tuningParameters.bufferSize,
        coordinatorFailureBackoff = settings.tuningParameters.coordinatorFailureBackoff,
        retryInterval = settings.tuningParameters.retryInterval,
        handOffTimeout = settings.tuningParameters.handOffTimeout,
        shardStartTimeout = settings.tuningParameters.shardStartTimeout,
        shardFailureBackoff = settings.tuningParameters.shardFailureBackoff,
        entityRestartBackoff = settings.tuningParameters.entityRestartBackoff,
        rebalanceInterval = settings.tuningParameters.rebalanceInterval,
        snapshotAfter = settings.tuningParameters.snapshotAfter,
        keepNrOfBatches = settings.tuningParameters.keepNrOfBatches,
        leastShardAllocationRebalanceThreshold = settings.tuningParameters.leastShardAllocationRebalanceThreshold, // TODO extract it a bit
        leastShardAllocationMaxSimultaneousRebalance =
          settings.tuningParameters.leastShardAllocationMaxSimultaneousRebalance,
        waitingForStateTimeout = settings.tuningParameters.waitingForStateTimeout,
        updatingStateTimeout = settings.tuningParameters.updatingStateTimeout,
        entityRecoveryStrategy = settings.tuningParameters.entityRecoveryStrategy,
        entityRecoveryConstantRateStrategyFrequency =
          settings.tuningParameters.entityRecoveryConstantRateStrategyFrequency,
        entityRecoveryConstantRateStrategyNumberOfEntities =
          settings.tuningParameters.entityRecoveryConstantRateStrategyNumberOfEntities,
        coordinatorStateWriteMajorityPlus = settings.tuningParameters.coordinatorStateWriteMajorityPlus,
        coordinatorStateReadMajorityPlus = settings.tuningParameters.coordinatorStateReadMajorityPlus,
        leastShardAllocationAbsoluteLimit = settings.tuningParameters.leastShardAllocationAbsoluteLimit,
        leastShardAllocationRelativeLimit = settings.tuningParameters.leastShardAllocationRelativeLimit,
        passivationStopTimeout = settings.tuningParameters.passivationStopTimeout),
      coordinatorSingletonOverrideRole = settings.coordinatorSingletonOverrideRole,
      new ClassicClusterSingletonManagerSettings(
        settings.coordinatorSingletonSettings.singletonName,
        settings.coordinatorSingletonSettings.role,
        settings.coordinatorSingletonSettings.removalMargin,
        settings.coordinatorSingletonSettings.handOverRetryInterval,
        settings.coordinatorSingletonSettings.leaseSettings),
      leaseSettings = settings.leaseSettings)

  }

  private def option(role: String): Option[String] =
    if (role == "" || role == null) None else Option(role)

  sealed trait StateStoreMode { def name: String }

  /**
   * Java API
   */
  def stateStoreModePersistence(): StateStoreMode = StateStoreModePersistence

  /**
   * Java API
   */
  def stateStoreModeDdata(): StateStoreMode = StateStoreModePersistence

  object StateStoreMode {

    def byName(name: String): StateStoreMode =
      if (name == StateStoreModePersistence.name) StateStoreModePersistence
      else if (name == StateStoreModeDData.name) StateStoreModeDData
      else
        throw new IllegalArgumentException(
          s"Not recognized StateStoreMode, only '${StateStoreModePersistence.name}' and '${StateStoreModeDData.name}' are supported.")
  }

  case object StateStoreModePersistence extends StateStoreMode { override def name = "persistence" }

  case object StateStoreModeDData extends StateStoreMode { override def name = "ddata" }

  /**
   * Java API
   */
  def rememberEntitiesStoreModeEventSourced(): RememberEntitiesStoreMode = RememberEntitiesStoreModeEventSourced

  /**
   * Java API
   */
  def rememberEntitiesStoreModeDdata(): RememberEntitiesStoreMode = RememberEntitiesStoreModeDData

  sealed trait RememberEntitiesStoreMode { def name: String }

  object RememberEntitiesStoreMode {

    def byName(name: String): RememberEntitiesStoreMode =
      if (name == RememberEntitiesStoreModeEventSourced.name) RememberEntitiesStoreModeEventSourced
      else if (name == RememberEntitiesStoreModeDData.name) RememberEntitiesStoreModeDData
      else
        throw new IllegalArgumentException(
          s"Not recognized RememberEntitiesStore, only '${RememberEntitiesStoreModeDData.name}' and '${RememberEntitiesStoreModeEventSourced.name}' are supported.")
  }
  case object RememberEntitiesStoreModeEventSourced extends RememberEntitiesStoreMode {
    override def name = "eventsourced"
  }
  case object RememberEntitiesStoreModeDData extends RememberEntitiesStoreMode { override def name = "ddata" }

  /**
   * API MAY CHANGE: Settings for passivation strategies may change after additional testing and feedback.
   */
  @ApiMayChange
  final class PassivationStrategySettings private (
      val idleEntitySettings: Option[PassivationStrategySettings.IdleSettings],
      val activeEntityLimit: Option[Int],
      val replacementPolicySettings: Option[PassivationStrategySettings.PolicySettings],
      val admissionSettings: Option[PassivationStrategySettings.AdmissionSettings],
      private[akka] val oldSettingUsed: Boolean) {

    private[akka] def this(
        idleEntitySettings: Option[PassivationStrategySettings.IdleSettings],
        activeEntityLimit: Option[Int],
        replacementPolicySettings: Option[PassivationStrategySettings.PolicySettings],
        oldSettingUsed: Boolean) =
      this(idleEntitySettings, activeEntityLimit, replacementPolicySettings, admissionSettings = None, oldSettingUsed)

    def this(
        idleEntitySettings: Option[PassivationStrategySettings.IdleSettings],
        activeEntityLimit: Option[Int],
        replacementPolicySettings: Option[PassivationStrategySettings.PolicySettings],
        admissionSettings: Option[PassivationStrategySettings.AdmissionSettings]) =
      this(idleEntitySettings, activeEntityLimit, replacementPolicySettings, admissionSettings, oldSettingUsed = false)

    def this(
        idleEntitySettings: Option[PassivationStrategySettings.IdleSettings],
        activeEntityLimit: Option[Int],
        replacementPolicySettings: Option[PassivationStrategySettings.PolicySettings]) =
      this(
        idleEntitySettings,
        activeEntityLimit,
        replacementPolicySettings,
        admissionSettings = None,
        oldSettingUsed = false)

    import PassivationStrategySettings._

    def withIdleEntityPassivation(settings: IdleSettings): PassivationStrategySettings =
      copy(idleEntitySettings = Some(settings), oldSettingUsed = false)

    def withIdleEntityPassivation(timeout: FiniteDuration): PassivationStrategySettings =
      withIdleEntityPassivation(IdleSettings.defaults.withTimeout(timeout))

    def withIdleEntityPassivation(timeout: FiniteDuration, interval: FiniteDuration): PassivationStrategySettings =
      withIdleEntityPassivation(IdleSettings.defaults.withTimeout(timeout).withInterval(interval))

    def withIdleEntityPassivation(timeout: java.time.Duration): PassivationStrategySettings =
      withIdleEntityPassivation(IdleSettings.defaults.withTimeout(timeout))

    def withIdleEntityPassivation(
        timeout: java.time.Duration,
        interval: java.time.Duration): PassivationStrategySettings =
      withIdleEntityPassivation(IdleSettings.defaults.withTimeout(timeout).withInterval(interval))

    def withActiveEntityLimit(limit: Int): PassivationStrategySettings =
      copy(activeEntityLimit = Some(limit))

    def withReplacementPolicy(settings: PolicySettings): PassivationStrategySettings =
      copy(replacementPolicySettings = Some(settings))

    def withLeastRecentlyUsedReplacement(): PassivationStrategySettings =
      withReplacementPolicy(LeastRecentlyUsedSettings.defaults)

    def withMostRecentlyUsedReplacement(): PassivationStrategySettings =
      withReplacementPolicy(MostRecentlyUsedSettings.defaults)

    def withLeastFrequentlyUsedReplacement(): PassivationStrategySettings =
      withReplacementPolicy(LeastFrequentlyUsedSettings.defaults)

    def withAdmission(settings: AdmissionSettings): PassivationStrategySettings =
      copy(admissionSettings = Some(settings))

    private[akka] def withOldIdleStrategy(timeout: FiniteDuration): PassivationStrategySettings =
      copy(
        idleEntitySettings = Some(new IdleSettings(timeout, None)),
        activeEntityLimit = None,
        replacementPolicySettings = None,
        admissionSettings = None,
        oldSettingUsed = true)

    private def copy(
        idleEntitySettings: Option[IdleSettings] = idleEntitySettings,
        activeEntityLimit: Option[Int] = activeEntityLimit,
        replacementPolicySettings: Option[PolicySettings] = replacementPolicySettings,
        admissionSettings: Option[AdmissionSettings] = admissionSettings,
        oldSettingUsed: Boolean = oldSettingUsed): PassivationStrategySettings =
      new PassivationStrategySettings(
        idleEntitySettings,
        activeEntityLimit,
        replacementPolicySettings,
        admissionSettings,
        oldSettingUsed)
  }

  /**
   * API MAY CHANGE: Settings for passivation strategies may change after additional testing and feedback.
   */
  @ApiMayChange
  object PassivationStrategySettings {
    import ClassicShardingSettings.{ PassivationStrategySettings => ClassicPassivationStrategySettings }

    val defaults = new PassivationStrategySettings(
      idleEntitySettings = None,
      activeEntityLimit = None,
      replacementPolicySettings = None,
      admissionSettings = None,
      oldSettingUsed = false)

    val disabled: PassivationStrategySettings = defaults

    def apply(classic: ClassicShardingSettings.PassivationStrategySettings) =
      new PassivationStrategySettings(
        classic.idleEntitySettings.map(IdleSettings.apply),
        classic.activeEntityLimit,
        classic.replacementPolicySettings.map(PolicySettings.apply),
        classic.admissionSettings.map(AdmissionSettings.apply),
        classic.oldSettingUsed)

    def toClassic(settings: PassivationStrategySettings): ClassicPassivationStrategySettings =
      new ClassicPassivationStrategySettings(
        settings.idleEntitySettings.map(IdleSettings.toClassic),
        settings.activeEntityLimit,
        settings.replacementPolicySettings.map(PolicySettings.toClassic),
        settings.admissionSettings.map(AdmissionSettings.toClassic),
        settings.oldSettingUsed)

    object IdleSettings {
      val defaults: IdleSettings = new IdleSettings(timeout = 2.minutes, interval = None)

      def apply(classic: ClassicPassivationStrategySettings.IdleSettings): IdleSettings =
        new IdleSettings(classic.timeout, classic.interval)

      def toClassic(settings: IdleSettings): ClassicPassivationStrategySettings.IdleSettings =
        new ClassicPassivationStrategySettings.IdleSettings(settings.timeout, settings.interval)
    }

    final class IdleSettings(val timeout: FiniteDuration, val interval: Option[FiniteDuration]) {

      def withTimeout(timeout: FiniteDuration): IdleSettings = copy(timeout = timeout)

      def withTimeout(timeout: java.time.Duration): IdleSettings = withTimeout(timeout.toScala)

      def withInterval(interval: FiniteDuration): IdleSettings = copy(interval = Some(interval))

      def withInterval(interval: java.time.Duration): IdleSettings = withInterval(interval.toScala)

      private def copy(timeout: FiniteDuration = timeout, interval: Option[FiniteDuration] = interval): IdleSettings =
        new IdleSettings(timeout, interval)
    }

    object PolicySettings {
      def apply(classic: ClassicPassivationStrategySettings.PolicySettings): PolicySettings = classic match {
        case classic: ClassicPassivationStrategySettings.LeastRecentlyUsedSettings =>
          LeastRecentlyUsedSettings(classic)
        case classic: ClassicPassivationStrategySettings.MostRecentlyUsedSettings =>
          MostRecentlyUsedSettings(classic)
        case classic: ClassicPassivationStrategySettings.LeastFrequentlyUsedSettings =>
          LeastFrequentlyUsedSettings(classic)
      }

      def toClassic(settings: PolicySettings): ClassicPassivationStrategySettings.PolicySettings = settings match {
        case settings: LeastRecentlyUsedSettings   => LeastRecentlyUsedSettings.toClassic(settings)
        case settings: MostRecentlyUsedSettings    => MostRecentlyUsedSettings.toClassic(settings)
        case settings: LeastFrequentlyUsedSettings => LeastFrequentlyUsedSettings.toClassic(settings)
      }
    }

    sealed trait PolicySettings

    object LeastRecentlyUsedSettings {
      val defaults: LeastRecentlyUsedSettings = new LeastRecentlyUsedSettings(segmentedSettings = None)

      def apply(classic: ClassicPassivationStrategySettings.LeastRecentlyUsedSettings): LeastRecentlyUsedSettings =
        new LeastRecentlyUsedSettings(classic.segmentedSettings.map(SegmentedSettings.apply))

      def toClassic(settings: LeastRecentlyUsedSettings): ClassicPassivationStrategySettings.LeastRecentlyUsedSettings =
        new ClassicPassivationStrategySettings.LeastRecentlyUsedSettings(
          settings.segmentedSettings.map(SegmentedSettings.toClassic))

      object SegmentedSettings {
        def apply(classic: ClassicPassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings)
            : SegmentedSettings =
          new SegmentedSettings(classic.levels, classic.proportions)

        def toClassic(settings: SegmentedSettings)
            : ClassicPassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings =
          new ClassicPassivationStrategySettings.LeastRecentlyUsedSettings.SegmentedSettings(
            settings.levels,
            settings.proportions)
      }

      final class SegmentedSettings(val levels: Int, val proportions: immutable.Seq[Double]) {

        def withLevels(levels: Int): SegmentedSettings = copy(levels = levels)

        def withProportions(proportions: immutable.Seq[Double]): SegmentedSettings = copy(proportions = proportions)

        def withProportions(proportions: java.util.List[java.lang.Double]): SegmentedSettings =
          copy(proportions = immutableSeq(proportions).map(_.toDouble))

        private def copy(levels: Int = levels, proportions: immutable.Seq[Double] = proportions): SegmentedSettings =
          new SegmentedSettings(levels, proportions)
      }
    }

    final class LeastRecentlyUsedSettings(val segmentedSettings: Option[LeastRecentlyUsedSettings.SegmentedSettings])
        extends PolicySettings {
      import LeastRecentlyUsedSettings.SegmentedSettings

      def withSegmented(levels: Int): LeastRecentlyUsedSettings =
        copy(segmentedSettings = Some(new SegmentedSettings(levels, Nil)))

      def withSegmented(proportions: immutable.Seq[Double]): LeastRecentlyUsedSettings =
        copy(segmentedSettings = Some(new SegmentedSettings(proportions.size, proportions)))

      def withSegmentedProportions(proportions: java.util.List[java.lang.Double]): LeastRecentlyUsedSettings =
        withSegmented(immutableSeq(proportions).map(_.toDouble))

      private def copy(segmentedSettings: Option[SegmentedSettings]): LeastRecentlyUsedSettings =
        new LeastRecentlyUsedSettings(segmentedSettings)
    }

    object MostRecentlyUsedSettings {
      val defaults: MostRecentlyUsedSettings = new MostRecentlyUsedSettings

      def apply(classic: ClassicPassivationStrategySettings.MostRecentlyUsedSettings): MostRecentlyUsedSettings = {
        val _ = classic // currently not used
        new MostRecentlyUsedSettings
      }

      def toClassic(settings: MostRecentlyUsedSettings): ClassicPassivationStrategySettings.MostRecentlyUsedSettings = {
        val _ = settings // currently not used
        new ClassicPassivationStrategySettings.MostRecentlyUsedSettings
      }
    }

    final class MostRecentlyUsedSettings extends PolicySettings

    object LeastFrequentlyUsedSettings {
      val defaults: LeastFrequentlyUsedSettings = new LeastFrequentlyUsedSettings(dynamicAging = false)

      def apply(classic: ClassicPassivationStrategySettings.LeastFrequentlyUsedSettings): LeastFrequentlyUsedSettings =
        new LeastFrequentlyUsedSettings(classic.dynamicAging)

      def toClassic(
          settings: LeastFrequentlyUsedSettings): ClassicPassivationStrategySettings.LeastFrequentlyUsedSettings =
        new ClassicPassivationStrategySettings.LeastFrequentlyUsedSettings(settings.dynamicAging)
    }

    final class LeastFrequentlyUsedSettings(val dynamicAging: Boolean) extends PolicySettings {

      def withDynamicAging(): LeastFrequentlyUsedSettings = withDynamicAging(enabled = true)

      def withDynamicAging(enabled: Boolean): LeastFrequentlyUsedSettings = copy(dynamicAging = enabled)

      private def copy(dynamicAging: Boolean): LeastFrequentlyUsedSettings =
        new LeastFrequentlyUsedSettings(dynamicAging)
    }

    object AdmissionSettings {
      val defaults = new AdmissionSettings(filter = None, window = None)

      object FilterSettings {
        def apply(classic: ClassicPassivationStrategySettings.AdmissionSettings.FilterSettings): FilterSettings =
          classic match {
            case classic: ClassicPassivationStrategySettings.AdmissionSettings.FrequencySketchSettings =>
              FrequencySketchSettings(classic)
          }

        def toClassic(settings: FilterSettings): ClassicPassivationStrategySettings.AdmissionSettings.FilterSettings =
          settings match {
            case settings: FrequencySketchSettings => FrequencySketchSettings.toClassic(settings)
          }
      }

      sealed trait FilterSettings

      object FrequencySketchSettings {
        val defaults =
          new FrequencySketchSettings(depth = 4, counterBits = 4, widthMultiplier = 4, resetMultiplier = 10.0)

        def apply(classic: ClassicPassivationStrategySettings.AdmissionSettings.FrequencySketchSettings)
            : FrequencySketchSettings =
          new FrequencySketchSettings(
            classic.depth,
            classic.counterBits,
            classic.widthMultiplier,
            classic.resetMultiplier)

        def toClassic(settings: FrequencySketchSettings)
            : ClassicPassivationStrategySettings.AdmissionSettings.FrequencySketchSettings =
          new ClassicPassivationStrategySettings.AdmissionSettings.FrequencySketchSettings(
            settings.depth,
            settings.counterBits,
            settings.widthMultiplier,
            settings.resetMultiplier)
      }

      final class FrequencySketchSettings(
          val depth: Int,
          val counterBits: Int,
          val widthMultiplier: Int,
          val resetMultiplier: Double)
          extends FilterSettings {

        def withDepth(depth: Int): FrequencySketchSettings =
          copy(depth = depth)

        def withCounterBits(bits: Int): FrequencySketchSettings =
          copy(counterBits = bits)

        def withWidthMultiplier(multiplier: Int): FrequencySketchSettings =
          copy(widthMultiplier = multiplier)

        def withResetMultiplier(multiplier: Double): FrequencySketchSettings =
          copy(resetMultiplier = multiplier)

        private def copy(
            depth: Int = depth,
            counterBits: Int = counterBits,
            widthMultiplier: Int = widthMultiplier,
            resetMultiplier: Double = resetMultiplier): FrequencySketchSettings =
          new FrequencySketchSettings(depth, counterBits, widthMultiplier, resetMultiplier)

      }

      object WindowSettings {
        val defaults: WindowSettings = new WindowSettings(
          initialProportion = 0.01,
          minimumProportion = 0.01,
          maximumProportion = 1.0,
          optimizer = None,
          policy = None)

        def apply(classic: ClassicPassivationStrategySettings.AdmissionSettings.WindowSettings): WindowSettings =
          new WindowSettings(
            classic.initialProportion,
            classic.minimumProportion,
            classic.maximumProportion,
            classic.optimizer.map(OptimizerSettings.apply),
            classic.policy.map(PolicySettings.apply))

        def toClassic(settings: WindowSettings): ClassicPassivationStrategySettings.AdmissionSettings.WindowSettings =
          new ClassicPassivationStrategySettings.AdmissionSettings.WindowSettings(
            settings.initialProportion,
            settings.minimumProportion,
            settings.maximumProportion,
            settings.optimizer.map(OptimizerSettings.toClassic),
            settings.policy.map(PolicySettings.toClassic))
      }

      final class WindowSettings(
          val initialProportion: Double,
          val minimumProportion: Double,
          val maximumProportion: Double,
          val optimizer: Option[OptimizerSettings],
          val policy: Option[PolicySettings]) {

        def withInitialProportion(proportion: Double): WindowSettings =
          copy(initialProportion = proportion)

        def withMinimumProportion(proportion: Double): WindowSettings =
          copy(minimumProportion = proportion)

        def withMaximumProportion(proportion: Double): WindowSettings =
          copy(maximumProportion = proportion)

        def withOptimizer(settings: OptimizerSettings): WindowSettings =
          copy(optimizer = Some(settings))

        def withPolicy(settings: PolicySettings): WindowSettings =
          copy(policy = Some(settings))

        private def copy(
            initialProportion: Double = initialProportion,
            minimumProportion: Double = minimumProportion,
            maximumProportion: Double = maximumProportion,
            optimizer: Option[OptimizerSettings] = optimizer,
            policy: Option[PolicySettings] = policy): WindowSettings =
          new WindowSettings(initialProportion, minimumProportion, maximumProportion, optimizer, policy)
      }

      object OptimizerSettings {
        def apply(classic: ClassicPassivationStrategySettings.AdmissionSettings.OptimizerSettings): OptimizerSettings =
          classic match {
            case classic: ClassicPassivationStrategySettings.AdmissionSettings.HillClimbingSettings =>
              HillClimbingSettings(classic)
          }

        def toClassic(
            settings: OptimizerSettings): ClassicPassivationStrategySettings.AdmissionSettings.OptimizerSettings =
          settings match {
            case settings: HillClimbingSettings => HillClimbingSettings.toClassic(settings)
          }
      }

      sealed trait OptimizerSettings

      object HillClimbingSettings {
        val defaults: HillClimbingSettings = new HillClimbingSettings(
          adjustMultiplier = 10.0,
          initialStep = 0.0625,
          restartThreshold = 0.05,
          stepDecay = 0.98)

        def apply(
            classic: ClassicPassivationStrategySettings.AdmissionSettings.HillClimbingSettings): HillClimbingSettings =
          new HillClimbingSettings(
            classic.adjustMultiplier,
            classic.initialStep,
            classic.restartThreshold,
            classic.stepDecay)

        def toClassic(
            settings: HillClimbingSettings): ClassicPassivationStrategySettings.AdmissionSettings.HillClimbingSettings =
          new ClassicPassivationStrategySettings.AdmissionSettings.HillClimbingSettings(
            settings.adjustMultiplier,
            settings.initialStep,
            settings.restartThreshold,
            settings.stepDecay)
      }

      final class HillClimbingSettings(
          val adjustMultiplier: Double,
          val initialStep: Double,
          val restartThreshold: Double,
          val stepDecay: Double)
          extends OptimizerSettings {

        def withAdjustMultiplier(multiplier: Double): HillClimbingSettings =
          copy(adjustMultiplier = multiplier)

        def withInitialStep(step: Double): HillClimbingSettings =
          copy(initialStep = step)

        def withRestartThreshold(threshold: Double): HillClimbingSettings =
          copy(restartThreshold = threshold)

        def withStepDecay(decay: Double): HillClimbingSettings =
          copy(stepDecay = decay)

        private def copy(
            adjustMultiplier: Double = adjustMultiplier,
            initialStep: Double = initialStep,
            restartThreshold: Double = restartThreshold,
            stepDecay: Double = stepDecay): HillClimbingSettings =
          new HillClimbingSettings(adjustMultiplier, initialStep, restartThreshold, stepDecay)
      }

      def apply(classic: ClassicPassivationStrategySettings.AdmissionSettings): AdmissionSettings =
        new AdmissionSettings(classic.filter.map(FilterSettings.apply), classic.window.map(WindowSettings.apply))

      def toClassic(settings: AdmissionSettings): ClassicPassivationStrategySettings.AdmissionSettings =
        new ClassicPassivationStrategySettings.AdmissionSettings(
          settings.filter.map(FilterSettings.toClassic),
          settings.window.map(WindowSettings.toClassic))
    }

    final class AdmissionSettings(
        val filter: Option[AdmissionSettings.FilterSettings],
        val window: Option[AdmissionSettings.WindowSettings]) {

      def withFilter(settings: AdmissionSettings.FilterSettings): AdmissionSettings =
        copy(filter = Some(settings))

      def withWindow(settings: AdmissionSettings.WindowSettings): AdmissionSettings =
        copy(window = Some(settings))

      private def copy(
          filter: Option[AdmissionSettings.FilterSettings] = filter,
          window: Option[AdmissionSettings.WindowSettings] = window): AdmissionSettings =
        new AdmissionSettings(filter, window)
    }

    private[akka] def oldDefault(idleTimeout: FiniteDuration): PassivationStrategySettings =
      disabled.withOldIdleStrategy(idleTimeout)
  }

  // generated using kaze-class
  final class TuningParameters private (
      val bufferSize: Int,
      val coordinatorFailureBackoff: FiniteDuration,
      val entityRecoveryConstantRateStrategyFrequency: FiniteDuration,
      val entityRecoveryConstantRateStrategyNumberOfEntities: Int,
      val entityRecoveryStrategy: String,
      val entityRestartBackoff: FiniteDuration,
      val handOffTimeout: FiniteDuration,
      val keepNrOfBatches: Int,
      val leastShardAllocationMaxSimultaneousRebalance: Int,
      val leastShardAllocationRebalanceThreshold: Int,
      val rebalanceInterval: FiniteDuration,
      val retryInterval: FiniteDuration,
      val shardFailureBackoff: FiniteDuration,
      val shardStartTimeout: FiniteDuration,
      val snapshotAfter: Int,
      val updatingStateTimeout: FiniteDuration,
      val waitingForStateTimeout: FiniteDuration,
      val coordinatorStateWriteMajorityPlus: Int,
      val coordinatorStateReadMajorityPlus: Int,
      val leastShardAllocationAbsoluteLimit: Int,
      val leastShardAllocationRelativeLimit: Double,
      val passivationStopTimeout: FiniteDuration) {

    def this(classic: ClassicShardingSettings.TuningParameters) =
      this(
        bufferSize = classic.bufferSize,
        coordinatorFailureBackoff = classic.coordinatorFailureBackoff,
        retryInterval = classic.retryInterval,
        handOffTimeout = classic.handOffTimeout,
        shardStartTimeout = classic.shardStartTimeout,
        shardFailureBackoff = classic.shardFailureBackoff,
        entityRestartBackoff = classic.entityRestartBackoff,
        rebalanceInterval = classic.rebalanceInterval,
        snapshotAfter = classic.snapshotAfter,
        keepNrOfBatches = classic.keepNrOfBatches,
        leastShardAllocationRebalanceThreshold = classic.leastShardAllocationRebalanceThreshold, // TODO extract it a bit
        leastShardAllocationMaxSimultaneousRebalance = classic.leastShardAllocationMaxSimultaneousRebalance,
        waitingForStateTimeout = classic.waitingForStateTimeout,
        updatingStateTimeout = classic.updatingStateTimeout,
        entityRecoveryStrategy = classic.entityRecoveryStrategy,
        entityRecoveryConstantRateStrategyFrequency = classic.entityRecoveryConstantRateStrategyFrequency,
        entityRecoveryConstantRateStrategyNumberOfEntities = classic.entityRecoveryConstantRateStrategyNumberOfEntities,
        coordinatorStateWriteMajorityPlus = classic.coordinatorStateWriteMajorityPlus,
        coordinatorStateReadMajorityPlus = classic.coordinatorStateReadMajorityPlus,
        leastShardAllocationAbsoluteLimit = classic.leastShardAllocationAbsoluteLimit,
        leastShardAllocationRelativeLimit = classic.leastShardAllocationRelativeLimit,
        passivationStopTimeout = classic.passivationStopTimeout)

    require(
      entityRecoveryStrategy == "all" || entityRecoveryStrategy == "constant",
      s"Unknown 'entity-recovery-strategy' [$entityRecoveryStrategy], valid values are 'all' or 'constant'")

    def withBufferSize(value: Int): TuningParameters = copy(bufferSize = value)
    def withCoordinatorFailureBackoff(value: FiniteDuration): TuningParameters = copy(coordinatorFailureBackoff = value)
    def withCoordinatorFailureBackoff(value: java.time.Duration): TuningParameters =
      withCoordinatorFailureBackoff(value.toScala)
    def withEntityRecoveryConstantRateStrategyFrequency(value: FiniteDuration): TuningParameters =
      copy(entityRecoveryConstantRateStrategyFrequency = value)
    def withEntityRecoveryConstantRateStrategyFrequency(value: java.time.Duration): TuningParameters =
      withEntityRecoveryConstantRateStrategyFrequency(value.toScala)
    def withEntityRecoveryConstantRateStrategyNumberOfEntities(value: Int): TuningParameters =
      copy(entityRecoveryConstantRateStrategyNumberOfEntities = value)
    def withEntityRecoveryStrategy(value: java.lang.String): TuningParameters = copy(entityRecoveryStrategy = value)
    def withEntityRestartBackoff(value: FiniteDuration): TuningParameters = copy(entityRestartBackoff = value)
    def withEntityRestartBackoff(value: java.time.Duration): TuningParameters = withEntityRestartBackoff(value.toScala)
    def withHandOffTimeout(value: FiniteDuration): TuningParameters = copy(handOffTimeout = value)
    def withHandOffTimeout(value: java.time.Duration): TuningParameters = withHandOffTimeout(value.toScala)
    def withKeepNrOfBatches(value: Int): TuningParameters = copy(keepNrOfBatches = value)
    def withLeastShardAllocationMaxSimultaneousRebalance(value: Int): TuningParameters =
      copy(leastShardAllocationMaxSimultaneousRebalance = value)
    def withLeastShardAllocationRebalanceThreshold(value: Int): TuningParameters =
      copy(leastShardAllocationRebalanceThreshold = value)
    def withRebalanceInterval(value: FiniteDuration): TuningParameters = copy(rebalanceInterval = value)
    def withRebalanceInterval(value: java.time.Duration): TuningParameters = withRebalanceInterval(value.toScala)
    def withRetryInterval(value: FiniteDuration): TuningParameters = copy(retryInterval = value)
    def withRetryInterval(value: java.time.Duration): TuningParameters = withRetryInterval(value.toScala)
    def withShardFailureBackoff(value: FiniteDuration): TuningParameters = copy(shardFailureBackoff = value)
    def withShardFailureBackoff(value: java.time.Duration): TuningParameters = withShardFailureBackoff(value.toScala)
    def withShardStartTimeout(value: FiniteDuration): TuningParameters = copy(shardStartTimeout = value)
    def withShardStartTimeout(value: java.time.Duration): TuningParameters = withShardStartTimeout(value.toScala)
    def withSnapshotAfter(value: Int): TuningParameters = copy(snapshotAfter = value)
    def withUpdatingStateTimeout(value: FiniteDuration): TuningParameters = copy(updatingStateTimeout = value)
    def withUpdatingStateTimeout(value: java.time.Duration): TuningParameters = withUpdatingStateTimeout(value.toScala)
    def withWaitingForStateTimeout(value: FiniteDuration): TuningParameters = copy(waitingForStateTimeout = value)
    def withWaitingForStateTimeout(value: java.time.Duration): TuningParameters =
      withWaitingForStateTimeout(value.toScala)
    def withCoordinatorStateWriteMajorityPlus(value: Int): TuningParameters =
      copy(coordinatorStateWriteMajorityPlus = value)
    def withCoordinatorStateReadMajorityPlus(value: Int): TuningParameters =
      copy(coordinatorStateReadMajorityPlus = value)
    def withLeastShardAllocationAbsoluteLimit(value: Int): TuningParameters =
      copy(leastShardAllocationAbsoluteLimit = value)
    def withLeastShardAllocationRelativeLimit(value: Double): TuningParameters =
      copy(leastShardAllocationRelativeLimit = value)
    def withPassivationStopTimeout(value: FiniteDuration): TuningParameters = copy(passivationStopTimeout = value)
    def withPassivationStopTimeout(value: java.time.Duration): TuningParameters =
      withPassivationStopTimeout(value.toScala)

    private def copy(
        bufferSize: Int = bufferSize,
        coordinatorFailureBackoff: FiniteDuration = coordinatorFailureBackoff,
        entityRecoveryConstantRateStrategyFrequency: FiniteDuration = entityRecoveryConstantRateStrategyFrequency,
        entityRecoveryConstantRateStrategyNumberOfEntities: Int = entityRecoveryConstantRateStrategyNumberOfEntities,
        entityRecoveryStrategy: java.lang.String = entityRecoveryStrategy,
        entityRestartBackoff: FiniteDuration = entityRestartBackoff,
        handOffTimeout: FiniteDuration = handOffTimeout,
        keepNrOfBatches: Int = keepNrOfBatches,
        leastShardAllocationMaxSimultaneousRebalance: Int = leastShardAllocationMaxSimultaneousRebalance,
        leastShardAllocationRebalanceThreshold: Int = leastShardAllocationRebalanceThreshold,
        rebalanceInterval: FiniteDuration = rebalanceInterval,
        retryInterval: FiniteDuration = retryInterval,
        shardFailureBackoff: FiniteDuration = shardFailureBackoff,
        shardStartTimeout: FiniteDuration = shardStartTimeout,
        snapshotAfter: Int = snapshotAfter,
        updatingStateTimeout: FiniteDuration = updatingStateTimeout,
        waitingForStateTimeout: FiniteDuration = waitingForStateTimeout,
        coordinatorStateWriteMajorityPlus: Int = coordinatorStateWriteMajorityPlus,
        coordinatorStateReadMajorityPlus: Int = coordinatorStateReadMajorityPlus,
        leastShardAllocationAbsoluteLimit: Int = leastShardAllocationAbsoluteLimit,
        leastShardAllocationRelativeLimit: Double = leastShardAllocationRelativeLimit,
        passivationStopTimeout: FiniteDuration = passivationStopTimeout): TuningParameters =
      new TuningParameters(
        bufferSize = bufferSize,
        coordinatorFailureBackoff = coordinatorFailureBackoff,
        entityRecoveryConstantRateStrategyFrequency = entityRecoveryConstantRateStrategyFrequency,
        entityRecoveryConstantRateStrategyNumberOfEntities = entityRecoveryConstantRateStrategyNumberOfEntities,
        entityRecoveryStrategy = entityRecoveryStrategy,
        entityRestartBackoff = entityRestartBackoff,
        handOffTimeout = handOffTimeout,
        keepNrOfBatches = keepNrOfBatches,
        leastShardAllocationMaxSimultaneousRebalance = leastShardAllocationMaxSimultaneousRebalance,
        leastShardAllocationRebalanceThreshold = leastShardAllocationRebalanceThreshold,
        rebalanceInterval = rebalanceInterval,
        retryInterval = retryInterval,
        shardFailureBackoff = shardFailureBackoff,
        shardStartTimeout = shardStartTimeout,
        snapshotAfter = snapshotAfter,
        updatingStateTimeout = updatingStateTimeout,
        waitingForStateTimeout = waitingForStateTimeout,
        coordinatorStateWriteMajorityPlus = coordinatorStateWriteMajorityPlus,
        coordinatorStateReadMajorityPlus = coordinatorStateReadMajorityPlus,
        leastShardAllocationAbsoluteLimit = leastShardAllocationAbsoluteLimit,
        leastShardAllocationRelativeLimit = leastShardAllocationRelativeLimit,
        passivationStopTimeout = passivationStopTimeout)

    override def toString =
      s"""TuningParameters($bufferSize,$coordinatorFailureBackoff,$entityRecoveryConstantRateStrategyFrequency,$entityRecoveryConstantRateStrategyNumberOfEntities,$entityRecoveryStrategy,$entityRestartBackoff,$handOffTimeout,$keepNrOfBatches,$leastShardAllocationMaxSimultaneousRebalance,$leastShardAllocationRebalanceThreshold,$rebalanceInterval,$retryInterval,$shardFailureBackoff,$shardStartTimeout,$snapshotAfter,$updatingStateTimeout,$waitingForStateTimeout,$coordinatorStateReadMajorityPlus,$coordinatorStateReadMajorityPlus,$leastShardAllocationAbsoluteLimit,$leastShardAllocationRelativeLimit,$passivationStopTimeout)"""
  }
}

/**
 * @param numberOfShards number of shards used by the default [[HashCodeMessageExtractor]]
 * @param role Specifies that this entity type requires cluster nodes with a specific role.
 *   If the role is not specified all nodes in the cluster are used. If the given role does
 *   not match the role of the current node the `ShardRegion` will be started in proxy mode.
 * @param dataCenter The data center of the cluster nodes where the cluster sharding is running.
 *   If the dataCenter is not specified then the same data center as current node. If the given
 *   dataCenter does not match the data center of the current node the `ShardRegion` will be started
 *   in proxy mode.
 * @param rememberEntities true if active entity actors shall be automatically restarted upon `Shard`
 *   restart. i.e. if the `Shard` is started on a different `ShardRegion` due to rebalance or crash.
 * @param journalPluginId Absolute path to the journal plugin configuration entity that is to
 *   be used for the internal persistence of ClusterSharding. If not defined the default
 *   journal plugin is used. Note that this is not related to persistence used by the entity
 *   actors.
 * @param passivationStrategySettings settings for automatic passivation strategy, see descriptions in reference.conf
 * @param snapshotPluginId Absolute path to the snapshot plugin configuration entity that is to
 *   be used for the internal persistence of ClusterSharding. If not defined the default
 *   snapshot plugin is used. Note that this is not related to persistence used by the entity
 *   actors.
 * @param tuningParameters additional tuning parameters, see descriptions in reference.conf
 * @param leaseSettings LeaseSettings for acquiring before creating the shard.
 *   Note that if you define a custom lease name and have several sharding entity types each one must have a unique
 *   lease name. If the lease name is undefined it will be derived from ActorSystem name and shard name,
 *   but that may result in too long lease names.
 */
@nowarn("msg=Use Akka Distributed Cluster")
final class ClusterShardingSettings(
    val numberOfShards: Int,
    val role: Option[String],
    @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
    val dataCenter: Option[DataCenter],
    val rememberEntities: Boolean,
    val journalPluginId: String,
    val snapshotPluginId: String,
    val passivationStrategySettings: ClusterShardingSettings.PassivationStrategySettings,
    val shardRegionQueryTimeout: FiniteDuration,
    val stateStoreMode: ClusterShardingSettings.StateStoreMode,
    val rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode,
    val tuningParameters: ClusterShardingSettings.TuningParameters,
    val coordinatorSingletonOverrideRole: Boolean,
    val coordinatorSingletonSettings: ClusterSingletonManagerSettings,
    val leaseSettings: Option[LeaseUsageSettings]) {

  @deprecated("Use constructor with coordinatorSingletonOverrideRole", "2.6.20")
  def this(
      numberOfShards: Int,
      role: Option[String],
      dataCenter: Option[DataCenter],
      rememberEntities: Boolean,
      journalPluginId: String,
      snapshotPluginId: String,
      passivationStrategySettings: ClusterShardingSettings.PassivationStrategySettings,
      shardRegionQueryTimeout: FiniteDuration,
      stateStoreMode: ClusterShardingSettings.StateStoreMode,
      rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode,
      tuningParameters: ClusterShardingSettings.TuningParameters,
      coordinatorSingletonSettings: ClusterSingletonManagerSettings,
      leaseSettings: Option[LeaseUsageSettings]) =
    this(
      numberOfShards,
      role,
      dataCenter,
      rememberEntities,
      journalPluginId,
      snapshotPluginId,
      passivationStrategySettings,
      shardRegionQueryTimeout,
      stateStoreMode,
      rememberEntitiesStoreMode,
      tuningParameters,
      coordinatorSingletonOverrideRole = true,
      coordinatorSingletonSettings,
      leaseSettings)

  @deprecated("Use constructor with passivationStrategySettings", "2.6.18")
  def this(
      numberOfShards: Int,
      role: Option[String],
      dataCenter: Option[DataCenter],
      rememberEntities: Boolean,
      journalPluginId: String,
      snapshotPluginId: String,
      passivateIdleEntityAfter: FiniteDuration,
      shardRegionQueryTimeout: FiniteDuration,
      stateStoreMode: ClusterShardingSettings.StateStoreMode,
      rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode,
      tuningParameters: ClusterShardingSettings.TuningParameters,
      coordinatorSingletonSettings: ClusterSingletonManagerSettings,
      leaseSettings: Option[LeaseUsageSettings]) =
    this(
      numberOfShards,
      role,
      dataCenter,
      rememberEntities,
      journalPluginId,
      snapshotPluginId,
      ClusterShardingSettings.PassivationStrategySettings.oldDefault(passivateIdleEntityAfter),
      shardRegionQueryTimeout,
      stateStoreMode,
      rememberEntitiesStoreMode,
      tuningParameters,
      coordinatorSingletonOverrideRole = true,
      coordinatorSingletonSettings,
      leaseSettings)

  @deprecated("Use constructor with leaseSettings", "2.6.11")
  def this(
      numberOfShards: Int,
      role: Option[String],
      dataCenter: Option[DataCenter],
      rememberEntities: Boolean,
      journalPluginId: String,
      snapshotPluginId: String,
      passivateIdleEntityAfter: FiniteDuration,
      shardRegionQueryTimeout: FiniteDuration,
      stateStoreMode: ClusterShardingSettings.StateStoreMode,
      rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode,
      tuningParameters: ClusterShardingSettings.TuningParameters,
      coordinatorSingletonSettings: ClusterSingletonManagerSettings) =
    this(
      numberOfShards,
      role,
      dataCenter,
      rememberEntities,
      journalPluginId,
      snapshotPluginId,
      passivateIdleEntityAfter,
      shardRegionQueryTimeout,
      stateStoreMode,
      rememberEntitiesStoreMode,
      tuningParameters,
      coordinatorSingletonSettings,
      None)

  @deprecated("Use constructor with rememberEntitiesStoreMode", "2.6.6")
  def this(
      numberOfShards: Int,
      role: Option[String],
      dataCenter: Option[DataCenter],
      rememberEntities: Boolean,
      journalPluginId: String,
      snapshotPluginId: String,
      passivateIdleEntityAfter: FiniteDuration,
      shardRegionQueryTimeout: FiniteDuration,
      stateStoreMode: ClusterShardingSettings.StateStoreMode,
      tuningParameters: ClusterShardingSettings.TuningParameters,
      coordinatorSingletonSettings: ClusterSingletonManagerSettings) =
    this(
      numberOfShards,
      role,
      dataCenter,
      rememberEntities,
      journalPluginId,
      snapshotPluginId,
      passivateIdleEntityAfter,
      shardRegionQueryTimeout,
      stateStoreMode,
      RememberEntitiesStoreModeDData,
      tuningParameters,
      coordinatorSingletonSettings,
      None)

  /**
   * INTERNAL API
   * If true, this node should run the shard region, otherwise just a shard proxy should started on this node.
   * It's checking if the `role` and `dataCenter` are matching.
   */
  @InternalApi
  private[akka] def shouldHostShard(cluster: Cluster): Boolean =
    role.forall(cluster.selfMember.roles.contains) &&
    dataCenter.forall(_ == cluster.selfMember.dataCenter)

  // no withNumberOfShards because it should be defined in configuration to be able to verify same
  // value on all nodes with `JoinConfigCompatChecker`

  def withRole(role: String): ClusterShardingSettings = copy(role = ClusterShardingSettings.option(role))

  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def withDataCenter(dataCenter: DataCenter): ClusterShardingSettings =
    copy(dataCenter = ClusterShardingSettings.option(dataCenter))

  def withRememberEntities(rememberEntities: Boolean): ClusterShardingSettings =
    copy(rememberEntities = rememberEntities)

  def withJournalPluginId(journalPluginId: String): ClusterShardingSettings =
    copy(journalPluginId = journalPluginId)

  def withSnapshotPluginId(snapshotPluginId: String): ClusterShardingSettings =
    copy(snapshotPluginId = snapshotPluginId)

  def withTuningParameters(tuningParameters: ClusterShardingSettings.TuningParameters): ClusterShardingSettings =
    copy(tuningParameters = tuningParameters)

  def withStateStoreMode(stateStoreMode: ClusterShardingSettings.StateStoreMode): ClusterShardingSettings =
    copy(stateStoreMode = stateStoreMode)

  def withRememberEntitiesStoreMode(
      rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode): ClusterShardingSettings =
    copy(rememberEntitiesStoreMode = rememberEntitiesStoreMode)

  @deprecated("See passivationStrategySettings.idleEntitySettings instead", since = "2.6.18")
  def passivateIdleEntityAfter: FiniteDuration =
    passivationStrategySettings.idleEntitySettings.fold(Duration.Zero)(_.timeout)

  @deprecated("Use withPassivationStrategy instead", since = "2.6.18")
  def withPassivateIdleEntityAfter(duration: FiniteDuration): ClusterShardingSettings =
    copy(passivationStrategySettings = passivationStrategySettings.withOldIdleStrategy(duration))

  @deprecated("Use withPassivationStrategy instead", since = "2.6.18")
  def withPassivateIdleEntityAfter(duration: java.time.Duration): ClusterShardingSettings =
    copy(passivationStrategySettings = passivationStrategySettings.withOldIdleStrategy(duration.toScala))

  /**
   * API MAY CHANGE: Settings for passivation strategies may change after additional testing and feedback.
   */
  @ApiMayChange
  def withPassivationStrategy(settings: ClusterShardingSettings.PassivationStrategySettings): ClusterShardingSettings =
    copy(passivationStrategySettings = settings)

  def withNoPassivationStrategy(): ClusterShardingSettings =
    copy(passivationStrategySettings = ClusterShardingSettings.PassivationStrategySettings.disabled)

  def withShardRegionQueryTimeout(duration: FiniteDuration): ClusterShardingSettings =
    copy(shardRegionQueryTimeout = duration)

  def withShardRegionQueryTimeout(duration: java.time.Duration): ClusterShardingSettings =
    copy(shardRegionQueryTimeout = duration.toScala)

  /**
   * Note that if you define a custom lease name and have several sharding entity types each one must have a unique
   * lease name. If the lease name is undefined it will be derived from ActorSystem name and shard name,
   * but that may result in too long lease names.
   */
  def withLeaseSettings(leaseSettings: LeaseUsageSettings) = copy(leaseSettings = Option(leaseSettings))

  /**
   * The `role` of the `ClusterSingletonManagerSettings` is not used. The `role` of the
   * coordinator singleton will be the same as the `role` of `ClusterShardingSettings`.
   */
  def withCoordinatorSingletonSettings(
      coordinatorSingletonSettings: ClusterSingletonManagerSettings): ClusterShardingSettings =
    copy(coordinatorSingletonSettings = coordinatorSingletonSettings)

  private def copy(
      role: Option[String] = role,
      dataCenter: Option[DataCenter] = dataCenter,
      rememberEntities: Boolean = rememberEntities,
      journalPluginId: String = journalPluginId,
      snapshotPluginId: String = snapshotPluginId,
      stateStoreMode: ClusterShardingSettings.StateStoreMode = stateStoreMode,
      rememberEntitiesStoreMode: ClusterShardingSettings.RememberEntitiesStoreMode = rememberEntitiesStoreMode,
      tuningParameters: ClusterShardingSettings.TuningParameters = tuningParameters,
      coordinatorSingletonOverrideRole: Boolean = coordinatorSingletonOverrideRole,
      coordinatorSingletonSettings: ClusterSingletonManagerSettings = coordinatorSingletonSettings,
      passivationStrategySettings: ClusterShardingSettings.PassivationStrategySettings = passivationStrategySettings,
      shardRegionQueryTimeout: FiniteDuration = shardRegionQueryTimeout,
      leaseSettings: Option[LeaseUsageSettings] = leaseSettings): ClusterShardingSettings =
    new ClusterShardingSettings(
      numberOfShards,
      role,
      dataCenter,
      rememberEntities,
      journalPluginId,
      snapshotPluginId,
      passivationStrategySettings,
      shardRegionQueryTimeout,
      stateStoreMode,
      rememberEntitiesStoreMode,
      tuningParameters,
      coordinatorSingletonOverrideRole,
      coordinatorSingletonSettings,
      leaseSettings)
}
