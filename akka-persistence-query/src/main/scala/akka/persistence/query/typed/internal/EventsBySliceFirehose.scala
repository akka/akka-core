/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.internal

import java.time.{ Duration => JDuration }
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.jdk.DurationConverters._
import scala.util.control.NoStackTrace

import com.typesafe.config.Config

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.stream.Attributes
import akka.stream.FanInShape2
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.Outlet
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.StageLogging
import akka.util.OptionVal

/**
 * INTERNAL API
 *
 * The purpose is to share the stream of events from the database and fan out to connected consumer
 * streams. Thereby less queries and loading of events from the database. The shared stream is called
 * the firehose stream.
 *
 * The fan out of the firehose stream is via a `BroadcastHub` that consumer streams dynamically attach to.
 *
 * A new consumer starts a catchup stream since the start offset typically is behind the live
 * firehose stream. In the beginning it will emit events only from the catchup stream. Offset progress
 * for the firehose stream is tracked and when the catchup stream has caught up with the firehose
 * stream it will switch over to emitting from firehose stream and close the catchup stream. During
 * an overlap period of time it will use events from both catchup and firehose streams to make sure
 * that no events are missed. During this overlap time there is best effort deduplication.
 *
 * The `BroadcastHub` has a limited buffer that holds events between the slowest and fastest consumer.
 * When the buffer is full the fastest consumer can't progress faster than the slowest. Short periods of
 * slow down can be fine, but after a while the slow consumers are detected and aborted. They have to
 * connect again and try catching up, but without slowing down other streams.
 *
 * The firehose stream is started on demand when the first consumer is attaching. It will be stopped
 * when the last consumer is stopped, but it stays around for a while to make it more efficient for
 * new or restarted consumers to attach again.
 */
@InternalApi private[akka] object EventsBySliceFirehose
    extends ExtensionId[EventsBySliceFirehose]
    with ExtensionIdProvider {

  override def get(system: ActorSystem): EventsBySliceFirehose = super.get(system)

  override def get(system: ClassicActorSystemProvider): EventsBySliceFirehose = super.get(system)

  override def lookup = EventsBySliceFirehose

  override def createExtension(system: ExtendedActorSystem): EventsBySliceFirehose =
    new EventsBySliceFirehose(system)

  object Settings {
    def apply(system: ActorSystem, pluginId: String): Settings =
      apply(system.settings.config.getConfig(pluginId))

    def apply(config: Config): Settings =
      Settings(
        delegateQueryPluginId = delegateQueryPluginId(config),
        broadcastBufferSize = config.getInt("broadcast-buffer-size"),
        firehoseLingerTimeout = config.getDuration("firehose-linger-timeout").toScala,
        catchupOverlap = config.getDuration("catchup-overlap"),
        deduplicationCapacity = config.getInt("deduplication-capacity"),
        slowConsumerReaperInterval = config.getDuration("slow-consumer-reaper-interval").toScala,
        slowConsumerLagThreshold = config.getDuration("slow-consumer-lag-threshold"),
        abortSlowConsumerAfter = config.getDuration("abort-slow-consumer-after"),
        verboseLogging = config.getBoolean("verbose-debug-logging"))

    def delegateQueryPluginId(config: Config): String =
      config.getString("delegate-query-plugin-id")
  }

  final case class Settings(
      delegateQueryPluginId: String,
      broadcastBufferSize: Int,
      firehoseLingerTimeout: FiniteDuration,
      catchupOverlap: JDuration,
      deduplicationCapacity: Int,
      slowConsumerReaperInterval: FiniteDuration,
      slowConsumerLagThreshold: JDuration,
      abortSlowConsumerAfter: JDuration,
      verboseLogging: Boolean) {
    require(
      delegateQueryPluginId != null && delegateQueryPluginId.nonEmpty,
      "Configuration of delegate-query-plugin-id must defined.")
  }

  final class SlowConsumerException(message: String) extends RuntimeException(message) with NoStackTrace

  final case class FirehoseKey(pluginId: String, entityType: String, sliceRange: Range)

  final case class ConsumerTracking(
      consumerId: String,
      history: Vector[TimestampOffset],
      firehoseOnly: Boolean,
      consumerKillSwitch: KillSwitch,
      slowConsumerCandidate: Option[Instant]) {
    def offsetTimestamp: Instant =
      if (history.isEmpty) Instant.EPOCH
      else history.last.timestamp
  }

  final class Firehose(
      val firehoseKey: FirehoseKey,
      val settings: Settings,
      val firehoseHub: Source[EventEnvelope[Any], NotUsed],
      firehoseKillSwitch: KillSwitch,
      log: LoggingAdapter) {

    val consumerTracking: ConcurrentHashMap[String, ConsumerTracking] = new ConcurrentHashMap
    @volatile private var firehoseIsShutdown = false

    private def entityType = firehoseKey.entityType
    private val sliceRangeStr = s"${firehoseKey.sliceRange.min}-${firehoseKey.sliceRange.max}"

    private def consumerTrackingValues(): Vector[ConsumerTracking] = {
      import scala.jdk.CollectionConverters._
      consumerTracking.values.iterator.asScala.filter(h => h.history.nonEmpty && h.firehoseOnly).toVector
    }

    def consumerStarted(consumerId: String, consumerKillSwitch: KillSwitch): Unit = {
      log.debug("Firehose entityType [{}] sliceRange [{}] consumer [{}] started", entityType, sliceRangeStr, consumerId)
      consumerTracking.putIfAbsent(
        consumerId,
        ConsumerTracking(consumerId, history = Vector.empty, firehoseOnly = false, consumerKillSwitch, None))
    }

    def consumerTerminated(consumerId: String): Int = {
      log.debug(
        "Firehose entityType [{}] sliceRange [{}] consumer [{}] terminated",
        entityType,
        sliceRangeStr,
        consumerId)
      consumerTracking.remove(consumerId)
      consumerTracking.size
    }

    def shutdownFirehoseIfNoConsumers(): Boolean = {
      if (consumerTracking.isEmpty) {
        log.debug("Firehose entityType [{}] sliceRange [{}] is shutting down, no consumers", entityType, sliceRangeStr)
        firehoseIsShutdown = true
        firehoseKillSwitch.shutdown()
        true
      } else
        false
    }

    def isShutdown: Boolean =
      firehoseIsShutdown

    def updateConsumerTracking(
        consumerId: String,
        now: Instant,
        offset: TimestampOffset,
        consumerKillSwitch: KillSwitch): Unit = {

      val newTracking = consumerTracking.compute(
        consumerId,
        (_, existing) => {
          if (existing == null)
            ConsumerTracking(consumerId, history = Vector(offset), firehoseOnly = false, consumerKillSwitch, None)
          else {
            val newHistory =
              if (existing.history.size <= settings.broadcastBufferSize)
                existing.history :+ offset
              else
                existing.history.tail :+ offset // drop one, add one
            existing.copy(history = newHistory)
          }
        })
      logUpdateConsumerTracking(consumerId, now, newTracking)
    }

    private def logUpdateConsumerTracking(consumerId: String, now: Instant, tracking: ConsumerTracking): Unit = {
      if (settings.verboseLogging && log.isDebugEnabled) {
        val trackingValues = consumerTrackingValues()
        if (trackingValues.size > 1) {
          val slowestConsumer = trackingValues.minBy(_.offsetTimestamp)
          val fastestConsumer = trackingValues.maxBy(_.offsetTimestamp)
          val behind = elementsBehind(fastestConsumer.history, slowestConsumer.history)
          if (behind > 0) {
            val diffSlowestFastestsMillis = fastestConsumer.offsetTimestamp.toEpochMilli - slowestConsumer.offsetTimestamp.toEpochMilli
            val fastestLagMillis = now.toEpochMilli - fastestConsumer.offsetTimestamp.toEpochMilli

            val diffFastest = fastestConsumer.offsetTimestamp.toEpochMilli - tracking.offsetTimestamp.toEpochMilli
            val diffFastestStr =
              if (diffFastest > 0) s"behind fastest [$diffFastest] ms"
              else if (diffFastest < 0) s"ahead of fastest [$diffFastest] ms" // not possible
              else "same as fastest"
            val diffSlowest = slowestConsumer.offsetTimestamp.toEpochMilli - tracking.offsetTimestamp.toEpochMilli
            val diffSlowestStr =
              if (diffSlowest > 0) s"behind slowest [$diffSlowest] ms" // not possible
              else if (diffSlowest < 0) s"ahead of slowest [${-diffSlowest}] ms"
              else "same as slowest"
            val consumerBehind = elementsBehind(fastestConsumer.history, tracking.history)

            log.debug(
              s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr] updateConsumerTracking [$consumerId], " +
              s"behind [$consumerBehind] events from fastest, " +
              s"$diffFastestStr, $diffSlowestStr, " +
              s"slowest [${slowestConsumer.consumerId}] is behind [$behind] " +
              s"events from fastest [${fastestConsumer.consumerId}], [$diffSlowestFastestsMillis] ms. " +
              s"Consumer lag of fastest [$fastestLagMillis] ms.")
          }
        }
      }
    }

    def detectSlowConsumers(now: Instant): Unit = {
      val trackingValues = consumerTrackingValues()
      if (trackingValues.size > 1) {
        val slowestConsumer = trackingValues.minBy(_.offsetTimestamp)
        val fastestConsumer = trackingValues.maxBy(_.offsetTimestamp)

        val behind = elementsBehind(fastestConsumer.history, slowestConsumer.history)
        val fastestLagMillis = now.toEpochMilli - fastestConsumer.offsetTimestamp.toEpochMilli

        if (behind >= settings.broadcastBufferSize / 2 && fastestLagMillis > settings.slowConsumerLagThreshold.toMillis) {
          logDetectSlowConsumers(trackingValues, slowestConsumer, fastestConsumer, behind, fastestLagMillis)

          val changedConsumerTrackingValues = trackingValues.flatMap { tracking =>
            val consumerBehind = elementsBehind(fastestConsumer.history, tracking.history)
            if (consumerBehind >= settings.broadcastBufferSize / 2) {
              if (tracking.slowConsumerCandidate.isDefined)
                None // keep original
              else
                Some(tracking.copy(slowConsumerCandidate = Some(now)))
            } else if (tracking.slowConsumerCandidate.isDefined) {
              Some(tracking.copy(slowConsumerCandidate = None)) // not slow any more
            } else {
              None
            }
          }

          changedConsumerTrackingValues.foreach { tracking =>
            consumerTracking.computeIfPresent(
              tracking.consumerId,
              (_, existing) => existing.copy(slowConsumerCandidate = tracking.slowConsumerCandidate))
          }

          val newTrackingValues = consumerTrackingValues()

          val confirmedSlowConsumers = newTrackingValues.filter {
            _.slowConsumerCandidate.exists(isDurationGreaterThan(_, now, settings.abortSlowConsumerAfter))
          }

          if (confirmedSlowConsumers.nonEmpty) {
            if (log.isInfoEnabled) {
              val behindMillis = fastestConsumer.offsetTimestamp.toEpochMilli - confirmedSlowConsumers
                  .maxBy(_.offsetTimestamp)
                  .offsetTimestamp
                  .toEpochMilli
              log.info(
                s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr], [${confirmedSlowConsumers.size}] " +
                s"slow consumers are aborted [${confirmedSlowConsumers.map(_.consumerId).mkString(", ")}], " +
                s"behind by [$behind] events [$behindMillis] ms.")
            }

            confirmedSlowConsumers.foreach { tracking =>
              tracking.consumerKillSwitch.abort(
                new SlowConsumerException(s"Consumer [${tracking.consumerId}] is too slow."))
            }
          }

        } else if (behind < 0) {
          // can happen if fastest is a new consumer, but ignoring should be ok
          log.debug(
            s"Firehose entityType [{}] sliceRange [{}] missing history for fastest consumer [{}] corresponding to slowest consumer [{}]",
            entityType,
            sliceRangeStr,
            fastestConsumer.consumerId,
            slowestConsumer.consumerId)
        } else if (settings.verboseLogging) {
          logDetectSlowConsumers(trackingValues, slowestConsumer, fastestConsumer, behind, fastestLagMillis)
        }

      }
    }

    private def logDetectSlowConsumers(
        consumerTrackingValues: Vector[ConsumerTracking],
        slowestConsumer: ConsumerTracking,
        fastestConsumer: ConsumerTracking,
        behind: Int,
        fastestLagMillis: Long): Unit = {
      if (behind > 0) {
        val behindMillis = fastestConsumer.offsetTimestamp.toEpochMilli - slowestConsumer.offsetTimestamp.toEpochMilli
        log.debug(
          s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr] detectSlowConsumers, " +
          s"slowest [${slowestConsumer.consumerId}] is behind [$behind] " +
          s"events from fastest [${fastestConsumer.consumerId}], [$behindMillis] ms. " +
          s"Consumer lag of fastest [$fastestLagMillis] ms")

        consumerTrackingValues.foreach { tracking =>
          val diffFastest = fastestConsumer.offsetTimestamp.toEpochMilli - tracking.offsetTimestamp.toEpochMilli
          val diffFastestStr =
            if (diffFastest > 0) s"behind fastest [$diffFastest] ms"
            else if (diffFastest < 0) s"ahead of fastest [$diffFastest] ms" // not possible
            else "same as fastest"
          val diffSlowest = slowestConsumer.offsetTimestamp.toEpochMilli - tracking.offsetTimestamp.toEpochMilli
          val diffSlowestStr =
            if (diffSlowest > 0) s"behind slowest [$diffSlowest] ms" // not possible
            else if (diffSlowest < 0) s"ahead of slowest [${-diffSlowest}] ms"
            else "same as slowest"
          val consumerBehind = elementsBehind(fastestConsumer.history, tracking.history)

          val logMessage = s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr] consumer [${tracking.consumerId}], " +
            s"behind [$consumerBehind] events from fastest, " +
            s"$diffFastestStr, $diffSlowestStr, firehoseOnly [${tracking.firehoseOnly}]"

          log.debug(logMessage)
        }
      }
    }

    private def elementsBehind(fastest: Vector[TimestampOffset], slowest: Vector[TimestampOffset]): Int = {
      if (fastest.last == slowest.last) {
        0
      } else {
        val i = fastest.indexOf(slowest.last)
        if (i >= 0)
          fastest.size - i - 1
        else
          -1
      }
    }

    @tailrec def updateConsumerFirehoseOnly(consumerId: String): Unit = {
      val existingTracking = consumerTracking.get(consumerId)
      val tracking = existingTracking match {
        case null =>
          throw new IllegalStateException(s"Expected existing tracking for consumer [$consumerId]")
        case existing =>
          existing.copy(firehoseOnly = true)
      }

      if (!consumerTracking.replace(consumerId, existingTracking, tracking))
        // concurrent update, try again
        updateConsumerFirehoseOnly(consumerId)
    }

  }

  def timestampOffset(env: EventEnvelope[Any]): TimestampOffset =
    env match {
      case eventEnvelope: EventEnvelope[_] if eventEnvelope.offset.isInstanceOf[TimestampOffset] =>
        eventEnvelope.offset.asInstanceOf[TimestampOffset]
      case _ =>
        throw new IllegalArgumentException(s"Expected TimestampOffset, but was [${env.offset.getClass.getName}]")
    }

  def isDurationGreaterThan(from: Instant, to: Instant, duration: JDuration): Boolean =
    JDuration.between(from, to).compareTo(duration) > 0
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventsBySliceFirehose(system: ActorSystem) extends Extension {
  import EventsBySliceFirehose._
  private val log = Logging(system, classOf[EventsBySliceFirehose])
  private val firehoses = new ConcurrentHashMap[FirehoseKey, Firehose]()

  @tailrec final def getFirehose(firehoseKey: FirehoseKey): Firehose = {
    val firehose = firehoses.computeIfAbsent(firehoseKey, key => createFirehose(key))
    if (firehose.isShutdown) {
      // concurrency race condition, but it should be removed
      firehoses.remove(firehoseKey, firehose)
      getFirehose(firehoseKey) // try again
    } else
      firehose
  }

  private def createFirehose(key: FirehoseKey): Firehose = {
    implicit val sys: ActorSystem = system
    val sliceRangeStr = s"${key.sliceRange.min}-${key.sliceRange.max}"

    log.debug("Create firehose entityType [{}], sliceRange [{}]", key.entityType, sliceRangeStr)

    val settings = Settings(sys, key.pluginId)

    val firehoseKillSwitch = KillSwitches.shared("firehoseKillSwitch")

    val firehoseHub: Source[EventEnvelope[Any], NotUsed] =
      underlyingEventsBySlices[Any](
        settings.delegateQueryPluginId,
        key.entityType,
        key.sliceRange.min,
        key.sliceRange.max,
        TimestampOffset(Instant.now(), Map.empty),
        firehose = true)
        .via(firehoseKillSwitch.flow)
        .runWith(BroadcastHub.sink[EventEnvelope[Any]](settings.broadcastBufferSize))

    val firehose = new Firehose(key, settings, firehoseHub, firehoseKillSwitch, log)

    val reaperInterval = settings.slowConsumerReaperInterval
    // var because it is used inside the scheduled block to cancel itself
    var reaperTask: Cancellable = null
    reaperTask = system.scheduler.scheduleWithFixedDelay(reaperInterval, reaperInterval) { () =>
      if (reaperTask == null)
        () // theoretical possibility but would only mean that the first tick is ignored
      else if (firehose.isShutdown)
        reaperTask.cancel()
      else
        firehose.detectSlowConsumers(Instant.now)
    }(system.dispatcher)

    firehose
  }

  def eventsBySlices[Event](
      pluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    val sliceRange = minSlice to maxSlice
    val firehoseKey = FirehoseKey(pluginId, entityType, sliceRange)
    val firehose = getFirehose(firehoseKey)
    val catchupSource = underlyingEventsBySlices[Event](
      firehose.settings.delegateQueryPluginId,
      entityType,
      minSlice,
      maxSlice,
      offset,
      firehose = false)
    eventsBySlicesImpl(firehose, catchupSource)
  }

  def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
      pluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    val sliceRange = minSlice to maxSlice
    val firehoseKey = FirehoseKey(pluginId, entityType, sliceRange)
    val firehose = getFirehose(firehoseKey)
    val catchupSource = underlyingEventsBySlicesStartingFromSnapshots[Snapshot, Event](
      firehose.settings.delegateQueryPluginId,
      entityType,
      minSlice,
      maxSlice,
      offset,
      transformSnapshot)
    eventsBySlicesImpl(firehose, catchupSource)
  }

  private def eventsBySlicesImpl[Event](
      firehose: Firehose,
      catchupSource: Source[EventEnvelope[Event], NotUsed]): Source[EventEnvelope[Event], NotUsed] = {

    val firehoseKey = firehose.firehoseKey
    val settings = firehose.settings
    val consumerId = UUID.randomUUID().toString

    def consumerTerminated(): Unit = {
      if (firehose.consumerTerminated(consumerId) == 0) {
        // Don't shutdown firehose immediately because Projection it should survive Projection restart
        system.scheduler.scheduleOnce(settings.firehoseLingerTimeout) {
          if (firehose.shutdownFirehoseIfNoConsumers())
            firehoses.remove(firehoseKey)
        }(system.dispatcher)
      }
    }

    val catchupKillSwitch = KillSwitches.shared("catchupKillSwitch")
    val catchupSourceWithKillSwitch =
      catchupSource.asInstanceOf[Source[EventEnvelope[Any], NotUsed]].via(catchupKillSwitch.flow)

    val consumerKillSwitch = KillSwitches.shared("consumerKillSwitch")

    val firehoseSource = firehose.firehoseHub

    import GraphDSL.Implicits._
    val catchupOrFirehose = GraphDSL.createGraph(catchupSourceWithKillSwitch) { implicit b => r =>
      val merge = b.add(new CatchupOrFirehose(consumerId, firehose, catchupKillSwitch))
      r ~> merge.in1
      FlowShape(merge.in0, merge.out)
    }

    firehoseSource
      .via(catchupOrFirehose)
      .map { env =>
        // don't look at pub-sub or backtracking events
        if (env.source == "")
          firehose.updateConsumerTracking(consumerId, Instant.now(), timestampOffset(env), consumerKillSwitch)
        env.asInstanceOf[EventEnvelope[Event]]
      }
      .via(consumerKillSwitch.flow)
      .watchTermination()(Keep.right)
      .mapMaterializedValue { termination =>
        firehose.consumerStarted(consumerId, consumerKillSwitch)
        termination.onComplete { _ =>
          consumerTerminated()
        }(system.dispatcher)
        NotUsed
      }
  }

  // can be overridden in tests
  protected def underlyingEventsBySlices[Event](
      pluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      @nowarn("msg=never used") firehose: Boolean): Source[EventEnvelope[Event], NotUsed] = {
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceQuery](pluginId)
      .eventsBySlices(entityType, minSlice, maxSlice, offset)
  }

  private def underlyingEventsBySlicesStartingFromSnapshots[Snapshot, Event](
      pluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceStartingFromSnapshotsQuery](pluginId)
      .eventsBySlicesStartingFromSnapshots(entityType, minSlice, maxSlice, offset, transformSnapshot)
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CatchupOrFirehose {
  private sealed trait Mode
  private case object CatchUpOnly extends Mode
  private final case class Both(caughtUpTimestamp: Instant) extends Mode
  private case object FirehoseOnly extends Mode

  private case class DeduplicationCacheEntry(pid: String, seqNr: Long, source: String)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CatchupOrFirehose(
    consumerId: String,
    firehose: EventsBySliceFirehose.Firehose,
    catchupKillSwitch: KillSwitch)
    extends GraphStage[FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]] {
  import CatchupOrFirehose._
  import EventsBySliceFirehose.isDurationGreaterThan
  import EventsBySliceFirehose.timestampOffset
  import firehose.firehoseKey.entityType
  import firehose.settings

  override def initialAttributes = Attributes.name("CatchupOrFirehose")
  override val shape: FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]] =
    new FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]("CatchupOrFirehose")
  def out: Outlet[EventEnvelope[Any]] = shape.out
  val firehoseInlet: Inlet[EventEnvelope[Any]] = shape.in0
  val catchupInlet: Inlet[EventEnvelope[Any]] = shape.in1

  private val sliceRangeStr = s"${firehose.firehoseKey.sliceRange.min}-${firehose.firehoseKey.sliceRange.max}"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      // Without this the completion signalling would take one extra pull
      private def willShutDown: Boolean = isClosed(firehoseInlet)

      private val firehoseHandler = new FirehoseHandler(firehoseInlet)
      private val catchupHandler = new CatchupHandler(catchupInlet)

      private var mode: Mode = CatchUpOnly

      // cache of seen pid/seqNr
      private var deduplicationCache = mutable.LinkedHashSet.empty[DeduplicationCacheEntry]
      private val deduplicationCacheEvictThreshold = (settings.deduplicationCapacity * 1.1).toInt

      override protected def logSource: Class[_] = classOf[CatchupOrFirehose]

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          tryPushOutput()
          tryPullAllIfNeeded()
        }
      })

      setHandler(firehoseInlet, firehoseHandler)
      setHandler(catchupInlet, catchupHandler)

      private def tryPushOutput(): Unit = {
        def tryPushFirehoseValue(deduplicate: Boolean): Boolean =
          firehoseHandler.value match {
            case OptionVal.Some(env) =>
              firehoseHandler.value = OptionVal.None
              if (!(deduplicate && isDuplicate(env))) {
                if (settings.verboseLogging && log.isDebugEnabled)
                  log.debug(
                    s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr] consumer [$consumerId] push from " +
                    s" firehose [${env.persistenceId}] seqNr [${env.sequenceNr}], source [${env.source}]")
                push(out, env)
                true
              } else
                false
            case _ =>
              false
          }

        def tryPushCatchupValue(deduplicate: Boolean): Boolean =
          catchupHandler.value match {
            case OptionVal.Some(env) =>
              catchupHandler.value = OptionVal.None
              if (!(deduplicate && isDuplicate(env))) {
                if (settings.verboseLogging && log.isDebugEnabled)
                  log.debug(
                    s"Firehose entityType [$entityType] sliceRange [$sliceRangeStr] consumer [$consumerId] push from " +
                    s"catchup [${env.persistenceId}] seqNr [${env.sequenceNr}], source [${env.source}]")
                push(out, env)
                true
              } else
                false
            case _ =>
              false
          }

        if (isAvailable(out)) {
          mode match {
            case FirehoseOnly =>
              // there can be one final value from catchup when switching to FirehoseOnly
              if (!tryPushCatchupValue(deduplicate = false))
                tryPushFirehoseValue(deduplicate = false)
            case Both(_) =>
              if (!tryPushFirehoseValue(deduplicate = true))
                tryPushCatchupValue(deduplicate = true)
            case CatchUpOnly =>
              tryPushCatchupValue(deduplicate = false)
          }
        }

        if (willShutDown) completeStage()
      }

      def isDuplicate(env: EventEnvelope[Any]): Boolean = {
        if (settings.deduplicationCapacity == 0)
          false
        else {
          val entry = DeduplicationCacheEntry(env.persistenceId, env.sequenceNr, env.source)
          val result = {
            if (deduplicationCache.contains(entry)) {
              true
            } else {
              deduplicationCache.add(entry)
              false
            }
          }

          if (deduplicationCache.size >= deduplicationCacheEvictThreshold) {
            // weird that add modifies the instance but drop returns a new instance
            deduplicationCache = deduplicationCache.drop(deduplicationCache.size - settings.deduplicationCapacity)
          }

          result
        }
      }

      private def tryPullAllIfNeeded(): Unit = {
        if (isClosed(firehoseInlet)) {
          completeStage()
        } else {
          if (!hasBeenPulled(firehoseInlet) && firehoseHandler.value.isEmpty) {
            tryPull(firehoseInlet)
          }
          if (mode != FirehoseOnly && !hasBeenPulled(catchupInlet) && catchupHandler.value.isEmpty) {
            tryPull(catchupInlet)
          }
        }
      }

      def isCaughtUp(env: EventEnvelope[Any]): Boolean = {
        if (env.source == "") {
          val offset = timestampOffset(env)
          firehoseHandler.firehoseOffset.timestamp != Instant.EPOCH &&
          !firehoseHandler.firehoseOffset.timestamp.isAfter(offset.timestamp)
        } else
          false // don't look at pub-sub or backtracking events
      }

      private class FirehoseHandler(in: Inlet[EventEnvelope[Any]]) extends InHandler {
        var value: OptionVal[EventEnvelope[Any]] = OptionVal.None
        var firehoseOffset: TimestampOffset = TimestampOffset(Instant.EPOCH, Map.empty)

        def updateFirehoseOffset(env: EventEnvelope[Any]): Unit = {
          // don't look at pub-sub or backtracking events
          if (env.source == "")
            firehoseOffset = timestampOffset(env)
        }

        override def onPush(): Unit = {
          if (value.isDefined)
            throw new IllegalStateException("FirehoseInlet.onPush but has already value. This is a bug.")

          val env = grab(in)

          mode match {
            case FirehoseOnly =>
              value = OptionVal.Some(env)
            case Both(_) =>
              updateFirehoseOffset(env)
              value = OptionVal.Some(env)
            case CatchUpOnly =>
              updateFirehoseOffset(env)
          }

          tryPushOutput()
          tryPullAllIfNeeded()
        }
      }

      private class CatchupHandler(in: Inlet[EventEnvelope[Any]]) extends InHandler {
        var value: OptionVal[EventEnvelope[Any]] = OptionVal.None

        override def onPush(): Unit = {
          if (value.isDefined)
            throw new IllegalStateException("CatchupInlet.onPush but has already value. This is a bug.")

          val env = grab(in)

          mode match {
            case CatchUpOnly =>
              if (isCaughtUp(env)) {
                val timestamp = timestampOffset(env).timestamp
                log.debug(
                  "Firehose entityType [{}] sliceRange [{}] consumer [{}] caught up at [{}]",
                  entityType,
                  sliceRangeStr,
                  consumerId,
                  timestamp)
                mode = Both(timestamp)
              }
              value = OptionVal.Some(env)

            case Both(caughtUpTimestamp) =>
              // don't look at pub-sub or backtracking events
              if (env.source == "") {
                val timestamp = timestampOffset(env).timestamp
                if (isDurationGreaterThan(caughtUpTimestamp, timestamp, settings.catchupOverlap)) {
                  firehose.updateConsumerFirehoseOnly(consumerId)
                  log.debug(
                    "Firehose entityType [{}] sliceRange [{}] consumer [{}] switching to firehose only [{}]",
                    entityType,
                    sliceRangeStr,
                    consumerId,
                    timestamp)
                  catchupKillSwitch.shutdown()
                  mode = FirehoseOnly
                  deduplicationCache = mutable.LinkedHashSet.empty[DeduplicationCacheEntry]
                }
              }

              value = OptionVal.Some(env)

            case FirehoseOnly =>
            // skip
          }

          tryPushOutput()
          tryPullAllIfNeeded()
        }

        override def onUpstreamFinish(): Unit = {
          // important to override onUpstreamFinish, otherwise it will close everything
          log.debug(
            "Firehose entityType [{}] sliceRange [{}] consumer [{}] catchup closed",
            entityType,
            sliceRangeStr,
            consumerId)
        }
      }

    }

  override def toString = "CatchupOrFirehose"
}
