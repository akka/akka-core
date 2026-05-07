/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.time.LocalDateTime
import java.util.concurrent.ThreadLocalRandom

import scala.collection.immutable
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import OptimalSizeExploringResizer._
import com.typesafe.config.Config

import akka.actor._
import akka.annotation.InternalApi

trait OptimalSizeExploringResizer extends Resizer {

  /**
   * Report the messageCount as well as current routees so that the
   * it can collect metrics.
   * Caution: this method is not thread safe.
   *
   * @param currentRoutees
   * @param messageCounter
   */
  def reportMessageCount(currentRoutees: immutable.IndexedSeq[Routee], messageCounter: Long): Unit
}

case object OptimalSizeExploringResizer {

  /**
   * INTERNAL API
   */
  private[routing] type PoolSize = Int

  /**
   * INTERNAL API
   */
  private[routing] case class UnderUtilizationStreak(start: LocalDateTime, highestUtilization: Int)

  /**
   * INTERNAL API
   */
  private[routing] case class ResizeRecord(
      underutilizationStreak: Option[UnderUtilizationStreak] = None,
      messageCount: Long = 0,
      totalQueueLength: Int = 0,
      checkTime: Long = 0)

  /**
   * INTERNAL API
   */
  private[routing] type PerformanceLog = Map[PoolSize, Duration]

  def apply(resizerCfg: Config): OptimalSizeExploringResizer =
    DefaultOptimalSizeExploringResizer(
      lowerBound = resizerCfg.getInt("lower-bound"),
      upperBound = resizerCfg.getInt("upper-bound"),
      chanceOfScalingDownWhenFull = resizerCfg.getDouble("chance-of-ramping-down-when-full"),
      actionInterval = resizerCfg.getDuration("action-interval").toScala,
      downsizeAfterUnderutilizedFor = resizerCfg.getDuration("downsize-after-underutilized-for").toScala,
      numOfAdjacentSizesToConsiderDuringOptimization = resizerCfg.getInt("optimization-range"),
      exploreStepSize = resizerCfg.getDouble("explore-step-size"),
      explorationProbability = resizerCfg.getDouble("chance-of-exploration"),
      weightOfLatestMetric = resizerCfg.getDouble("weight-of-latest-metric"),
      downsizeRatio = resizerCfg.getDouble("downsize-ratio"))

}

/**
 * This resizer resizes the pool to an optimal size that provides
 * the most message throughput.
 *
 * This resizer works best when you expect the pool size to
 * performance function to be a convex function.
 *
 * For example, when you have a CPU bound tasks, the optimal
 * size is bound to the number of CPU cores.
 * When your task is IO bound, the optimal size is bound to
 * optimal number of concurrent connections to that IO service -
 * e.g. a 4 node elastic search cluster may handle 4-8
 * concurrent requests at optimal speed.
 *
 * It achieves this by keeping track of message throughput at
 * each pool size and performing the following three
 * resizing operations (one at a time) periodically:
 *
 *   * Downsize if it hasn't seen all routees ever fully
 *     utilized for a period of time.
 *   * Explore to a random nearby pool size to try and
 *     collect throughput metrics.
 *   * Optimize to a nearby pool size with a better (than any other
 *     nearby sizes) throughput metrics.
 *
 * When the pool is fully-utilized (i.e. all routees are busy),
 * it randomly choose between exploring and optimizing.
 * When the pool has not been fully-utilized for a period of time,
 * it will downsize the pool to the last seen max utilization
 * multiplied by a configurable ratio.
 *
 * By constantly exploring and optimizing, the resizer will
 * eventually walk to the optimal size and remain nearby.
 * When the optimal size changes it will start walking towards
 * the new one.
 *
 * It keeps a performance log so it's stateful as well as
 * having a larger memory footprint than the default [[Resizer]].
 * The memory usage is O(n) where n is the number of sizes
 * you allow, i.e. upperBound - lowerBound.
 *
 * For documentation about the parameters, see the reference.conf -
 * akka.actor.deployment.default.optimal-size-exploring-resizer
 *
 */
@SerialVersionUID(1L)
case class DefaultOptimalSizeExploringResizer(
    lowerBound: PoolSize = 1,
    upperBound: PoolSize = 30,
    chanceOfScalingDownWhenFull: Double = 0.2,
    actionInterval: Duration = 5.seconds,
    numOfAdjacentSizesToConsiderDuringOptimization: Int = 16,
    exploreStepSize: Double = 0.1,
    downsizeRatio: Double = 0.8,
    downsizeAfterUnderutilizedFor: Duration = 72.hours,
    explorationProbability: Double = 0.4,
    weightOfLatestMetric: Double = 0.5)
    extends OptimalSizeExploringResizer {

  /**
   * INTERNAL API
   *
   * Leave package accessible for testing purpose
   */
  @InternalApi
  private[routing] var performanceLog: PerformanceLog = Map.empty

  /**
   * INTERNAL API
   *
   * Leave package accessible for testing purpose
   */
  @InternalApi
  private[routing] var record: ResizeRecord = ResizeRecord()

  /**
   * INTERNAL API
   *
   * Leave package accessible for testing purpose
   */
  @InternalApi
  private[routing] var stopExploring = false

  /**
   * INTERNAL API
   *
   * Introduced to avoid changing downsizeAfterUnderutilizedFor to FiniteDuration
   */
  @InternalApi
  private val downsizeAfterUnderutilizedDuration = downsizeAfterUnderutilizedFor.asInstanceOf[FiniteDuration].toJava

  private def random = ThreadLocalRandom.current()

  private def checkParamAsProbability(value: Double, paramName: String): Unit =
    if (value < 0 || value > 1)
      throw new IllegalArgumentException(s"$paramName must be between 0 and 1 (inclusive), was: [%s]".format(value))

  private def checkParamAsPositiveNum(value: Double, paramName: String): Unit =
    checkParamLowerBound(value, 0, paramName)

  private def checkParamLowerBound(value: Double, lowerBound: Double, paramName: String): Unit =
    if (value < lowerBound)
      throw new IllegalArgumentException(s"$paramName must be >= $lowerBound, was: [%s]".format(value))

  checkParamAsPositiveNum(lowerBound, "lowerBound")
  checkParamAsPositiveNum(upperBound, "upperBound")
  if (upperBound < lowerBound)
    throw new IllegalArgumentException(
      "upperBound must be >= lowerBound, was: [%s] < [%s]".format(upperBound, lowerBound))

  checkParamLowerBound(
    numOfAdjacentSizesToConsiderDuringOptimization,
    2,
    "numOfAdjacentSizesToConsiderDuringOptimization")
  checkParamAsProbability(chanceOfScalingDownWhenFull, "chanceOfScalingDownWhenFull")
  checkParamAsPositiveNum(
    numOfAdjacentSizesToConsiderDuringOptimization,
    "numOfAdjacentSizesToConsiderDuringOptimization")
  checkParamAsPositiveNum(exploreStepSize, "exploreStepSize")
  checkParamAsPositiveNum(downsizeRatio, "downsizeRatio")
  checkParamAsProbability(explorationProbability, "explorationProbability")
  checkParamAsProbability(weightOfLatestMetric, "weightOfLatestMetric")

  private val actionInternalNanos = actionInterval.toNanos

  def isTimeForResize(messageCounter: Long): Boolean = {
    System.nanoTime() > record.checkTime + actionInternalNanos
  }

  def reportMessageCount(currentRoutees: immutable.IndexedSeq[Routee], messageCounter: Long): Unit = {
    val (newPerfLog, newRecord) = updatedStats(currentRoutees, messageCounter)

    performanceLog = newPerfLog
    record = newRecord
  }

  private[routing] def updatedStats(
      currentRoutees: immutable.IndexedSeq[Routee],
      messageCounter: Long): (PerformanceLog, ResizeRecord) = {
    val now = LocalDateTime.now
    val currentSize = currentRoutees.length

    val messagesInRoutees = currentRoutees.map {
      case ActorRefRoutee(a: ActorRefWithCell) =>
        a.underlying match {
          case cell: ActorCell =>
            cell.mailbox.numberOfMessages + (if (cell.currentMessage != null) 1 else 0)
          case cell => cell.numberOfMessages
        }
      case _ => 0
    }

    val totalQueueLength = messagesInRoutees.sum
    val utilized = messagesInRoutees.count(_ > 0)

    val fullyUtilized = utilized == currentSize

    val newUnderutilizationStreak =
      if (fullyUtilized)
        None
      else
        Some(
          UnderUtilizationStreak(
            record.underutilizationStreak.fold(now)(_.start),
            Math.max(record.underutilizationStreak.fold(0)(_.highestUtilization), utilized)))

    val newPerformanceLog: PerformanceLog =
      if (fullyUtilized && record.underutilizationStreak.isEmpty && record.checkTime > 0) {
        val totalMessageReceived = messageCounter - record.messageCount
        val queueSizeChange = record.totalQueueLength - totalQueueLength
        val totalProcessed = queueSizeChange + totalMessageReceived
        if (totalProcessed > 0) {
          val duration = Duration.fromNanos(System.nanoTime() - record.checkTime)
          val last: Duration = duration / totalProcessed
          //exponentially decrease the weight of old last metrics data
          val toUpdate = performanceLog.get(currentSize).fold(last) { oldSpeed =>
            (oldSpeed * (1.0 - weightOfLatestMetric)) + (last * weightOfLatestMetric)
          }
          performanceLog + (currentSize -> toUpdate)
        } else performanceLog
      } else performanceLog

    val newRecord = record.copy(
      underutilizationStreak = newUnderutilizationStreak,
      messageCount = messageCounter,
      totalQueueLength = totalQueueLength,
      checkTime = System.nanoTime())

    (newPerformanceLog, newRecord)

  }

  def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int = {
    val currentSize = currentRoutees.length
    val now = LocalDateTime.now
    val proposedChange =
      if (record.underutilizationStreak.fold(false)(_.start.isBefore(now.minus(downsizeAfterUnderutilizedDuration)))) {
        val downsizeTo = (record.underutilizationStreak.get.highestUtilization * downsizeRatio).toInt
        Math.min(downsizeTo - currentSize, 0)
      } else if (performanceLog.isEmpty || record.underutilizationStreak.isDefined) {
        0
      } else {
        if (!stopExploring && random.nextDouble() < explorationProbability)
          explore(currentSize)
        else
          optimize(currentSize)
      }
    Math.max(lowerBound, Math.min(proposedChange + currentSize, upperBound)) - currentSize
  }

  private def optimize(currentSize: PoolSize): Int = {

    val adjacentDispatchWaits: Map[PoolSize, Duration] = {
      def adjacency = (size: Int) => Math.abs(currentSize - size)
      val sizes = performanceLog.keys.toSeq
      val numOfSizesEachSide = numOfAdjacentSizesToConsiderDuringOptimization / 2
      val leftBoundary =
        sizes.filter(_ < currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      val rightBoundary =
        sizes.filter(_ >= currentSize).sortBy(adjacency).take(numOfSizesEachSide).lastOption.getOrElse(currentSize)
      performanceLog.filter { case (size, _) => size >= leftBoundary && size <= rightBoundary }
    }

    val optimalSize = adjacentDispatchWaits.minBy(_._2)._1
    val movement = (optimalSize - currentSize) / 2.0
    if (movement < 0)
      Math.floor(movement).toInt
    else
      Math.ceil(movement).toInt

  }

  private def explore(currentSize: PoolSize): Int = {
    val change = Math.max(1, random.nextInt(Math.ceil(currentSize * exploreStepSize).toInt))
    if (random.nextDouble() < chanceOfScalingDownWhenFull)
      -change
    else
      change
  }

}
