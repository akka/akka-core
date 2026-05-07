/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import com.codahale.metrics.Metric
import org.{ HdrHistogram => hdr }

/**
 * Adapts Gil Tene's HdrHistogram to Metric's Metric interface.
 *
 * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
 *                              integer that is &gt;= 2.
 * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
 *                                       maintain value resolution and separation. Must be a non-negative
 *                                       integer between 0 and 5.
 */
private[akka] class HdrHistogram(
    highestTrackableValue: Long,
    numberOfSignificantValueDigits: Int,
    val unit: String = "")
    extends Metric {

  private val hist = new hdr.Histogram(highestTrackableValue, numberOfSignificantValueDigits)

  def update(value: Long): Unit = {
    try hist.recordValue(value)
    catch {
      case ex: ArrayIndexOutOfBoundsException => throw wrapHistogramOutOfBoundsException(value, ex)
    }
  }

  def updateWithCount(value: Long, count: Long): Unit = {
    try hist.recordValueWithCount(value, count)
    catch {
      case ex: ArrayIndexOutOfBoundsException => throw wrapHistogramOutOfBoundsException(value, ex)
    }
  }

  private def wrapHistogramOutOfBoundsException(
      value: Long,
      ex: ArrayIndexOutOfBoundsException): IllegalArgumentException =
    new IllegalArgumentException(
      s"Given value $value can not be stored in this histogram " +
      s"(min: ${hist.getLowestDiscernibleValue}, max: ${hist.getHighestTrackableValue}})",
      ex)

  def getData = hist.copy()

}
