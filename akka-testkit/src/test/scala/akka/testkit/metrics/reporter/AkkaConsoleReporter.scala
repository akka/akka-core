/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics.reporter

import java.io.PrintStream
import java.util
import java.util.concurrent.TimeUnit

import com.codahale.metrics._

import akka.testkit.metrics._

/**
 * Used to report `akka.testkit.metric.Metric` types that the original `com.codahale.metrics.ConsoleReporter` is unaware of (cannot re-use directly because of private constructor).
 */
class AkkaConsoleReporter(registry: AkkaMetricRegistry, verbose: Boolean, output: PrintStream = System.out)
    extends ScheduledReporter(
      registry.asInstanceOf[MetricRegistry],
      "akka-console-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.NANOSECONDS) {

  private final val ConsoleWidth = 80

  override def report(
      gauges: util.SortedMap[String, Gauge[_]],
      counters: util.SortedMap[String, Counter],
      histograms: util.SortedMap[String, Histogram],
      meters: util.SortedMap[String, Meter],
      timers: util.SortedMap[String, Timer]): Unit = {
    import scala.jdk.CollectionConverters._

    // default Metrics types
    printMetrics(gauges.asScala, printGauge)
    printMetrics(counters.asScala, printCounter)
    printMetrics(histograms.asScala, printHistogram)
    printMetrics(meters.asScala, printMeter)
    printMetrics(timers.asScala, printTimer)

    // custom Akka types
    printMetrics(registry.getKnownOpsInTimespanCounters, printKnownOpsInTimespanCounter)
    printMetrics(registry.getHdrHistograms, printHdrHistogram)
    printMetrics(registry.getAveragingGauges, printAveragingGauge)

    output.println()
    output.flush()
  }

  def printMetrics[T <: Metric](metrics: Iterable[(String, T)], printer: T => Unit): Unit = {
    if (metrics.nonEmpty) {
      printWithBanner(s"-- ${simpleName(metrics.head._2.getClass)}", '-')
      for ((key, metric) <- metrics) {
        output.println("  " + key)
        printer(metric)
      }
      output.println()
    }
  }

  private def printMeter(meter: Meter): Unit = {
    output.print("             count = %d%n".format(meter.getCount))
    output.print("         mean rate = %2.2f events/%s%n".format(convertRate(meter.getMeanRate), getRateUnit))
    output.print("     1-minute rate = %2.2f events/%s%n".format(convertRate(meter.getOneMinuteRate), getRateUnit))
    output.print("     5-minute rate = %2.2f events/%s%n".format(convertRate(meter.getFiveMinuteRate), getRateUnit))
    output.print("    15-minute rate = %2.2f events/%s%n".format(convertRate(meter.getFifteenMinuteRate), getRateUnit))
  }

  private def printCounter(entry: Counter): Unit = {
    output.print("             count = %d%n".format(entry.getCount))
  }

  private def printGauge(entry: Gauge[_]): Unit = {
    output.print("             value = %s%n".format(entry.getValue))
  }

  private def printHistogram(histogram: Histogram): Unit = {
    val snapshot = histogram.getSnapshot
    output.print("             count = %d%n".format(histogram.getCount))
    output.print("               min = %d%n".format(snapshot.getMin))
    output.print("               max = %d%n".format(snapshot.getMax))
    output.print("              mean = %2.2f%n".format(snapshot.getMean))
    output.print("            stddev = %2.2f%n".format(snapshot.getStdDev))
    output.print("            median = %2.2f%n".format(snapshot.getMedian))
    output.print("              75%% <= %2.2f%n".format(snapshot.get75thPercentile))
    output.print("              95%% <= %2.2f%n".format(snapshot.get95thPercentile))
    output.print("              98%% <= %2.2f%n".format(snapshot.get98thPercentile))
    output.print("              99%% <= %2.2f%n".format(snapshot.get99thPercentile))
    output.print("            99.9%% <= %2.2f%n".format(snapshot.get999thPercentile))
  }

  private def printTimer(timer: Timer): Unit = {
    val snapshot = timer.getSnapshot
    output.print("             count = %d%n".format(timer.getCount))
    output.print("         mean rate = %2.2f calls/%s%n".format(convertRate(timer.getMeanRate), getRateUnit))
    output.print("     1-minute rate = %2.2f calls/%s%n".format(convertRate(timer.getOneMinuteRate), getRateUnit))
    output.print("     5-minute rate = %2.2f calls/%s%n".format(convertRate(timer.getFiveMinuteRate), getRateUnit))
    output.print("    15-minute rate = %2.2f calls/%s%n".format(convertRate(timer.getFifteenMinuteRate), getRateUnit))
    output.print("               min = %2.2f %s%n".format(convertDuration(snapshot.getMin.toDouble), getDurationUnit))
    output.print("               max = %2.2f %s%n".format(convertDuration(snapshot.getMax.toDouble), getDurationUnit))
    output.print("              mean = %2.2f %s%n".format(convertDuration(snapshot.getMean), getDurationUnit))
    output.print("            stddev = %2.2f %s%n".format(convertDuration(snapshot.getStdDev), getDurationUnit))
    output.print("            median = %2.2f %s%n".format(convertDuration(snapshot.getMedian), getDurationUnit))
    output.print(
      "              75%% <= %2.2f %s%n".format(convertDuration(snapshot.get75thPercentile), getDurationUnit))
    output.print(
      "              95%% <= %2.2f %s%n".format(convertDuration(snapshot.get95thPercentile), getDurationUnit))
    output.print(
      "              98%% <= %2.2f %s%n".format(convertDuration(snapshot.get98thPercentile), getDurationUnit))
    output.print(
      "              99%% <= %2.2f %s%n".format(convertDuration(snapshot.get99thPercentile), getDurationUnit))
    output.print(
      "            99.9%% <= %2.2f %s%n".format(convertDuration(snapshot.get999thPercentile), getDurationUnit))
  }

  private def printKnownOpsInTimespanCounter(counter: KnownOpsInTimespanTimer): Unit = {
    import concurrent.duration._

    import akka.util.PrettyDuration._
    output.print("               ops = %d%n".format(counter.getCount()))
    output.print("              time = %s%n".format(counter.elapsedTime.nanos.pretty))
    output.print("             ops/s = %2.2f%n".format(counter.opsPerSecond))
    output.print("               avg = %s%n".format(counter.avgDuration.nanos.pretty))
  }

  private def printHdrHistogram(hist: HdrHistogram): Unit = {
    val data = hist.getData
    val unit = hist.unit
    output.print("               min = %d %s%n".format(data.getMinValue, unit))
    output.print("               max = %d %s%n".format(data.getMaxValue, unit))
    output.print("              mean = %2.2f %s%n".format(data.getMean, unit))
    output.print("            stddev = %2.2f%n".format(data.getStdDeviation))
    output.print("              75%% <= %d %s%n".format(data.getValueAtPercentile(75.0), unit))
    output.print("              95%% <= %d %s%n".format(data.getValueAtPercentile(95.0), unit))
    output.print("              98%% <= %d %s%n".format(data.getValueAtPercentile(98.0), unit))
    output.print("              99%% <= %d %s%n".format(data.getValueAtPercentile(99.0), unit))
    output.print("            99.9%% <= %d %s%n".format(data.getValueAtPercentile(99.9), unit))

    if (verbose)
      data.outputPercentileDistribution(output, 1)
  }

  private def printAveragingGauge(gauge: AveragingGauge): Unit = {
    output.print("                avg = %2.2f%n".format(gauge.getValue()))
  }

  private def printWithBanner(s: String, c: Char): Unit = {
    output.print(s)
    output.print(' ')
    var i: Int = 0
    while (i < (ConsoleWidth - s.length - 1)) {
      output.print(c)
      i += 1
    }
    output.println()
  }

  /** Required for getting simple names of refined instances */
  private def simpleName(clazz: Class[_]): String = {
    val n = clazz.getName
    val i = n.lastIndexOf('.')
    n.substring(i + 1)
  }

}
