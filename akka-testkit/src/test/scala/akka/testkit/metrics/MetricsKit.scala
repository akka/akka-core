/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit.metrics

import java.util
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.matching.Regex

import com.codahale.metrics._
import com.typesafe.config.Config
import org.scalatest.Notifying

import akka.testkit.metrics.reporter.AkkaConsoleReporter

/**
 * Allows to easily measure performance / memory / file descriptor use in tests.
 *
 * WARNING: This trait should not be seen as utility for micro-benchmarking,
 * please refer to <a href="https://openjdk.java.net/projects/code-tools/jmh/">JMH</a> if that's what you're writing.
 * This trait instead aims to give an high level overview as well as data for trend-analysis of long running tests.
 *
 * Reporting defaults to `ConsoleReporter`.
 */
private[akka] trait MetricsKit extends MetricsKitOps {
  this: Notifying =>

  import MetricsKit._

  import scala.jdk.CollectionConverters._

  private var reporters: List[ScheduledReporter] = Nil

  /**
   * A configuration containing [[MetricsKitSettings]] under the key `akka.test.registry` must be provided.
   * This can be the ActorSystems config.
   *
   * The reason this is not handled by an Extension is thatwe do not want to enforce having to start an ActorSystem,
   * since code measured using this Kit may not need one (e.g. measuring plain Queue implementations).
   */
  def metricsConfig: Config

  private[metrics] val registry = new MetricRegistry() with AkkaMetricRegistry

  initMetricReporters()

  def initMetricReporters(): Unit = {
    val settings = new MetricsKitSettings(metricsConfig)

    def configureConsoleReporter(): Unit = {
      if (settings.Reporters.contains("console")) {
        val akkaConsoleReporter = new AkkaConsoleReporter(registry, settings.ConsoleReporter.Verbose)

        if (settings.ConsoleReporter.ScheduledReportInterval > Duration.Zero)
          akkaConsoleReporter.start(settings.ConsoleReporter.ScheduledReportInterval.toMillis, TimeUnit.MILLISECONDS)

        reporters ::= akkaConsoleReporter
      }
    }

    configureConsoleReporter()
  }

  /**
   * Schedule metric reports execution iterval. Should not be used multiple times
   */
  def scheduleMetricReports(every: FiniteDuration): Unit = {
    reporters.foreach { _.start(every.toMillis, TimeUnit.MILLISECONDS) }
  }

  def registeredMetrics = registry.getMetrics.asScala

  /**
   * Causes immediate flush of metrics, using all registered reporters.
   * Afterwards all metrics are removed from the registry.
   *
   * HINT: this operation can be costy, run outside of your tested code, or rely on scheduled reporting.
   */
  def reportAndClearMetrics(): Unit = {
    reportMetrics()
    clearMetrics()
  }

  def reportMetricsEnabled: Boolean = true

  /**
   * Causes immediate flush of metrics, using all registered reporters.
   *
   * HINT: this operation can be costy, run outside of your tested code, or rely on scheduled reporting.
   */
  def reportMetrics(): Unit = {
    if (reportMetricsEnabled)
      reporters.foreach { _.report() }
  }

  /**
   * Causes immediate flush of only memory related metrics, using all registered reporters.
   *
   * HINT: this operation can be costy, run outside of your tested code, or rely on scheduled reporting.
   */
  def reportMemoryMetrics(): Unit = {
    val gauges = registry.getGauges(MemMetricsFilter)

    reporters.foreach { _.report(gauges, empty, empty, empty, empty) }
  }

  /**
   * Causes immediate flush of only memory related metrics, using all registered reporters.
   *
   * HINT: this operation can be costy, run outside of your tested code, or rely on scheduled reporting.
   */
  def reportGcMetrics(): Unit = {
    val gauges = registry.getGauges(GcMetricsFilter)

    reporters.foreach { _.report(gauges, empty, empty, empty, empty) }
  }

  /**
   * Causes immediate flush of only file descriptor metrics, using all registered reporters.
   *
   * HINT: this operation can be costy, run outside of your tested code, or rely on scheduled reporting.
   */
  def reportFileDescriptorMetrics(): Unit = {
    val gauges = registry.getGauges(FileDescriptorMetricsFilter)

    reporters.foreach { _.report(gauges, empty, empty, empty, empty) }
  }

  /**
   * Removes registered registry from registry.
   * You should call this method then you're done measuring something - usually at the end of your test case,
   * otherwise the registry from different tests would influence each others results (avg, min, max, ...).
   *
   * Please note that, if you have registered a `timer("thing")` previously, you will need to call `timer("thing")` again,
   * in order to register a new timer.
   */
  def clearMetrics(matching: MetricFilter = MetricFilter.ALL): Unit = {
    registry.removeMatching(matching)
  }

  /**
   * MUST be called after all tests have finished.
   */
  def shutdownMetrics(): Unit = {
    reporters.foreach { _.stop() }
  }

  private[metrics] def getOrRegister[M <: Metric](key: String, metric: => M)(implicit tag: ClassTag[M]): M = {
    import scala.jdk.CollectionConverters._
    registry.getMetrics.asScala.find(_._1 == key).map(_._2) match {
      case Some(existing: M) => existing
      case Some(_) =>
        throw new IllegalArgumentException(
          "Key: [%s] is already for different kind of metric! Was [%s], expected [%s]"
            .format(key, metric.getClass.getSimpleName, tag.runtimeClass.getSimpleName))
      case _ => registry.register(key, metric)
    }
  }

  private val emptySortedMap = new util.TreeMap[String, Nothing]()
  private def empty[T] = emptySortedMap.asInstanceOf[util.TreeMap[String, T]]
}

private[akka] object MetricsKit {

  class RegexMetricFilter(regex: Regex) extends MetricFilter {
    override def matches(name: String, metric: Metric) = regex.pattern.matcher(name).matches()
  }

  val MemMetricsFilter = new RegexMetricFilter(""".*\.mem\..*""".r)

  val FileDescriptorMetricsFilter = new RegexMetricFilter(""".*\.file-descriptors\..*""".r)

  val KnownOpsInTimespanCounterFilter = new MetricFilter {
    override def matches(name: String, metric: Metric) = classOf[KnownOpsInTimespanTimer].isInstance(metric)
  }

  val GcMetricsFilter: MetricFilter = new MetricFilter {
    val keyPattern = """.*\.gc\..*""".r.pattern

    override def matches(name: String, metric: Metric) = keyPattern.matcher(name).matches()
  }
}

/** Provides access to custom Akka `com.codahale.metrics.Metric`, with named methods. */
trait AkkaMetricRegistry {
  this: MetricRegistry =>

  def getKnownOpsInTimespanCounters = filterFor(classOf[KnownOpsInTimespanTimer])
  def getHdrHistograms = filterFor(classOf[HdrHistogram])
  def getAveragingGauges = filterFor(classOf[AveragingGauge])

  import scala.jdk.CollectionConverters._
  private def filterFor[T](clazz: Class[T]): mutable.Iterable[(String, T)] =
    for {
      (key, metric) <- getMetrics.asScala
      if clazz.isInstance(metric)
    } yield key -> metric.asInstanceOf[T]
}

private[akka] class MetricsKitSettings(config: Config) {

  import akka.util.Helpers._

  val Reporters = config.getStringList("akka.test.metrics.reporters")

  object ConsoleReporter {
    val ScheduledReportInterval =
      config.getMillisDuration("akka.test.metrics.reporter.console.scheduled-report-interval")
    val Verbose = config.getBoolean("akka.test.metrics.reporter.console.verbose")
  }

}
