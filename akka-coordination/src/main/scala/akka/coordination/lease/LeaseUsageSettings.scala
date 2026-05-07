/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.util.PrettyDuration._
import com.typesafe.config.Config

/**
 * Note that if you define a custom lease name and have several Cluster Singletons or Cluster Sharding
 * entity types each one must have a unique lease name. If the lease name is undefined it will be derived
 * from ActorSystem name and other component names, but that may result in too long lease names.
 */
object LeaseUsageSettings {

  /**
   * Scala API: Load the settings from the given lease config block
   */
  def apply(config: Config): LeaseUsageSettings =
    new LeaseUsageSettings(
      config.getString("use-lease"),
      config.getDuration("lease-retry-interval").toScala,
      leaseName = config.getString("lease-name"))

  /**
   * Java API: Load the settings from the given lease config block
   */
  def create(config: Config): LeaseUsageSettings =
    apply(config)

  /**
   * Scala API: Create lease settings with undefined lease name to trigger auto generated names when used with
   *           for example singleton or sharding
   */
  def apply(leaseImplementation: String, leaseRetryInterval: FiniteDuration): LeaseUsageSettings =
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval)

  /**
   * Scala API:
   *
   * @param leaseName Note that if you have several Cluster Singletons or Cluster Sharding
   *                  entity types using lease each one must have a unique lease name
   */
  def apply(leaseImplementation: String, leaseRetryInterval: FiniteDuration, leaseName: String): LeaseUsageSettings =
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval, leaseName)

  /**
   * Java API: Create lease settings with undefined lease name to trigger auto generated names when used with
   *           for example singleton or sharding
   */
  def create(leaseImplementation: String, leaseRetryInterval: java.time.Duration): LeaseUsageSettings =
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval.toScala)

  /**
   * Java API:
   * @param leaseName Note that if you have several Cluster Singletons or Cluster Sharding
   *                  entity types using lease each one must have a unique lease name
   */
  def create(
      leaseImplementation: String,
      leaseRetryInterval: java.time.Duration,
      leaseName: String): LeaseUsageSettings =
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval.toScala, leaseName)
}

final class LeaseUsageSettings private[akka] (
    val leaseImplementation: String,
    val leaseRetryInterval: FiniteDuration,
    val leaseName: String) {
  def this(leaseImplementation: String, leaseRetryInterval: FiniteDuration) =
    this(leaseImplementation, leaseRetryInterval, leaseName = "")

  def getLeaseRetryInterval(): java.time.Duration = leaseRetryInterval.toJava

  /**
   * Note that if you have several Cluster Singletons or Cluster Sharding entity types using lease each one must have
   * a unique lease name
   */
  def withLeaseName(name: String): LeaseUsageSettings =
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval, name)

  /**
   * Scala API:
   */
  def withLeaseRetryInterval(leaseRetryInterval: FiniteDuration): LeaseUsageSettings = {
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval, leaseName)
  }

  /**
   * Java API:
   */
  def withLeaseRetryInterval(leaseRetryInterval: java.time.Duration): LeaseUsageSettings = {
    new LeaseUsageSettings(leaseImplementation, leaseRetryInterval.toScala, leaseName)
  }

  override def toString = s"LeaseUsageSettings($leaseImplementation, ${leaseRetryInterval.pretty})"
}
