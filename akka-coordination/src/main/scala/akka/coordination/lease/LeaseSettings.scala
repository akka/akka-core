/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease

import com.typesafe.config.Config

object LeaseSettings {
  def apply(config: Config, leaseName: String, ownerName: String): LeaseSettings = {
    new LeaseSettings(leaseName, ownerName, TimeoutSettings(config), config)
  }
}

final class LeaseSettings(
    val leaseName: String,
    val ownerName: String,
    val timeoutSettings: TimeoutSettings,
    val leaseConfig: Config) {

  def withTimeoutSettings(timeoutSettings: TimeoutSettings): LeaseSettings =
    new LeaseSettings(leaseName, ownerName, timeoutSettings, leaseConfig)

  override def toString = s"LeaseSettings($leaseName, $ownerName, $timeoutSettings)"
}
