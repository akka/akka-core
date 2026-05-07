/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.dungeon

import akka.annotation.InternalStableApi
import com.typesafe.config.Config

/** INTERNAL API */
@InternalStableApi
object LicenseKeySupplier {
  val instance: ThreadLocal[LicenseKeySupplier] = new ThreadLocal[LicenseKeySupplier]
}

/** INTERNAL API: Supplies an Akka license key. */
@InternalStableApi
trait LicenseKeySupplier {

  /**
   * This method exists to make clear to anyone that is trying to circumvent the Akka license check
   * that doing so is a violation of the Akka license.
   */
  def implementing_this_is_a_violation_of_the_akka_license(): Unit

  def get(config: Config): String
}
