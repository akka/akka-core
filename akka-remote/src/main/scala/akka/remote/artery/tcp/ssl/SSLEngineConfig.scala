/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

import com.typesafe.config.Config

import akka.annotation.InternalApi
import akka.japi.Util.immutableSeq

/**
 * INTERNAL API
 */
@InternalApi
private[tcp] class SSLEngineConfig(config: Config) {
  private[tcp] val SSLRandomNumberGenerator: String = config.getString("random-number-generator")

  private[tcp] val SSLProtocol: String = config.getString("protocol")
  private[tcp] val SSLEnabledAlgorithms: Set[String] =
    immutableSeq(config.getStringList("enabled-algorithms")).toSet
  private[tcp] val SSLRequireMutualAuthentication: Boolean = {
    if (config.hasPath("require-mutual-authentication"))
      config.getBoolean("require-mutual-authentication")
    else
      false
  }
  private[tcp] val HostnameVerification: Boolean = {
    if (config.hasPath("hostname-verification"))
      config.getBoolean("hostname-verification")
    else
      false
  }
  private[tcp] val SSLContextCacheTime: FiniteDuration =
    if (config.hasPath("ssl-context-cache-ttl"))
      config.getDuration("ssl-context-cache-ttl").toMillis.millis
    else
      1024.days // a lot of days (not Inf, because `Inf` is not a FiniteDuration
}
