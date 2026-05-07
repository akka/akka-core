/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import scala.concurrent.duration._

import com.typesafe.config.Config

import akka.actor.Props
import akka.annotation.InternalApi
import akka.remote.artery.ArterySettings
import akka.util.Helpers.ConfigOps
import akka.util.Helpers.Requiring

final class RemoteSettings(val config: Config) {
  import config._

  val Artery = ArterySettings(getConfig("akka.remote.artery"))

  val WarnAboutDirectUse: Boolean = getBoolean("akka.remote.warn-about-direct-use")

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def untrustedMode: Boolean = Artery.UntrustedMode

  def configureDispatcher(props: Props): Props =
    if (Artery.Advanced.Dispatcher.isEmpty) props else props.withDispatcher(Artery.Advanced.Dispatcher)

  val UseUnsafeRemoteFeaturesWithoutCluster: Boolean = getBoolean(
    "akka.remote.use-unsafe-remote-features-outside-cluster")

  val WarnUnsafeWatchWithoutCluster: Boolean = getBoolean("akka.remote.warn-unsafe-watch-outside-cluster")

  val WatchFailureDetectorConfig: Config = getConfig("akka.remote.watch-failure-detector")
  val WatchFailureDetectorImplementationClass: String = WatchFailureDetectorConfig.getString("implementation-class")
  val WatchHeartBeatInterval: FiniteDuration = {
    WatchFailureDetectorConfig.getMillisDuration("heartbeat-interval")
  }.requiring(_ > Duration.Zero, "watch-failure-detector.heartbeat-interval must be > 0")
  val WatchUnreachableReaperInterval: FiniteDuration = {
    WatchFailureDetectorConfig.getMillisDuration("unreachable-nodes-reaper-interval")
  }.requiring(_ > Duration.Zero, "watch-failure-detector.unreachable-nodes-reaper-interval must be > 0")
  val WatchHeartbeatExpectedResponseAfter: FiniteDuration = {
    WatchFailureDetectorConfig.getMillisDuration("expected-response-after")
  }.requiring(_ > Duration.Zero, "watch-failure-detector.expected-response-after > 0")

}
