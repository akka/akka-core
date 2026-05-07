/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding.typed

import java.time.{ Duration => JDuration }

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi

object ShardedDaemonProcessSettings {

  /** Scala API: Create default settings for system */
  def apply(system: ActorSystem[_]): ShardedDaemonProcessSettings = {
    fromConfig(system.settings.config.getConfig("akka.cluster.sharded-daemon-process"))
  }

  /** Java API: Create default settings for system */
  def create(system: ActorSystem[_]): ShardedDaemonProcessSettings =
    apply(system)

  /**
   * Load settings from a specific config location.
   */
  def fromConfig(config: Config): ShardedDaemonProcessSettings = {
    val keepAliveInterval = config.getDuration("keep-alive-interval").toScala
    val keepAliveFromNumberOfNodes = config.getInt("keep-alive-from-number-of-nodes")
    val keepAliveThrottleInterval = config.getDuration("keep-alive-throttle-interval").toScala
    new ShardedDaemonProcessSettings(
      keepAliveInterval,
      None,
      None,
      keepAliveFromNumberOfNodes,
      keepAliveThrottleInterval)
  }

}

/**
 * Not for user constructions, use factory methods to instantiate.
 */
final class ShardedDaemonProcessSettings @InternalApi private[akka] (
    val keepAliveInterval: FiniteDuration,
    val shardingSettings: Option[ClusterShardingSettings],
    val role: Option[String],
    val keepAliveFromNumberOfNodes: Int,
    val keepAliveThrottleInterval: FiniteDuration) {

  /**
   * Scala API: The interval each parent of the sharded set is pinged from each node in the cluster.
   *
   * Note: How the sharded set is kept alive may change in the future meaning this setting may go away.
   */
  def withKeepAliveInterval(keepAliveInterval: FiniteDuration): ShardedDaemonProcessSettings =
    copy(keepAliveInterval = keepAliveInterval)

  /**
   * Java API: The interval each parent of the sharded set is pinged from each node in the cluster.
   *
   * Note: How the sharded set is kept alive may change in the future meaning this setting may go away.
   */
  def withKeepAliveInterval(keepAliveInterval: JDuration): ShardedDaemonProcessSettings =
    copy(keepAliveInterval = keepAliveInterval.toScala)

  /**
   * Specify sharding settings that should be used for the sharded daemon process instead of loading from config.
   * Some settings can not be changed (remember-entitites and related settings, passivation, number-of-shards),
   * changing those settings will be ignored.
   */
  def withShardingSettings(shardingSettings: ClusterShardingSettings): ShardedDaemonProcessSettings =
    copy(shardingSettings = Option(shardingSettings))

  /**
   * Specifies that the ShardedDaemonProcess should run on nodes with a specific role.
   * If the role is not specified all nodes in the cluster are used. If the given role does
   * not match the role of the current node the the ShardedDaemonProcess will not be started.
   */
  def withRole(role: String): ShardedDaemonProcessSettings =
    copy(role = Option(role))

  /**
   * Keep alive messages from this number of nodes.
   */
  def withKeepAliveFromNumberOfNodes(keepAliveFromNumberOfNodes: Int): ShardedDaemonProcessSettings =
    copy(keepAliveFromNumberOfNodes = keepAliveFromNumberOfNodes)

  /**
   * Scala API: Keep alive messages are sent with this delay between each message.
   */
  def withKeepAliveThrottleInterval(keepAliveThrottleInterval: FiniteDuration): ShardedDaemonProcessSettings =
    copy(keepAliveThrottleInterval = keepAliveThrottleInterval)

  /**
   * Java API: Keep alive messages are sent with this delay between each message.
   */
  def withKeepAliveThrottleInterval(keepAliveThrottleInterval: JDuration): ShardedDaemonProcessSettings =
    copy(keepAliveThrottleInterval = keepAliveThrottleInterval.toScala)

  private def copy(
      keepAliveInterval: FiniteDuration = keepAliveInterval,
      shardingSettings: Option[ClusterShardingSettings] = shardingSettings,
      role: Option[String] = role,
      keepAliveFromNumberOfNodes: Int = keepAliveFromNumberOfNodes,
      keepAliveThrottleInterval: FiniteDuration = keepAliveThrottleInterval): ShardedDaemonProcessSettings =
    new ShardedDaemonProcessSettings(
      keepAliveInterval,
      shardingSettings,
      role,
      keepAliveFromNumberOfNodes,
      keepAliveThrottleInterval)

}

/**
 * Context with details about the Sharded Daemon Process instance to use when starting it
 *
 * Not for user extension
 */
@DoNotInherit
@ApiMayChange
trait ShardedDaemonProcessContext {
  def processNumber: Int
  def totalProcesses: Int

  /**
   * The revision starts at 0 and each time the number of processes is changed, the revision increases with 1
   */
  def revision: Long

  def name: String

}
