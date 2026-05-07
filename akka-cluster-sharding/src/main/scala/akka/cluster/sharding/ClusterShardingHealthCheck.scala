/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sharding

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.Cluster
import akka.cluster.MemberStatus
import akka.event.Logging
import akka.pattern.AskTimeoutException
import akka.pattern.ask
import akka.util.Timeout
import scala.jdk.CollectionConverters._

/**
 * Internal API
 */
@InternalApi
private[akka] object ClusterShardingHealthCheckSettings {
  def apply(config: Config): ClusterShardingHealthCheckSettings =
    new ClusterShardingHealthCheckSettings(
      config.getStringList("names").asScala.toSet,
      config.getDuration("timeout").toScala,
      config.getDuration("disabled-after").toScala)
}

final class ClusterShardingHealthCheckSettings(
    val names: Set[String],
    val timeout: FiniteDuration,
    val disableAfter: FiniteDuration) {

  // for binary backwards compatibility
  @deprecated("Use full constructor", "2.8.0")
  def this(names: Set[String], timeout: FiniteDuration) = this(names, timeout, 10.seconds)
}

private object ClusterShardingHealthCheck {
  val Success = Future.successful(true)
}

/**
 * INTERNAL API (ctr)
 */
final class ClusterShardingHealthCheck private[akka] (
    system: ActorSystem,
    settings: ClusterShardingHealthCheckSettings,
    shardRegion: String => ActorRef)
    extends (() => Future[Boolean]) {

  private val log = Logging(system, classOf[ClusterShardingHealthCheck])

  def this(system: ActorSystem) =
    this(
      system,
      ClusterShardingHealthCheckSettings(system.settings.config.getConfig("akka.cluster.sharding.healthcheck")),
      name => ClusterSharding(system).shardRegion(name))

  private implicit val timeout: Timeout = settings.timeout
  private implicit val ec: ExecutionContext = system.dispatchers.internalDispatcher

  // Once the check has passed it always does
  @volatile private var registered = false
  @volatile private var startedTimestamp = 0L

  private def isMemberUp(): Boolean = {
    val memberStatus = Cluster(system).selfMember.status
    memberStatus != MemberStatus.Joining && memberStatus != MemberStatus.Removed
  }

  override def apply(): Future[Boolean] = {
    if (settings.names.isEmpty || registered) {
      ClusterShardingHealthCheck.Success
    } else if (startedTimestamp != 0L && System
                 .currentTimeMillis() > startedTimestamp + settings.disableAfter.toMillis) {
      ClusterShardingHealthCheck.Success
    } else {
      if (startedTimestamp == 0 && isMemberUp())
        startedTimestamp = System.currentTimeMillis()

      Future
        .traverse(settings.names) { name =>
          shardRegion(name) // this can throw if shard region not registered and it'll fail the check
            .ask(ShardRegion.GetShardRegionStatus)
            .mapTo[ShardRegion.ShardRegionStatus]
        }
        .map { allResponses =>
          val allRegistered = allResponses.forall(_.registeredWithCoordinator)
          if (!allRegistered && log.isInfoEnabled) {
            log.info(
              "Not all shard regions have registered with coordinator. Still to register: [{}]",
              allResponses
                .collect {
                  case response if !response.registeredWithCoordinator => response.typeName
                }
                .mkString(","))
          }
          if (allRegistered) {
            registered = true
          }
          allRegistered
        }
        .recover {
          case _: AskTimeoutException =>
            if (log.isDebugEnabled) {
              log.debug(
                "Shard regions [{}] did not respond in time. Failing health check.",
                settings.names.mkString(","))
            }
            false
        }
    }
  }
}
