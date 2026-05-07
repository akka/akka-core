/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.concurrent.duration.Duration

import akka.actor.{ Actor, ActorLogging, Deploy, Props }
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.routing.FromConfig

final class SimpleDnsManager(val ext: DnsExt)
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics]
    with ActorLogging {

  import context._

  private val resolver = actorOf(
    FromConfig.props(
      Props(ext.provider.actorClass, ext.cache, ext.Settings.ResolverConfig)
        .withDeploy(Deploy.local)
        .withDispatcher(ext.Settings.Dispatcher)),
    ext.Settings.Resolver)

  private val cacheCleanup = ext.cache match {
    case cleanup: PeriodicCacheCleanup => Some(cleanup)
    case _                             => None
  }

  private val cleanupTimer = cacheCleanup.map { _ =>
    val interval = Duration(
      ext.Settings.ResolverConfig.getDuration("cache-cleanup-interval", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)
    system.scheduler.scheduleWithFixedDelay(interval, interval, self, SimpleDnsManager.CacheCleanup)
  }

  @nowarn("cat=deprecation")
  val oldApis: Receive = {
    case r @ Dns.Resolve(name) =>
      log.debug("(deprecated) Resolution request for {} from {}", name, sender())
      resolver.forward(r)
  }

  // the inet resolver supports the old and new DNS APIs
  override def receive: Receive = oldApis.orElse {
    case m: dns.DnsProtocol.Resolve =>
      log.debug("Resolution request for {} from {}", m.name, sender())
      resolver.forward(m)

    case SimpleDnsManager.CacheCleanup =>
      cacheCleanup.foreach(_.cleanup())
  }

  override def postStop(): Unit = {
    cleanupTimer.foreach(_.cancel())
  }
}

object SimpleDnsManager {
  private case object CacheCleanup
}
