/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.concurrent.duration.Duration

import com.typesafe.config.Config

import akka.actor.{ Actor, ActorLogging, ActorRefFactory, Deploy, ExtendedActorSystem, Props, Timers }
import akka.annotation.InternalApi
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.{ Dns, DnsExt, DnsProvider }
import akka.io.PeriodicCacheCleanup
import akka.io.dns.{ DnsProtocol, DnsSettings }
import akka.io.dns.internal.AsyncDnsManager.CacheCleanup
import akka.routing.FromConfig

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object AsyncDnsManager {
  private case object CacheCleanup

  case object GetCache
}

/**
 * INTERNAL API
 */
@InternalApi
@nowarn("msg=deprecated")
private[io] final class AsyncDnsManager(
    name: String,
    system: ExtendedActorSystem,
    resolverConfig: Config,
    cache: Dns,
    dispatcher: String,
    provider: DnsProvider)
    extends Actor
    with RequiresMessageQueue[UnboundedMessageQueueSemantics]
    with ActorLogging
    with Timers {

  /**
   * Ctr expected by the DnsExt for all DnsMangers
   */
  def this(ext: DnsExt) =
    this(
      ext.Settings.Resolver,
      ext.system,
      ext.Settings.ResolverConfig,
      ext.cache,
      ext.Settings.Dispatcher,
      ext.provider)

  val settings = new DnsSettings(system, resolverConfig)

  private val resolver = {
    val props: Props = FromConfig.props(
      Props(provider.actorClass, settings, cache, (factory: ActorRefFactory, dns: List[InetSocketAddress]) => {
        dns.map(ns => factory.actorOf(Props(new DnsClient(ns))))
      }).withDeploy(Deploy.local).withDispatcher(dispatcher))
    context.actorOf(props, name)
  }

  private val cacheCleanup = cache match {
    case cleanup: PeriodicCacheCleanup => Some(cleanup)
    case _                             => None
  }

  override def preStart(): Unit = {
    cacheCleanup.foreach { _ =>
      val interval =
        Duration(resolverConfig.getDuration("cache-cleanup-interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
      timers.startTimerWithFixedDelay(CacheCleanup, CacheCleanup, interval)
    }
  }

  override def receive: Receive = {
    case r: DnsProtocol.Resolve =>
      log.debug("Resolution request for {} {} from {}", r.name, r.requestType, sender())
      resolver.forward(r)

    case CacheCleanup =>
      cacheCleanup.foreach(_.cleanup())

    case AsyncDnsManager.GetCache =>
      sender() ! cache

  }
}
