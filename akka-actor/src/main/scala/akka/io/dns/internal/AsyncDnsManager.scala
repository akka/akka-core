/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

import com.typesafe.config.Config

import akka.actor.{ Actor, ActorLogging, ActorRefFactory, Deploy, ExtendedActorSystem, Props, Timers }
import akka.annotation.InternalApi
import akka.dispatch.{ RequiresMessageQueue, UnboundedMessageQueueSemantics }
import akka.io.{ Dns, DnsExt, DnsProvider }
import akka.io.PeriodicCacheCleanup
import akka.io.dns.{ AAAARecord, ARecord, DnsProtocol, DnsSettings }
import akka.io.dns.internal.AsyncDnsManager.CacheCleanup
import akka.routing.FromConfig
import akka.util.Timeout

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
  import akka.pattern.ask
  import akka.pattern.pipe

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

  implicit val ec: ExecutionContextExecutor = context.dispatcher

  val settings = new DnsSettings(system, resolverConfig)
  implicit val timeout: Timeout = Timeout(settings.ResolveTimeout)

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

  // still support deprecated DNS API
  @nowarn("msg=deprecated")
  override def receive: Receive = {
    case r: DnsProtocol.Resolve =>
      log.debug("Resolution request for {} {} from {}", r.name, r.requestType, sender())
      resolver.forward(r)

    case Dns.Resolve(name) =>
      // adapt legacy protocol to new protocol and back again, no where in akka
      // sends this message but supported until the old messages are removed
      log.debug("(deprecated) Resolution request for {} from {}", name, sender())
      val adapted = DnsProtocol.Resolve(name)
      val reply = (resolver ? adapted).mapTo[DnsProtocol.Resolved].map { asyncResolved =>
        val ips = asyncResolved.records.collect {
          case a: ARecord    => a.ip
          case a: AAAARecord => a.ip
        }
        Dns.Resolved(asyncResolved.name, ips)
      }
      reply.pipeTo(sender())

    case CacheCleanup =>
      cacheCleanup.foreach(_.cleanup())

    case AsyncDnsManager.GetCache =>
      sender() ! cache

  }
}
