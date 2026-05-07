/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.discovery.aggregate

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import com.typesafe.config.Config

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.discovery.{ Discovery, Lookup, ServiceDiscovery }
import akka.discovery.ServiceDiscovery.Resolved
import akka.discovery.aggregate.AggregateServiceDiscovery.Methods
import akka.dispatch.MessageDispatcher
import akka.event.Logging
import akka.util.Helpers.Requiring
import scala.jdk.CollectionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private final class AggregateServiceDiscoverySettings(config: Config) {

  val discoveryMethods = config
    .getStringList("discovery-methods")
    .asScala
    .toList
    .requiring(_.nonEmpty, "At least one discovery method should be specified")

}

/**
 * INTERNAL API
 */
@InternalApi
private object AggregateServiceDiscovery {
  type Methods = List[(String, ServiceDiscovery)]
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class AggregateServiceDiscovery(system: ExtendedActorSystem) extends ServiceDiscovery {

  private val log = Logging(system, classOf[AggregateServiceDiscovery])

  private val settings =
    new AggregateServiceDiscoverySettings(system.settings.config.getConfig("akka.discovery.aggregate"))

  private val methods = {
    val serviceDiscovery = Discovery(system)
    settings.discoveryMethods.map(mech => (mech, serviceDiscovery.loadServiceDiscovery(mech)))
  }
  private implicit val ec: MessageDispatcher = system.dispatchers.internalDispatcher

  /**
   * Each discovery method is given the resolveTimeout rather than reducing it each time between methods.
   */
  override def lookup(lookup: Lookup, resolveTimeout: FiniteDuration): Future[Resolved] =
    resolve(methods, lookup, resolveTimeout)

  private def resolve(sds: Methods, query: Lookup, resolveTimeout: FiniteDuration): Future[Resolved] = {
    sds match {
      case (method, next) :: Nil =>
        log.debug("Looking up [{}] with [{}]", query, method)
        next.lookup(query, resolveTimeout)
      case (method, next) :: tail =>
        log.debug("Looking up [{}] with [{}]", query, method)
        // If nothing comes back then try the next one
        next
          .lookup(query, resolveTimeout)
          .flatMap { resolved =>
            if (resolved.addresses.isEmpty) {
              log.debug("Method[{}] returned no ResolvedTargets, trying next", query)
              resolve(tail, query, resolveTimeout)
            } else
              Future.successful(resolved)
          }
          .recoverWith {
            case NonFatal(t) =>
              log.error(t, "[{}] Service discovery failed. Trying next discovery method", method)
              resolve(tail, query, resolveTimeout)
          }
      case Nil =>
        // this is checked in `discoveryMethods`, but silence compiler warning
        throw new IllegalStateException("At least one discovery method should be specified")
    }
  }
}
