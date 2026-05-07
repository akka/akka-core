/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import akka.actor.Actor

/**
 * Where as it is possible to plug in alternative DNS implementations it is not recommended.
 *
 * It is expected that this will be deprecated/removed in future Akka versions
 *
 *  TODO make private and remove deprecated in 2.7.0
 *
 */
@deprecated("Overriding the DNS implementation will be removed in future versions of Akka", "2.6.0")
trait DnsProvider {

  /**
   * Cache implementation that can be accessed via Dns(system) to avoid asks to the resolver actors.
   * It is not recommended to override the default SimpleDnsCache
   */
  def cache: Dns = new SimpleDnsCache()

  /**
   * DNS resolver actor. Should respond to [[akka.io.dns.DnsProtocol.Resolve]] with
   * [[akka.io.dns.DnsProtocol.Resolved]]
   */
  def actorClass: Class[_ <: Actor]

  /**
   * DNS manager class. Is responsible for creating resolvers and doing any cache cleanup.
   * The DNS extension will create one of these Actors. It should have a ctr that accepts
   * a [[DnsExt]]
   */
  def managerClass: Class[_ <: Actor]
}
