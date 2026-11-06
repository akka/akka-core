/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.{ Inet4Address, Inet6Address, InetAddress }
import java.util.concurrent.ConcurrentHashMap
import java.util.function.{ Function => JFunction }

import scala.annotation.nowarn

import com.typesafe.config.Config

import akka.actor._
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.event.Logging
import akka.io.dns.DnsProtocol

/**
 * Not for user extension.
 *
 * This used to be a supported extension point but will be removed in future versions of Akka.
 */
@DoNotInherit
abstract class Dns {

  def cached(request: DnsProtocol.Resolve): Option[DnsProtocol.Resolved] = None

  def resolve(request: DnsProtocol.Resolve, system: ActorSystem, sender: ActorRef): Option[DnsProtocol.Resolved] = {
    val ret = cached(request)
    if (ret.isEmpty)
      IO(Dns)(system).tell(request, sender)
    ret
  }
}

object Dns extends ExtensionId[DnsExt] with ExtensionIdProvider {

  /**
   * Lookup if a DNS resolved is cached. The exact behavior of caching will depend on
   * the akka.actor.io.dns.resolver that is configured.
   */
  def cached(name: DnsProtocol.Resolve)(system: ActorSystem): Option[DnsProtocol.Resolved] = {
    Dns(system).cache.cached(name)
  }

  /**
   * If an entry is cached return it immediately. If it is not then
   * trigger a resolve and return None.
   */
  def resolve(name: DnsProtocol.Resolve, system: ActorSystem, sender: ActorRef): Option[DnsProtocol.Resolved] = {
    Dns(system).cache.resolve(name, system, sender)
  }

  override def lookup = Dns

  override def createExtension(system: ExtendedActorSystem): DnsExt = new DnsExt(system)

  /**
   * Java API: retrieve the Udp extension for the given system.
   */
  override def get(system: ActorSystem): DnsExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): DnsExt = super.get(system)
}

class DnsExt private[akka] (val system: ExtendedActorSystem, resolverName: String, managerName: String)
    extends IO.Extension {

  private val asyncDns = new ConcurrentHashMap[String, ActorRef]

  /**
   * INTERNAL API
   *
   * Load an additional async-dns resolver. Can be used to use async-dns even if inet-resolver is the configured
   * default.
   * Intentionally chosen not to support loading an arbitrary resolver as it required a specific constructor
   * for the manager actor. The expected constructor for DNS plugins is just to take in a DnsExt which can't
   * be used in this case
   */
  @InternalApi
  @nowarn("msg=deprecated")
  private[akka] def loadAsyncDns(managerName: String): ActorRef = {
    // This can't pass in `this` as then AsyncDns would pick up the system settings
    asyncDns.computeIfAbsent(
      managerName,
      new JFunction[String, ActorRef] {
        override def apply(r: String): ActorRef = {
          val settings =
            new Settings(system.settings.config.getConfig("akka.io.dns"), "async-dns")
          val provider = system.dynamicAccess.createInstanceFor[DnsProvider](settings.ProviderObjectName, Nil).get
          Logging(system, classOf[DnsExt])
            .info("Creating async dns resolver {} with manager name {}", settings.Resolver, managerName)

          system.systemActorOf(
            props = Props(
              provider.managerClass,
              settings.Resolver,
              system,
              settings.ResolverConfig,
              provider.cache,
              settings.Dispatcher,
              provider).withDeploy(Deploy.local).withDispatcher(settings.Dispatcher),
            name = managerName)
        }
      })
  }

  /**
   * INTERNAL API
   *
   * Use IO(DNS) or Dns(system). Do not instantiate directly
   *
   * For binary compat as DnsExt constructor didn't used to have internal API on
   */
  @InternalApi
  def this(system: ExtendedActorSystem) =
    this(system, system.settings.config.getString("akka.io.dns.resolver"), "IO-DNS")

  class Settings private[DnsExt] (config: Config, resolverName: String) {

    /**
     * Load the default resolver
     */
    def this(config: Config) = this(config, config.getString("resolver"))

    val Dispatcher: String = config.getString("dispatcher")
    val Resolver: String = resolverName
    val ResolverConfig: Config = config.getConfig(Resolver)
    val ProviderObjectName: String = ResolverConfig.getString("provider-object")

    override def toString = s"Settings($Dispatcher, $Resolver, $ResolverConfig, $ProviderObjectName)"
  }

  // Settings for the system resolver
  val Settings: Settings = new Settings(system.settings.config.getConfig("akka.io.dns"), resolverName)

  // System DNS resolver
  @nowarn("msg=deprecated")
  val provider: DnsProvider =
    system.dynamicAccess.createInstanceFor[DnsProvider](Settings.ProviderObjectName, Nil).get

  // System DNS cache
  val cache: Dns = provider.cache

  // System DNS manager
  val manager: ActorRef = {
    system.systemActorOf(
      props = Props(provider.managerClass, this).withDeploy(Deploy.local).withDispatcher(Settings.Dispatcher),
      name = managerName)
  }

  // System DNS manager
  def getResolver: ActorRef = manager

}

/**
 * INTERNAL API
 */
@InternalApi
object IpVersionSelector {

  /**
   * INTERNAL API
   */
  @InternalApi
  def getInetAddress(ipv4: Option[Inet4Address], ipv6: Option[Inet6Address]): Option[InetAddress] =
    System.getProperty("java.net.preferIPv6Addresses") match {
      case "true" => ipv6.orElse(ipv4)
      case _      => ipv4.orElse(ipv6)
    }
}
