/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns

import java.io.File
import java.net.{ InetSocketAddress, URI }
import java.util

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.util.{ Failure, Success, Try }

import com.typesafe.config.{ Config, ConfigValueType }

import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.io.dns.CachePolicy.{ CachePolicy, Forever, Never, Ttl }
import akka.io.dns.internal.{ ResolvConf, ResolvConfParser }
import akka.util.Helpers
import akka.util.Helpers.Requiring
import scala.jdk.CollectionConverters._

/** INTERNAL API */
@InternalApi
private[dns] final class DnsSettings(system: ExtendedActorSystem, c: Config) {

  import DnsSettings._

  val NameServers: List[InetSocketAddress] = {
    c.getValue("nameservers").valueType() match {
      case ConfigValueType.STRING =>
        c.getString("nameservers") match {
          case "default" =>
            val osAddresses = getDefaultNameServers(system).getOrElse(failUnableToDetermineDefaultNameservers)
            if (osAddresses.isEmpty) failUnableToDetermineDefaultNameservers
            osAddresses
          case other =>
            parseNameserverAddress(other) :: Nil
        }
      case ConfigValueType.LIST =>
        val userAddresses =
          c.getStringList("nameservers").asScala.iterator.map(parseNameserverAddress).to(immutable.IndexedSeq)
        require(userAddresses.nonEmpty, "nameservers can not be empty")
        userAddresses.toList
      case _ => throw new IllegalArgumentException("Invalid type for nameservers. Must be a string or string list")
    }
  }

  val ResolveTimeout: FiniteDuration = c.getDuration("resolve-timeout").toScala

  val PositiveCachePolicy: CachePolicy = getTtl("positive-ttl")
  val NegativeCachePolicy: CachePolicy = getTtl("negative-ttl")

  private def getTtl(path: String): CachePolicy =
    c.getString(path) match {
      case "forever" => Forever
      case "never"   => Never
      case _ =>
        val finiteTtl = c
          .getDuration(path)
          .requiring(!_.isNegative, s"akka.io.dns.$path must be 'default', 'forever', 'never' or positive duration")
        Ttl.fromPositive(finiteTtl)
    }

  private lazy val resolvConf: Option[ResolvConf] = {
    val etcResolvConf = new File("/etc/resolv.conf")
    // Avoid doing the check on Windows, no point
    if (Helpers.isWindows) {
      None
    } else if (etcResolvConf.exists()) {
      val parsed = ResolvConfParser.parseFile(etcResolvConf)
      parsed match {
        case Success(value) => Some(value)
        case Failure(exception) =>
          val log = Logging(system, classOf[DnsSettings])
          if (log.isWarningEnabled) {
            log.error(exception, "Error parsing /etc/resolv.conf, ignoring.")
          }
          None
      }
    } else None
  }

  val SearchDomains: List[String] = {
    c.getValue("search-domains").valueType() match {
      case ConfigValueType.STRING =>
        c.getString("search-domains") match {
          case "default" => resolvConf.map(_.search).getOrElse(Nil)
          case single    => List(single)
        }
      case ConfigValueType.LIST =>
        c.getStringList("search-domains").asScala.toList
      case _ => throw new IllegalArgumentException("Invalid type for search-domains. Must be a string or string list.")
    }
  }

  val NDots: Int = {
    c.getValue("ndots").valueType() match {
      case ConfigValueType.STRING =>
        c.getString("ndots") match {
          case "default" => resolvConf.map(_.ndots).getOrElse(1)
          case _ =>
            throw new IllegalArgumentException("Invalid value for ndots. Must be the string 'default' or an integer.")
        }
      case ConfigValueType.NUMBER =>
        val ndots = c.getInt("ndots")
        if (ndots < 0) {
          throw new IllegalArgumentException("Invalid value for ndots, ndots must not be negative.")
        }
        ndots
      case _ =>
        throw new IllegalArgumentException("Invalid value for ndots. Must be the string 'default' or an integer.")
    }
  }

  val RandomStrategyName: String = c.getString("id-strategy")

  /**
   * A thunk to generate the next request ID.  Not thread-safe, requires some other coordination.
   */
  def idGenerator: Function0[Short] =
    if (RandomStrategyName == "NOT-IN-ANY-WAY-RANDOM-test-sequential") {
      new Function0[Short] {
        var lastId: Short = 0
        def apply(): Short = {
          lastId = (lastId + 1).toShort
          lastId
        }

        override def toString: String = "NOT-IN-ANY-WAY-RANDOM-test-sequential"
      }
    } else {
      import java.security.SecureRandom

      new Function0[Short] {
        val rng = RandomStrategyName match {
          case "" | "SecureRandom" =>
            system.log.debug("Using platform default SecureRandom algorithm for DNS request IDs")
            new SecureRandom

          case custom =>
            system.log.debug("Using {} SecureRandom algorithm for DNS request IDs", custom)
            SecureRandom.getInstance(custom)
        }

        // toShort just uses the low order 16-bits, so the distribution is as unpredictable as for ints
        def apply(): Short = rng.nextInt().toShort

        override val toString: String = RandomStrategyName match {
          case "" => "platform default SecureRandom algorithm"
          case s  => s"$s SecureRandom algorithm"
        }
      }
    }

  // -------------------------

  def failUnableToDetermineDefaultNameservers =
    throw new IllegalStateException(
      "Unable to obtain default nameservers from JNDI or via reflection. " +
      "Please set `akka.io.dns.async-dns.nameservers` explicitly in order to be able to resolve domain names. ")

}

object DnsSettings {

  private final val DnsFallbackPort = 53
  private val inetSocketAddress = """(.*?)(?::(\d+))?""".r

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def parseNameserverAddress(str: String): InetSocketAddress =
    str match {
      case inetSocketAddress(host, port) =>
        new InetSocketAddress(host, Option(port).fold(DnsFallbackPort)(_.toInt))
      case unexpected =>
        throw new IllegalArgumentException(s"Unparseable address string: $unexpected") // will not happen, for exhaustiveness check
    }

  /**
   * INTERNAL API
   * Find out the default search lists that Java would use normally, e.g. when using InetAddress to resolve domains.
   *
   * The default nameservers are attempted to be obtained from: jndi-dns and from `sun.net.dnsResolverConfiguration`
   * as a fallback (which is expected to fail though when running on JDK9+ due to the module encapsulation of sun packages).
   *
   * Based on: https://github.com/netty/netty/blob/4.1/resolver-dns/src/main/java/io/netty/resolver/dns/DefaultDnsServerAddressStreamProvider.java#L58-L146
   */
  private[akka] def getDefaultNameServers(system: ExtendedActorSystem): Try[List[InetSocketAddress]] = {
    def asInetSocketAddress(server: String): Try[InetSocketAddress] = {
      Try {
        val uri = new URI(server)
        val host = uri.getHost
        val port = uri.getPort match {
          case -1       => DnsFallbackPort
          case selected => selected
        }
        new InetSocketAddress(host, port)
      }
    }

    def getNameserversUsingJNDI: Try[List[InetSocketAddress]] = {
      import java.util
      import javax.naming.Context
      import javax.naming.directory.InitialDirContext
      // Using jndi-dns to obtain the default name servers.
      //
      // See:
      // - https://docs.oracle.com/javase/8/docs/technotes/guides/jndi/jndi-dns.html
      // - https://mail.openjdk.java.net/pipermail/net-dev/2017-March/010695.html
      val env = new util.Hashtable[String, String]
      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory")
      env.put("java.naming.provider.url", "dns://")

      Try {
        val ctx = new InitialDirContext(env)
        val dnsUrls = ctx.getEnvironment.get("java.naming.provider.url").asInstanceOf[String]
        // Only try if not empty as otherwise we will produce an exception
        if (dnsUrls != null && !dnsUrls.isEmpty) {
          val servers = dnsUrls.split(" ")
          servers.flatMap { server =>
            asInetSocketAddress(server).toOption
          }.toList
        } else Nil
      }
    }

    // this method is used as a fallback in case JNDI results in an empty list
    // this method will not work when running modularised of course since it needs access to internal sun classes
    def getNameserversUsingReflection: Try[List[InetSocketAddress]] = {
      system.dynamicAccess.getClassFor[Any]("sun.net.dns.ResolverConfiguration").flatMap { c =>
        Try {
          val open = c.getMethod("open")
          val nameservers = c.getMethod("nameservers")
          val instance = open.invoke(null)
          val ns = nameservers.invoke(instance).asInstanceOf[util.List[String]]
          val res = if (ns.isEmpty)
            throw new IllegalStateException(
              "Empty nameservers list discovered using reflection. Consider configuring default nameservers manually!")
          else ns.asScala.toList
          res.flatMap(s => asInetSocketAddress(s).toOption)
        }
      }
    }

    getNameserversUsingJNDI.orElse(getNameserversUsingReflection)
  }
}
