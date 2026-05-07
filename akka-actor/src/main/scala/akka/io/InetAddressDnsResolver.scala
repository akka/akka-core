/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.{ InetAddress, UnknownHostException }
import java.net.Inet4Address
import java.net.Inet6Address
import java.security.Security
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import com.typesafe.config.Config

import akka.actor.{ Actor, ActorLogging }
import akka.actor.Status
import akka.annotation.InternalApi
import akka.io.dns.AAAARecord
import akka.io.dns.ARecord
import akka.io.dns.CachePolicy._
import akka.io.dns.DnsProtocol
import akka.io.dns.DnsProtocol.Ip
import akka.io.dns.DnsProtocol.Srv
import akka.io.dns.ResourceRecord
import akka.util.Helpers.Requiring

/**
 * INTERNAL API
 *
 * Respects the settings that can be set on the Java runtime via parameters.
 */
@nowarn("msg=deprecated")
@InternalApi
class InetAddressDnsResolver(cache: SimpleDnsCache, config: Config) extends Actor with ActorLogging {

  // Controls the cache policy for successful lookups only
  private final val CachePolicyProp = "networkaddress.cache.ttl"
  // Deprecated JVM property key, keeping for legacy compatibility; replaced by CachePolicyProp
  private final val CachePolicyPropFallback = "sun.net.inetaddr.ttl"

  // Controls the cache policy for negative lookups only
  private final val NegativeCachePolicyProp = "networkaddress.cache.negative.ttl"
  // Deprecated JVM property key, keeping for legacy compatibility; replaced by NegativeCachePolicyProp
  private final val NegativeCachePolicyPropFallback = "sun.net.inetaddr.negative.ttl"

  private final val DefaultPositive = Ttl.fromPositive(30.seconds)

  private lazy val defaultCachePolicy: CachePolicy =
    Option(Security.getProperty(CachePolicyProp))
      .filter(_ != "")
      .orElse(Option(System.getProperty(CachePolicyPropFallback)))
      .filter(_ != "")
      .map(x => Try(x.toInt)) match {
      case None             => DefaultPositive
      case Some(Success(n)) => parsePolicy(n)
      case Some(Failure(_)) =>
        log.warning("Caching TTL misconfigured. Using default value {}.", DefaultPositive)
        DefaultPositive
    }

  private lazy val defaultNegativeCachePolicy: CachePolicy =
    Option(Security.getProperty(NegativeCachePolicyProp))
      .filter(_ != "")
      .orElse(Option(System.getProperty(NegativeCachePolicyPropFallback)))
      .filter(_ != "")
      .map(x => Try(x.toInt)) match {
      case None             => Never
      case Some(Success(n)) => parsePolicy(n)
      case Some(Failure(_)) =>
        log.warning("Negative caching TTL misconfigured. Using default value {}.", Never)
        Never
    }

  private def parsePolicy(n: Int): CachePolicy = {
    n match {
      case 0          => Never
      case x if x < 0 => Forever
      case x          => Ttl.fromPositive(x.seconds)
    }
  }

  private def getTtl(path: String, positive: Boolean): CachePolicy =
    config.getString(path) match {
      case "default" => if (positive) defaultCachePolicy else defaultNegativeCachePolicy
      case "forever" => Forever
      case "never"   => Never
      case _ => {
        val finiteTtl = config
          .getDuration(path, TimeUnit.SECONDS)
          .requiring(_ > 0, s"akka.io.dns.$path must be 'default', 'forever', 'never' or positive duration")
        Ttl.fromPositive(finiteTtl.seconds)
      }
    }

  val positiveCachePolicy: CachePolicy = getTtl("positive-ttl", positive = true)
  val negativeCachePolicy: CachePolicy = getTtl("negative-ttl", positive = false)
  @deprecated("Use positiveCacheDuration instead", "2.5.17")
  val positiveTtl: Long = toLongTtl(positiveCachePolicy)
  @deprecated("Use negativeCacheDuration instead", "2.5.17")
  val negativeTtl: Long = toLongTtl(negativeCachePolicy)

  private def toLongTtl(cp: CachePolicy): Long = {
    cp match {
      case Forever  => Long.MaxValue
      case Never    => 0
      case ttl: Ttl => ttl.value.toMillis
    }
  }

  override def receive: Receive = {
    case DnsProtocol.Resolve(_, Srv) =>
      sender() ! Status.Failure(
        new IllegalArgumentException(
          "SRV request sent to InetResolver. SRV requests are only supported by async-dns resolver."))

    case r @ DnsProtocol.Resolve(name, ip @ Ip(ipv4, ipv6)) =>
      val answer = cache.cached(r) match {
        case Some(a) => a
        case None =>
          log.debug("Request for [{}] was not yet cached", name)
          try {
            val addresses: Array[InetAddress] = InetAddress.getAllByName(name)
            val records = addressToRecords(name, addresses.toList, ipv4, ipv6)
            val answer = DnsProtocol.Resolved(name, records.toList)
            if (positiveCachePolicy != Never)
              cache.put((name, Ip()), DnsProtocol.Resolved(name, records), positiveCachePolicy)
            answer
          } catch {
            case _: UnknownHostException =>
              val answer = DnsProtocol.Resolved(name, immutable.Seq.empty)
              if (negativeCachePolicy != Never)
                cache.put((name, ip), answer, negativeCachePolicy)
              answer
          }
      }
      sender() ! answer
    case Dns.Resolve(name) =>
      // no where in akka now sends this message, but supported until Dns.Resolve/Resolved have been removed
      val answer: Dns.Resolved = cache.cached(name) match {
        case Some(a) => a
        case None =>
          try {
            val addresses = InetAddress.getAllByName(name)
            // respond with the old protocol as the request was the new protocol
            val answer = Dns.Resolved(name, addresses)
            if (positiveCachePolicy != Never) {
              val records = addressToRecords(name, addresses.toList, ipv4 = true, ipv6 = true)
              cache.put((name, Ip()), DnsProtocol.Resolved(name, records), positiveCachePolicy)
            }
            answer
          } catch {
            case _: UnknownHostException =>
              val answer = Dns.Resolved(name, immutable.Seq.empty, immutable.Seq.empty)
              if (negativeCachePolicy != Never)
                cache.put((name, Ip()), DnsProtocol.Resolved(name, immutable.Seq.empty), negativeCachePolicy)
              answer
          }
      }
      sender() ! answer
  }

  private def addressToRecords(
      name: String,
      addresses: immutable.Seq[InetAddress],
      ipv4: Boolean,
      ipv6: Boolean): immutable.Seq[ResourceRecord] = {
    addresses.collect {
      case a: Inet4Address if ipv4 => ARecord(name, Ttl.toTll(positiveCachePolicy), a)
      case a: Inet6Address if ipv6 => AAAARecord(name, Ttl.toTll(positiveCachePolicy), a)
    }
  }

}
