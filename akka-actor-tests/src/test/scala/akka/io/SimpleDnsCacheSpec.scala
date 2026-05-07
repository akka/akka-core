/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.io.dns.ARecord
import akka.io.dns.CachePolicy.Ttl
import akka.io.dns.DnsProtocol
import akka.io.dns.DnsProtocol.Ip

class SimpleDnsCacheSpec extends AnyWordSpec with Matchers {
  "Cache" should {
    "not reply with expired but not yet swept out entries" in {
      val localClock = new AtomicLong(0)
      val cache: SimpleDnsCache = new SimpleDnsCache() {
        override protected def clock() = localClock.get
      }
      val ttl = Ttl.fromPositive(5000.millis)
      val cacheEntry = DnsProtocol.Resolved(
        "test.local",
        immutable.Seq(ARecord("test.local", ttl, InetAddress.getByName("127.0.0.1"))))
      cache.put(("test.local", Ip()), cacheEntry, ttl)

      cache.cached(DnsProtocol.Resolve("test.local")) should ===(Some(cacheEntry))
      localClock.set(4999)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(Some(cacheEntry))
      localClock.set(5000)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(None)
    }

    "sweep out expired entries on cleanup()" in {
      val localClock = new AtomicLong(0)
      val cache: SimpleDnsCache = new SimpleDnsCache() {
        override protected def clock() = localClock.get
      }
      val ttl = Ttl.fromPositive(5000.millis)
      val cacheEntry =
        DnsProtocol.Resolved(
          "test.local",
          immutable.Seq(ARecord("test.local", ttl, InetAddress.getByName("127.0.0.1"))))
      cache.put(("test.local", Ip()), cacheEntry, ttl)

      cache.cached(DnsProtocol.Resolve("test.local")) should ===(Some(cacheEntry))
      localClock.set(5000)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(None)
      localClock.set(0)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(Some(cacheEntry))
      localClock.set(5000)
      cache.cleanup()
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(None)
      localClock.set(0)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(None)
    }

    "drop a specific key" in {
      val localClock = new AtomicLong(0)
      val cache: SimpleDnsCache = new SimpleDnsCache() {
        override protected def clock() = localClock.get
      }
      val ttl = Ttl.fromPositive(5000.millis)
      val cacheEntry1 =
        DnsProtocol.Resolved(
          "test.local",
          immutable.Seq(ARecord("test.local", ttl, InetAddress.getByName("127.0.0.1"))))
      cache.put(("test.local", Ip()), cacheEntry1, ttl)
      val cacheEntry2 =
        DnsProtocol.Resolved(
          "example.com",
          immutable.Seq(ARecord("example.com", ttl, InetAddress.getByName("127.0.0.1"))))
      cache.put(("example.com", Ip()), cacheEntry2, ttl)
      cache.drop(("example.com", Ip()))
      cache.cached(DnsProtocol.Resolve("example.com")) should ===(None)
      cache.cached(DnsProtocol.Resolve("test.local")) should ===(Some(cacheEntry1))

    }

  }

  // TODO test that the old protocol is converted correctly
}
