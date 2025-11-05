/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://akka.io>
 */

package akka.discovery.dns

import java.net.{ Inet6Address, InetAddress }

import scala.collection.{ immutable => im }
import scala.concurrent.duration._

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ActorRef
import akka.actor.ExtendedActorSystem
import akka.discovery
import akka.discovery.ServiceDiscovery
import akka.discovery.ServiceDiscovery.{ Resolved, ResolvedTarget }
import akka.discovery.ServiceDiscovery.DiscoveryTimeoutException
import akka.io.dns.{ AAAARecord, ARecord, DnsProtocol, SRVRecord }
import akka.io.dns.CachePolicy.Ttl
import akka.testkit.AkkaSpec
import akka.testkit.TestProbe

class DnsServiceDiscoverySpec extends AkkaSpec with AnyWordSpecLike with Matchers with ScalaFutures {

  "DnsServiceDiscovery" must {
    "fail future with DiscoveryTimeoutException if IP dns resolve does not respond" in {
      val dnsProbe = TestProbe()
      val underTest = new DnsServiceDiscovery(system.asInstanceOf[ExtendedActorSystem]) {
        override def initializeDns(): ActorRef = dnsProbe.ref
      }
      val result = underTest.lookup("cats.com", 1.second)
      result.failed.futureValue shouldBe a[DiscoveryTimeoutException]
    }

    "fail future with DiscoveryTimeoutException if SRV dns resolve does not respond" in {
      val dnsProbe = TestProbe()
      val underTest = new DnsServiceDiscovery(system.asInstanceOf[ExtendedActorSystem]) {
        override def initializeDns(): ActorRef = dnsProbe.ref
      }
      val result = underTest.lookup(discovery.Lookup("cats.com").withPortName("dog").withProtocol("snake"), 1.second)
      result.failed.futureValue shouldBe a[DiscoveryTimeoutException]
    }
  }

  "srvRecordsToResolved" must {
    "fill in ips from A records" in {
      val resolved = DnsProtocol.Resolved(
        "cats.com",
        im.Seq(new SRVRecord("cats.com", Ttl.fromPositive(1.second), 2, 3, 4, "kittens.com")),
        im.Seq(
          new ARecord("kittens.com", Ttl.fromPositive(1.second), InetAddress.getByName("127.0.0.2")),
          new ARecord("kittens.com", Ttl.fromPositive(1.second), InetAddress.getByName("127.0.0.3")),
          new ARecord("donkeys.com", Ttl.fromPositive(1.second), InetAddress.getByName("127.0.0.4"))))

      val result: ServiceDiscovery.Resolved =
        DnsServiceDiscovery.srvRecordsToResolved("cats.com", resolved)

      result.serviceName shouldEqual "cats.com"
      result.addresses.toSet shouldEqual Set(
        ResolvedTarget("kittens.com", Some(4), Some(InetAddress.getByName("127.0.0.2"))),
        ResolvedTarget("kittens.com", Some(4), Some(InetAddress.getByName("127.0.0.3"))))
    }

    // Naughty DNS server
    "use SRV target and port if no additional records" in {
      val resolved = DnsProtocol.Resolved(
        "cats.com",
        im.Seq(new SRVRecord("cats.com", Ttl.fromPositive(1.second), 2, 3, 8080, "kittens.com")),
        im.Seq(new ARecord("donkeys.com", Ttl.fromPositive(1.second), InetAddress.getByName("127.0.0.4"))))

      val result =
        DnsServiceDiscovery.srvRecordsToResolved("cats.com", resolved)

      result shouldEqual Resolved("cats.com", im.Seq(ResolvedTarget("kittens.com", Some(8080), None)))
    }

    "fill in ips from AAAA records" in {
      val resolved = DnsProtocol.Resolved(
        "cats.com",
        im.Seq(new SRVRecord("cats1.com", Ttl.fromPositive(1.second), 2, 3, 4, "kittens.com")),
        im.Seq(
          new AAAARecord(
            "kittens.com",
            Ttl.fromPositive(2.seconds),
            InetAddress.getByName("::1").asInstanceOf[Inet6Address]),
          new AAAARecord(
            "kittens.com",
            Ttl.fromPositive(2.seconds),
            InetAddress.getByName("::2").asInstanceOf[Inet6Address]),
          new AAAARecord(
            "donkeys.com",
            Ttl.fromPositive(2.seconds),
            InetAddress.getByName("::3").asInstanceOf[Inet6Address])))

      val result: ServiceDiscovery.Resolved =
        DnsServiceDiscovery.srvRecordsToResolved("cats.com", resolved)

      result.serviceName shouldEqual "cats.com"
      result.addresses.toSet shouldEqual Set(
        ResolvedTarget("kittens.com", Some(4), Some(InetAddress.getByName("::1"))),
        ResolvedTarget("kittens.com", Some(4), Some(InetAddress.getByName("::2"))))
    }

  }
}
