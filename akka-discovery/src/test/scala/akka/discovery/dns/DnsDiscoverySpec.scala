/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.discovery.dns

import java.net.InetAddress

import scala.concurrent.duration._

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.discovery.{ Discovery, Lookup }
import akka.discovery.ServiceDiscovery
import akka.discovery.ServiceDiscovery.ResolvedTarget
import akka.io.dns.DockerBindDnsService
import akka.testkit.{ SocketUtil, TestKit }

object DnsDiscoverySpec {

  val config = ConfigFactory.parseString(s"""
     akka {
       discovery {
         method = akka-dns
       }
     }
     akka {
       loglevel = DEBUG
     }
     akka.io.dns.async-dns.nameservers = ["localhost:${DnsDiscoverySpec.dockerDnsServerPort}"]
    """)

  lazy val dockerDnsServerPort = SocketUtil.temporaryLocalPort()

  val configWithAsyncDnsResolverAsDefault = ConfigFactory.parseString("""
      akka.io.dns.resolver = "async-dns"
    """).withFallback(config)

}

class DnsDiscoverySpec extends DockerBindDnsService(DnsDiscoverySpec.config) {

  import DnsDiscoverySpec._

  override val hostPort: Int = DnsDiscoverySpec.dockerDnsServerPort

  val systemWithAsyncDnsAsResolver = ActorSystem("AsyncDnsSystem", configWithAsyncDnsResolverAsDefault)

  private def testSrvRecords(discovery: ServiceDiscovery) = {
    val name = "_service._tcp.foo.test."

    def lookup() =
      discovery
        .lookup(Lookup("foo.test.").withPortName("service").withProtocol("tcp"), resolveTimeout = 10.seconds)
        .futureValue

    val expected = Set(
      ResolvedTarget("a-single.foo.test", Some(5060), Some(InetAddress.getByName("192.168.1.20"))),
      ResolvedTarget("a-double.foo.test", Some(65535), Some(InetAddress.getByName("192.168.1.21"))),
      ResolvedTarget("a-double.foo.test", Some(65535), Some(InetAddress.getByName("192.168.1.22"))))

    val result1 = lookup()
    result1.addresses.toSet shouldEqual expected
    result1.serviceName shouldEqual name

    // one more time to exercise the cache
    val result2 = lookup()
    result2.addresses.toSet shouldEqual expected
    result2.serviceName shouldEqual name
  }

  private def testIpRecords(discovery: ServiceDiscovery) = {
    val name = "a-single.foo.test"

    val expected = Set(ResolvedTarget("192.168.1.20", None, Some(InetAddress.getByName("192.168.1.20"))))

    def lookup() = discovery.lookup(name, resolveTimeout = 500.milliseconds).futureValue

    val result1 = lookup()
    result1.serviceName shouldEqual name
    result1.addresses.toSet shouldEqual expected

    // one more time to exercise the cache
    val result2 = lookup()
    result2.serviceName shouldEqual name
    result2.addresses.toSet shouldEqual expected
  }

  "Dns Discovery with isolated resolver" must {

    if (!dockerAvailable()) {
      info("Test not run as docker is not available")
      pending
    }

    "work with SRV records" in {
      val discovery = Discovery(system).discovery
      testSrvRecords(discovery)
    }

    "work with IP records" in {
      val discovery = Discovery(system).discovery
      testIpRecords(discovery)
    }

    "be using its own resolver" in {
      // future will fail if it it doesn't exist
      system.actorSelection("/system/SD-DNS/async-dns").resolveOne(2.seconds).futureValue
    }

  }

  "Dns discovery with the system resolver" must {
    if (!dockerAvailable()) {
      info("Test not run as docker is not available")
      pending
    }

    "work with SRV records" in {
      val discovery = Discovery(systemWithAsyncDnsAsResolver).discovery
      testSrvRecords(discovery)
    }

    "work with IP records" in {
      val discovery = Discovery(systemWithAsyncDnsAsResolver).discovery
      testIpRecords(discovery)
    }

    "be using the system resolver" in {
      // check the service discovery one doesn't exist
      systemWithAsyncDnsAsResolver.actorSelection("/system/SD-DNS/async-dns").resolveOne(2.seconds).failed.futureValue
    }

  }

  override def afterTermination(): Unit = {
    try {
      TestKit.shutdownActorSystem(systemWithAsyncDnsAsResolver)
    } finally {
      super.afterTermination()
    }
  }
}
