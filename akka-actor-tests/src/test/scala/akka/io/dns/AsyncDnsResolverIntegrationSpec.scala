/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns

import java.net.InetAddress

import scala.concurrent.duration._

import CachePolicy.Ttl
import com.typesafe.config.ConfigFactory
import org.scalatest.time.Millis
import org.scalatest.time.Span

import akka.io.{ Dns, IO }
import akka.io.dns.DnsProtocol.{ Ip, RequestType, Srv }
import akka.pattern.ask
import akka.testkit.SocketUtil
import akka.testkit.SocketUtil.Both
import akka.testkit.WithLogCapturing
import akka.util.Timeout

/**
 * These tests rely on a DNS server with 2 zones configured, foo.test and bar.example.
 *
 * The configuration to start a bind DNS server in Docker with this configuration
 * is included, and the test will automatically start this container when the
 * test starts and tear it down when it finishes.
 */
object AsyncDnsResolverIntegrationSpec {
  lazy val dockerDnsServerPort: Int = SocketUtil.temporaryLocalPort(Both)
  implicit val defaultTimeout: Timeout = Timeout(10.seconds)
  def conf = ConfigFactory.parseString(s"""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.dns.resolver = async-dns
    akka.io.dns.async-dns.nameservers = ["localhost:${dockerDnsServerPort}"]
    akka.io.dns.async-dns.search-domains = ["foo.test", "test"]
    akka.io.dns.async-dns.ndots = 2
    akka.io.dns.async-dns.resolve-timeout = ${defaultTimeout.duration.toSeconds}s
  """)
}

class AsyncDnsResolverIntegrationSpec
    extends DockerBindDnsService(AsyncDnsResolverIntegrationSpec.conf)
    with WithLogCapturing {
  import AsyncDnsResolverIntegrationSpec._

  override implicit val patience: PatienceConfig =
    PatienceConfig(defaultTimeout.duration + 1.second, Span(100, Millis))

  override val hostPort = dockerDnsServerPort

  "Resolver" must {
    if (!dockerAvailable()) {
      system.log.error("Test not run as docker is not available")
      pending
    } else {
      system.log.info("Docker available. Running DNS tests")
    }

    "resolve single A record" in {
      val name = "a-single.foo.test"
      val answer = resolve(name, DnsProtocol.Ip(ipv6 = false))
      withClue(answer) {
        answer.name shouldEqual name
        answer.records.size shouldEqual 1
        answer.records.head.name shouldEqual name
        answer.records.head.asInstanceOf[ARecord].ip shouldEqual InetAddress.getByName("192.168.1.20")
      }
    }

    "resolve double A records" in {
      val name = "a-double.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name
      answer.records.map(_.asInstanceOf[ARecord].ip).toSet shouldEqual Set(
        InetAddress.getByName("192.168.1.21"),
        InetAddress.getByName("192.168.1.22"))
    }

    "resolve single AAAA record" in {
      val name = "aaaa-single.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name
      answer.records.map(_.asInstanceOf[AAAARecord].ip) shouldEqual Seq(
        InetAddress.getByName("fd4d:36b2:3eca:a2d8:0:0:0:1"))
    }

    "resolve double AAAA records" in {
      val name = "aaaa-double.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name
      answer.records.map(_.asInstanceOf[AAAARecord].ip).toSet shouldEqual Set(
        InetAddress.getByName("fd4d:36b2:3eca:a2d8:0:0:0:2"),
        InetAddress.getByName("fd4d:36b2:3eca:a2d8:0:0:0:3"))
    }

    "resolve mixed A/AAAA records" in {
      val name = "a-aaaa.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name

      answer.records.collect { case r: ARecord => r.ip }.toSet shouldEqual Set(
        InetAddress.getByName("192.168.1.23"),
        InetAddress.getByName("192.168.1.24"))

      answer.records.collect { case r: AAAARecord => r.ip }.toSet shouldEqual Set(
        InetAddress.getByName("fd4d:36b2:3eca:a2d8:0:0:0:4"),
        InetAddress.getByName("fd4d:36b2:3eca:a2d8:0:0:0:5"))
    }

    "resolve external CNAME record" in {
      val name = "cname-ext.foo.test"
      val answer = (IO(Dns) ? DnsProtocol.Resolve(name)).mapTo[DnsProtocol.Resolved].futureValue
      answer.name shouldEqual name
      answer.records.collect { case r: CNameRecord => r.canonicalName }.toSet shouldEqual Set("a-single.bar.example")
      answer.records.collect { case r: ARecord     => r.ip }.toSet shouldEqual Set(InetAddress.getByName("192.168.2.20"))
    }

    "resolve internal CNAME record" in {
      val name = "cname-in.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name
      answer.records.collect { case r: CNameRecord => r.canonicalName }.toSet shouldEqual Set("a-double.foo.test")
      answer.records.collect { case r: ARecord     => r.ip }.toSet shouldEqual Set(
        InetAddress.getByName("192.168.1.21"),
        InetAddress.getByName("192.168.1.22"))
    }

    "resolve SRV record" in {
      val name = "_service._tcp.foo.test"
      val answer = resolve(name, Srv)

      answer.name shouldEqual name
      answer.records.collect { case r: SRVRecord => r }.toSet shouldEqual Set(
        SRVRecord("_service._tcp.foo.test", Ttl.fromPositive(86400.seconds), 10, 65534, 5060, "a-single.foo.test"),
        SRVRecord("_service._tcp.foo.test", Ttl.fromPositive(86400.seconds), 65533, 40, 65535, "a-double.foo.test"))
    }

    "resolve same address twice" in {
      resolve("a-single.foo.test").records.map(_.asInstanceOf[ARecord].ip) shouldEqual Seq(
        InetAddress.getByName("192.168.1.20"))
      resolve("a-single.foo.test").records.map(_.asInstanceOf[ARecord].ip) shouldEqual Seq(
        InetAddress.getByName("192.168.1.20"))
    }

    "handle nonexistent domains" in {
      val answer = (IO(Dns) ? DnsProtocol.Resolve("nonexistent.foo.test")).mapTo[DnsProtocol.Resolved].futureValue
      answer.records shouldEqual List.empty
    }

    "resolve queries that are too big for UDP" in {
      val name = "many.foo.test"
      val answer = resolve(name)
      answer.name shouldEqual name
      answer.records.length should be(48)
    }

    "resolve using search domains where some have not enough ndots" in {
      val name = "a-single"
      val expectedName = "a-single.foo.test"
      val answer = resolve(name, DnsProtocol.Ip(ipv6 = false))
      withClue(answer) {
        answer.name shouldEqual expectedName
        answer.records.size shouldEqual 1
        answer.records.head.name shouldEqual expectedName
        answer.records.head.asInstanceOf[ARecord].ip shouldEqual InetAddress.getByName("192.168.1.20")
      }
    }

    "resolve using search domains" in {
      val name = "a-single.foo"
      val expectedName = "a-single.foo.test"
      val answer = resolve(name, DnsProtocol.Ip(ipv6 = false))
      withClue(answer) {
        answer.name shouldEqual expectedName
        answer.records.size shouldEqual 1
        answer.records.head.name shouldEqual expectedName
        answer.records.head.asInstanceOf[ARecord].ip shouldEqual InetAddress.getByName("192.168.1.20")
      }
    }

    "resolve localhost even though ndots is greater than 0" in {
      val name = "localhost"
      val answer = resolve(name, DnsProtocol.Ip(ipv6 = false))
      withClue(answer) {
        answer.name shouldEqual "localhost"
        answer.records.size shouldEqual 1
        answer.records.head.name shouldEqual "localhost"
        answer.records.head.asInstanceOf[ARecord].ip shouldEqual InetAddress.getByName("127.0.0.1")
      }
    }

    def resolve(name: String, requestType: RequestType = Ip()): DnsProtocol.Resolved = {
      try {
        (IO(Dns) ? DnsProtocol.Resolve(name, requestType)).mapTo[DnsProtocol.Resolved].futureValue
      } catch {
        case e: Throwable =>
          dumpNameserverLogs()
          throw e
      }
    }

  }
}
