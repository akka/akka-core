/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import scala.collection.immutable.Seq

import akka.io.Dns
import akka.io.dns.AAAARecord
import akka.io.dns.CachePolicy.Ttl
import akka.io.dns.DnsProtocol.{ Resolve, Resolved }
import akka.testkit.{ AkkaSpec, ImplicitSender }
import akka.testkit.WithLogCapturing

class AsyncDnsManagerSpec extends AkkaSpec("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.dns.resolver = async-dns
    akka.io.dns.async-dns.nameservers = default
  """) with ImplicitSender with WithLogCapturing {

  val dns = Dns(system).manager

  "Async DNS Manager" must {
    "support ipv6" in {
      dns ! Resolve("::1") // ::1 will short circuit the resolution
      expectMsgType[Resolved] match {
        case Resolved("::1", Seq(AAAARecord("::1", Ttl.effectivelyForever, _)), Nil) =>
        case other                                                                   => fail(other.toString)
      }
    }

    "provide access to cache" in {
      dns ! AsyncDnsManager.GetCache
      ((expectMsgType[akka.io.SimpleDnsCache]: akka.io.SimpleDnsCache) should be).theSameInstanceAs(Dns(system).cache)
    }
  }

}
