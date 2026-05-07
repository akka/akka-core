/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.discovery

import akka.testkit.AkkaSpec
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.Future

object DnsDiscoveryDocSpec {
  val config = ConfigFactory.parseString("""
    // #configure-dns
    akka {
      discovery {
        method = akka-dns
      }
    }
    // #configure-dns
    """)
}

class DnsDiscoveryDocSpec extends AkkaSpec(DnsDiscoveryDocSpec.config) {

  "DNS Discovery" should {
    "find akka.io" in {
      // #lookup-dns
      import akka.discovery.Discovery
      import akka.discovery.ServiceDiscovery

      val discovery: ServiceDiscovery = Discovery(system).discovery
      // ...
      val result: Future[ServiceDiscovery.Resolved] = discovery.lookup("akka.io", resolveTimeout = 3.seconds)
      // #lookup-dns

      try {
        val resolved = result.futureValue
        resolved.serviceName shouldBe "akka.io"
        resolved.addresses shouldNot be(Symbol("empty"))
      } catch {
        case e: Exception =>
          info("Failed lookup akka.io, but ignoring: " + e)
          pending
      }
    }
  }

}
