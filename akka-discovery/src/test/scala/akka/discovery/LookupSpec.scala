/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.discovery

import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LookupSpec extends AnyWordSpec with Matchers with OptionValues {

  val (srvWithInvalidDomainNames, srvWithValidDomainNames) = {

    val portnameAndProtocol = "_portName._protocol."
    val char10 = "abcdefghij"
    val char63 = (char10 * 6) + "abc"
    val char64 = char63 + "d"

    val invalidDomainNames = Seq(
      portnameAndProtocol + "1" + char10,
      portnameAndProtocol + "." + char10,
      portnameAndProtocol + char10 + ".",
      portnameAndProtocol + "-" + char10,
      portnameAndProtocol + char10 + "_" + char10,
      portnameAndProtocol + char10 + "#" + char10,
      portnameAndProtocol + char10 + "$" + char10,
      portnameAndProtocol + char10 + "-",
      portnameAndProtocol + char10 + "." + char64,
      portnameAndProtocol + char64 + "." + char10)

    val validDomainNames = Seq(
      portnameAndProtocol + char10 + "." + char10,
      portnameAndProtocol + char10 + "-" + char10,
      portnameAndProtocol + char10 + "." + char63,
      portnameAndProtocol + char63 + "." + char10,
      portnameAndProtocol + char63 + "." + char63 + "." + char63)

    (invalidDomainNames, validDomainNames)
  }

  // No SRV that should result in simple A/AAAA lookups
  val noSrvLookups = List(
    "portName.protocol.serviceName.local",
    "serviceName.local",
    "_portName.serviceName",
    "_serviceName.local",
    "_serviceName,local",
    "-serviceName.local",
    "serviceName.local-")

  "Lookup.parseSrv" should {

    "extract service name (domain name) from a valid SRV String" in {
      val name = "_portName._protocol.serviceName.local"
      val lookup = Lookup.parseSrv(name)
      lookup.serviceName shouldBe "serviceName.local"
    }

    "generate a SRV Lookup from a valid SRV String" in {
      srvWithValidDomainNames.foreach { str =>
        withClue(s"parsing '$str'") {
          val lookup = Lookup.parseSrv(str)
          lookup.portName.value shouldBe "portName"
          lookup.protocol.value shouldBe "protocol"
        }
      }
    }

    "throw an IllegalArgumentException for any non-conforming SRV String" in {
      noSrvLookups.foreach { str =>
        withClue(s"parsing '$str'") {
          assertThrows[IllegalArgumentException] {
            Lookup.parseSrv(str)
          }
        }
      }
    }

    "throw an IllegalArgumentException for any SRV with invalid domain names" in {
      srvWithInvalidDomainNames.foreach { str =>
        withClue(s"parsing '$str'") {
          assertThrows[IllegalArgumentException] {
            Lookup.parseSrv(str)
          }
        }
      }
    }

    "throw an IllegalArgumentException when passing a 'null' SRV String" in {
      assertThrows[NullPointerException] {
        Lookup.parseSrv(null)
      }
    }

    "throw an IllegalArgumentException when passing an empty SRV String" in {
      assertThrows[IllegalArgumentException] {
        Lookup.parseSrv("")
      }
    }
  }

  "Lookup.isValidSrv" should {

    "return false for any non-conforming SRV String" in {
      noSrvLookups.foreach { str =>
        withClue(s"checking '$str'") {
          Lookup.isValidSrv(str) shouldBe false
        }
      }
    }

    "return false if domain name part in SRV String is an invalid domain name" in {
      srvWithInvalidDomainNames.foreach { str =>
        withClue(s"checking '$str'") {
          Lookup.isValidSrv(str) shouldBe false
        }
      }
    }

    "return true for any valid SRV String" in {
      srvWithValidDomainNames.foreach { str =>
        withClue(s"parsing '$str'") {
          Lookup.isValidSrv(str) shouldBe true
        }
      }
    }

    "return false for empty SRV String" in {
      Lookup.isValidSrv("") shouldBe false
    }

    "return false for 'null' SRV String" in {
      Lookup.isValidSrv(null) shouldBe false
    }

  }
}
