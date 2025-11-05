/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.pki.pem

import java.util.Base64

import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PEMDecoderSpec extends AnyWordSpec with Matchers with EitherValues {

  private val cert =
    """-----BEGIN CERTIFICATE-----
      |MIIDCzCCAfOgAwIBAgIQfEHPfR1p1xuW9TQlfxAugjANBgkqhkiG9w0BAQsFADAv
      |MS0wKwYDVQQDEyQwZDIwN2I2OC05YTIwLTRlZTgtOTJjYi1iZjk2OTk1ODFjZjgw
      |HhcNMTkxMDExMTMyODUzWhcNMjQxMDA5MTQyODUzWjAvMS0wKwYDVQQDEyQwZDIw
      |N2I2OC05YTIwLTRlZTgtOTJjYi1iZjk2OTk1ODFjZjgwggEiMA0GCSqGSIb3DQEB
      |AQUAA4IBDwAwggEKAoIBAQDhD0BxlDzEOzcp45lPHL60lnM6k3puEGb2lKHL5/nR
      |F94FCnZL0FH8EdxWzzAYgys+kUwSdo4QMuWuvjY2Km4Wob6k4uAeYEFTCfBdi4/z
      |r4kpWzu8xLz+uZWimLQrjqVytNNK3DMv6ebWUJ/92VTDS4yzWk4YV0MVr2b2OgMK
      |SgMvaFQ8L/CwyML72PBWIqU67+MMvvcTLxQdyEgQTTjP0bbiXMLDvfZDarLJojsW
      |SNBz7AIkznhGkzIGGdhAa41PnPu9XaBFhaqx9Qe3+MG2/k1l/46eHtmxCqhOUde1
      |i0vy6ZfgcGifua1tg1UBI/oT4S0dsq24dq7K1MYLyHTrAgMBAAGjIzAhMA4GA1Ud
      |DwEB/wQEAwICBDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQAa
      |5YOlvob4wqps3HLaOIi7VzDihNHciP+BI0mzxHa7D3bGaecRPSeG3xEoD/Uxs9o4
      |8cByPpYx1Wl8LLRx14wDcK0H+UPpo4gCI6h6q92cJj0YRjcSUUt8EIu3qnFMjtM+
      |sl/uc21fGlq6cvMRZXqtVYENoSNTDHi5a5eEXRa2eZ8XntjvOanOhIKWmxvr8r4a
      |Voz4WdnXx1C8/BzB62UBoMu4QqMGMLk5wXP0D6hECUuespMest+BeoJAVhTq7wZs
      |rSP9q08n1stZFF4+bEBaxcqIqdhOLQdHcYELN+a5v0Mcwdsy7jJMagmNPfsKoOKC
      |hLOsmNYKHdmWg37Jib5o
      |-----END CERTIFICATE-----""".stripMargin

  "The PEM decoder" should {
    "decode a real world certificate" in {
      PEMDecoder.decode(cert).label should ===("CERTIFICATE")
    }

    "decode data with no spaces" in {
      val result = PEMDecoder.decode(
        "-----BEGIN CERTIFICATE-----" + Base64.getEncoder
          .encodeToString("abc".getBytes()) + "-----END CERTIFICATE-----")
      result.label should ===("CERTIFICATE")
      new String(result.bytes) should ===("abc")
    }

    "decode data with whatever label" in {
      val result = PEMDecoder.decode(
        "-----BEGIN FOO-----" + Base64.getEncoder.encodeToString("abc".getBytes()) + "-----END FOO-----")
      result.label should ===("FOO")
      new String(result.bytes) should ===("abc")
    }

    "decode data with lots of spaces" in {
      val result = PEMDecoder.decode(
        "\n \t \r -----BEGIN CERTIFICATE-----\n" +
        Base64.getEncoder
          .encodeToString("abc".getBytes())
          .flatMap(c => s"$c\n\r  \t\n") + "-----END CERTIFICATE-----\n \t \r ")
      result.label should ===("CERTIFICATE")
      new String(result.bytes) should ===("abc")
    }

    "decode data with two padding characters" in {
      // A 4 byte input results in a 6 character output with 2 padding characters
      val result = PEMDecoder.decode(
        "-----BEGIN CERTIFICATE-----" + Base64.getEncoder
          .encodeToString("abcd".getBytes()) + "-----END CERTIFICATE-----")
      result.label should ===("CERTIFICATE")
      new String(result.bytes) should ===("abcd")
    }

    "decode data with one padding character" in {
      // A 5 byte input results in a 7 character output with 1 padding character1
      val result = PEMDecoder.decode(
        "-----BEGIN CERTIFICATE-----" + Base64.getEncoder
          .encodeToString("abcde".getBytes()) + "-----END CERTIFICATE-----")
      result.label should ===("CERTIFICATE")
      new String(result.bytes) should ===("abcde")
    }

    "fail decode when the format is wrong (not MIME BASE64, lines too long)" in {
      val input = """-----BEGIN CERTIFICATE-----
        |MIIDCzCCAfOgAwIBAgIQfEHPfR1p1xuW9TQlfxAugjANBgkqhkiG9w0BAQsFADAviZjk2OTk1ODFjZjgw
        |HhcNMTkxMDExMTMyODUzWhcNMjQxMDA5MTQyODUzWjAvMS0wKwYDVQQDEyQwZDIwhLOsmNYKHdmWg37Jib5o
        |-----END CERTIFICATE-----""".stripMargin

      assertThrows[PEMLoadingException] {
        PEMDecoder.decode(input)
      }
    }

    "fail decode when the format is wrong (not PEM, invalid per/post-EB)" in {
      val input = cert.replace("BEGIN", "BGN").replace("END ", "GLGLGL ")

      assertThrows[PEMLoadingException] {
        PEMDecoder.decode(input)
      }
    }

  }

}
