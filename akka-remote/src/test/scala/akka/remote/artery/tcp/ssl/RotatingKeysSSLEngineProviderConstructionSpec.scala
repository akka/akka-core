/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.nio.file.Files

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.event.NoMarkerLogging
import akka.remote.artery.tcp.SslTransportException
import akka.remote.artery.tcp.ssl.TestResources.nameToPath

/**
 * Lightweight unit test for [[RotatingKeysSSLEngineProvider]] construction, without
 * spinning up an ActorSystem or real network connections.
 */
class RotatingKeysSSLEngineProviderConstructionSpec extends AnyWordSpec with Matchers {

  private def configFor(keyFile: String, certFile: String, caCertFile: String): Config =
    ConfigFactory.parseString(s"""
        key-file = "$keyFile"
        cert-file = "$certFile"
        ca-cert-file = "$caCertFile"
        """).withFallback(ConfigFactory.load().getConfig("akka.remote.artery.ssl.rotating-keys-engine"))

  "RotatingKeysSSLEngineProvider" must {

    "fail fast when ca-cert-file is empty" in {
      val emptyCaCertFile = Files.createTempFile("empty-ca-cert-", ".crt")
      try {
        val config = configFor(
          nameToPath("ssl/node.example.com.pem"),
          nameToPath("ssl/node.example.com.crt"),
          emptyCaCertFile.toString)
        val provider = new RotatingKeysSSLEngineProvider(config, NoMarkerLogging)

        // An empty or truncated ca-cert-file otherwise loads as zero certificates without
        // error, the SSLContext gets cached with no trust anchors for the full
        // ssl-context-cache-ttl, and every handshake then fails later with an opaque
        // InvalidAlgorithmParameterException that points nowhere near the actual cause.
        intercept[SslTransportException] {
          provider.getSSLContext()
        }
      } finally Files.deleteIfExists(emptyCaCertFile)
    }

    "wrap a malformed ca-cert-file in SslTransportException instead of letting it escape raw" in {
      val garbageCaCertFile = Files.createTempFile("garbage-ca-cert-", ".crt")
      try {
        Files.write(garbageCaCertFile, "not a certificate".getBytes("UTF-8"))
        val config = configFor(
          nameToPath("ssl/node.example.com.pem"),
          nameToPath("ssl/node.example.com.crt"),
          garbageCaCertFile.toString)
        val provider = new RotatingKeysSSLEngineProvider(config, NoMarkerLogging)

        // CertificateException extends GeneralSecurityException, not IOException, so it isn't
        // caught by readFiles()'s own catch clauses; it must still come out as
        // SslTransportException like every other malformed-input case, not raw.
        intercept[SslTransportException] {
          provider.getSSLContext()
        }
      } finally Files.deleteIfExists(garbageCaCertFile)
    }
  }
}
