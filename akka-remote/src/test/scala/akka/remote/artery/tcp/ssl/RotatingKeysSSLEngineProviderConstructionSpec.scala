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

/**
 * Lightweight unit test for [[RotatingKeysSSLEngineProvider]] construction, without
 * spinning up an ActorSystem or real network connections.
 */
class RotatingKeysSSLEngineProviderConstructionSpec extends AnyWordSpec with Matchers {

  private def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath

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

        // Before the fix: this succeeds, silently caching an SSLContext with zero trust
        // anchors for the full ssl-context-cache-ttl. Every handshake using it then fails
        // later with an opaque InvalidAlgorithmParameterException that points nowhere near
        // the actual cause (the empty ca-cert-file).
        intercept[SslTransportException] {
          provider.getSSLContext()
        }
      } finally Files.deleteIfExists(emptyCaCertFile)
    }
  }
}
