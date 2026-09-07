/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.nio.file.Files
import java.nio.file.StandardCopyOption

import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.event.MarkerLoggingAdapter
import akka.event.NoMarkerLogging
import akka.remote.artery.tcp.SslTransportException

/**
 * Lightweight unit test for [[RotatingKeysSSLEngineProvider]] construction, without
 * spinning up an ActorSystem or real network connections.
 */
class RotatingKeysSSLEngineProviderConstructionSpec extends AnyWordSpec with Matchers {

  /** Records warnings instead of publishing them, so a test can assert on what was logged. */
  private class RecordingLogging extends MarkerLoggingAdapter(null, "test", classOf[String], null) {
    val warnings: collection.mutable.Buffer[String] = collection.mutable.Buffer.empty
    override def isErrorEnabled = false
    override def isWarningEnabled = true
    override def isInfoEnabled = false
    override def isDebugEnabled = false
    override protected def notifyError(message: String): Unit = ()
    override protected def notifyError(cause: Throwable, message: String): Unit = ()
    override protected def notifyWarning(message: String): Unit = warnings += message
    override protected def notifyInfo(message: String): Unit = ()
    override protected def notifyDebug(message: String): Unit = ()
  }

  private def nameToPath(name: String): String = getClass.getClassLoader.getResource(name).getPath

  private def configFor(
      keyFile: String,
      certFile: String,
      caCertFile: String,
      cacheTtl: FiniteDuration = 5.minutes): Config =
    ConfigFactory.parseString(s"""
        key-file = "$keyFile"
        cert-file = "$certFile"
        ca-cert-file = "$caCertFile"
        ssl-context-cache-ttl = ${cacheTtl.toMillis}ms
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

    "warn when a ca-cert-file rebuild loads fewer CA certificates than before" in {
      // Simulates a ca-cert-file caught mid-rewrite: a rebuild that silently narrows the
      // trust set is exactly the failure this warning exists to surface, since it produces
      // no exception and, at default log levels, no other signal at all.
      val bundle = nameToPath("ssl/rotation-ca2/ca-bundle.crt")
      val realCa = nameToPath("ssl/exampleca.crt")
      val caCertFile = Files.createTempFile("shrinking-ca-cert-", ".crt")
      try {
        Files.copy(java.nio.file.Paths.get(bundle), caCertFile, StandardCopyOption.REPLACE_EXISTING)
        val config = configFor(
          nameToPath("ssl/node.example.com.pem"),
          nameToPath("ssl/node.example.com.crt"),
          caCertFile.toString,
          cacheTtl = 1.milli)
        val logging = new RecordingLogging
        val provider = new RotatingKeysSSLEngineProvider(config, logging)

        provider.getSSLContext()
        logging.warnings must be(empty)

        Thread.sleep(10)
        Files.copy(java.nio.file.Paths.get(realCa), caCertFile, StandardCopyOption.REPLACE_EXISTING)
        provider.getSSLContext()

        logging.warnings.exists(_.contains("fewer")) must be(true)
      } finally Files.deleteIfExists(caCertFile)
    }
  }
}
