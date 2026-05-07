/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.tcp.ssl

import java.security.NoSuchAlgorithmException
import javax.net.ssl.SSLEngine

import scala.util.Try

import com.typesafe.config.Config

import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.event.NoMarkerLogging
import akka.remote.artery.tcp.SecureRandomFactory

object CipherSuiteSupportCheck {

  /**
   * Given a `configPath` runs a set of validations over the default protocols,
   * cipher suites, PRNG algorithm/provider, etc...
   */
  def isSupported(system: ActorSystem, configPath: String): Try[Unit] = Try {
    val config = system.settings.config
    val subConfig = config.getConfig(configPath)

    isPrngSupported(subConfig)
    val engine: SSLEngine = buildSslEngine(system)
    val sslFactoryConfig = new SSLEngineConfig(subConfig)

    areAlgorithmsSupported(sslFactoryConfig, engine)
    isProtocolSupported(sslFactoryConfig, engine)
  }

  private def isPrngSupported(config: Config): Unit = {
    val rng = SecureRandomFactory.createSecureRandom(config, NoMarkerLogging)
    rng.nextInt() // Has to work
    val setting = SecureRandomFactory.rngConfig(config)
    if (rng.getAlgorithm != setting && setting != "")
      throw new NoSuchAlgorithmException(setting)
  }

  private def areAlgorithmsSupported(sslFactoryConfig: SSLEngineConfig, engine: SSLEngine) = {
    import sslFactoryConfig._
    val gotAllSupported = SSLEnabledAlgorithms.diff(engine.getSupportedCipherSuites.toSet)
    val gotAllEnabled = SSLEnabledAlgorithms.diff(engine.getEnabledCipherSuites.toSet)
    if (gotAllSupported.nonEmpty) throw new IllegalArgumentException("Cipher Suite not supported: " + gotAllSupported)
    if (gotAllEnabled.nonEmpty) throw new IllegalArgumentException("Cipher Suite not enabled: " + gotAllEnabled)
  }

  private def isProtocolSupported(sslFactoryConfig: SSLEngineConfig, engine: SSLEngine) = {
    import sslFactoryConfig._
    if (!engine.getSupportedProtocols.contains(SSLProtocol))
      throw new IllegalArgumentException("Protocol not supported: " + SSLProtocol)
  }

  private def buildSslEngine(system: ActorSystem): SSLEngine = {
    val address = system.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress
    val host = address.host.get
    val port = address.port.get
    val provider = new akka.remote.artery.tcp.ConfigSSLEngineProvider(system)
    val engine = provider.createServerSSLEngine(host, port)
    engine
  }

}
