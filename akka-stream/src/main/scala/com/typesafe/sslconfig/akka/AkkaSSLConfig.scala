/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package com.typesafe.sslconfig.akka

import java.util.Collections
import javax.net.ssl._

import scala.annotation.nowarn

import com.typesafe.sslconfig.akka.util.AkkaLoggerFactory
import com.typesafe.sslconfig.ssl._
import com.typesafe.sslconfig.util.LoggerFactory

import akka.actor._
import akka.annotation.InternalApi
import akka.event.Logging

@deprecated("Use Tcp and TLS with SSLEngine parameters instead. Setup the SSLEngine with needed parameters.", "2.6.0")
object AkkaSSLConfig extends ExtensionId[AkkaSSLConfig] with ExtensionIdProvider {

  //////////////////// EXTENSION SETUP ///////////////////

  override def get(system: ActorSystem): AkkaSSLConfig = super.get(system)
  override def get(system: ClassicActorSystemProvider): AkkaSSLConfig = super.get(system)
  def apply()(implicit system: ActorSystem): AkkaSSLConfig = super.apply(system)

  override def lookup = AkkaSSLConfig

  override def createExtension(system: ExtendedActorSystem): AkkaSSLConfig =
    new AkkaSSLConfig(system, defaultSSLConfigSettings(system))

  def defaultSSLConfigSettings(system: ActorSystem): SSLConfigSettings = {
    val akkaOverrides = system.settings.config.getConfig("akka.ssl-config")
    val defaults = system.settings.config.getConfig("ssl-config")
    SSLConfigFactory.parse(akkaOverrides.withFallback(defaults))
  }

}

@deprecated("Use Tcp and TLS with SSLEngine parameters instead. Setup the SSLEngine with needed parameters.", "2.6.0")
final class AkkaSSLConfig(system: ExtendedActorSystem, val config: SSLConfigSettings) extends Extension {

  private val mkLogger = new AkkaLoggerFactory(system)

  private val log = Logging(system, classOf[AkkaSSLConfig])
  log.debug("Initializing AkkaSSLConfig extension...")

  /** Can be used to modify the underlying config, most typically used to change a few values in the default config */
  def withSettings(c: SSLConfigSettings): AkkaSSLConfig =
    new AkkaSSLConfig(system, c)

  /**
   * Returns a new [[AkkaSSLConfig]] instance with the settings changed by the given function.
   * Please note that the ActorSystem-wide extension always remains configured via typesafe config,
   * custom ones can be created for special-handling specific connections
   */
  def mapSettings(f: SSLConfigSettings => SSLConfigSettings): AkkaSSLConfig =
    new AkkaSSLConfig(system, f(config))

  /**
   * Returns a new [[AkkaSSLConfig]] instance with the settings changed by the given function.
   * Please note that the ActorSystem-wide extension always remains configured via typesafe config,
   * custom ones can be created for special-handling specific connections
   *
   * Java API
   */
  // Not same signature as mapSettings to allow latter deprecation of this once we hit Scala 2.12
  def convertSettings(f: java.util.function.Function[SSLConfigSettings, SSLConfigSettings]): AkkaSSLConfig =
    new AkkaSSLConfig(system, f.apply(config))

  val hostnameVerifier = buildHostnameVerifier(config)

  /**
   * INTERNAL API
   */
  @InternalApi def useJvmHostnameVerification: Boolean =
    hostnameVerifier match {
      case _: DefaultHostnameVerifier | _: NoopHostnameVerifier => true
      case _                                                    => false
    }

  val sslEngineConfigurator = {
    val sslContext = if (config.default) {
      log.info("ssl-config.default is true, using the JDK's default SSLContext")
      SSLContext.getDefault
    } else {
      // break out the static methods as much as we can...
      val keyManagerFactory = buildKeyManagerFactory(config)
      val trustManagerFactory = buildTrustManagerFactory(config)
      new ConfigSSLContextBuilder(mkLogger, config, keyManagerFactory, trustManagerFactory).build()
    }

    // protocols!
    val defaultParams = sslContext.getDefaultSSLParameters
    val defaultProtocols = defaultParams.getProtocols
    val protocols = configureProtocols(defaultProtocols, config)

    // ciphers!
    val defaultCiphers = defaultParams.getCipherSuites
    val cipherSuites = configureCipherSuites(defaultCiphers, config)

    // apply "loose" settings
    // !! SNI!
    looseDisableSNI(defaultParams)

    new DefaultSSLEngineConfigurator(config, protocols, cipherSuites)
  }

  ////////////////// CONFIGURING //////////////////////

  def buildKeyManagerFactory(ssl: SSLConfigSettings): KeyManagerFactoryWrapper = {
    val keyManagerAlgorithm = ssl.keyManagerConfig.algorithm
    new DefaultKeyManagerFactoryWrapper(keyManagerAlgorithm)
  }

  def buildTrustManagerFactory(ssl: SSLConfigSettings): TrustManagerFactoryWrapper = {
    val trustManagerAlgorithm = ssl.trustManagerConfig.algorithm
    new DefaultTrustManagerFactoryWrapper(trustManagerAlgorithm)
  }

  def buildHostnameVerifier(conf: SSLConfigSettings): HostnameVerifier = {
    conf ne null // @unused unavailable
    val clazz: Class[HostnameVerifier] =
      if (config.loose.disableHostnameVerification)
        classOf[DisabledComplainingHostnameVerifier].asInstanceOf[Class[HostnameVerifier]]
      else config.hostnameVerifierClass.asInstanceOf[Class[HostnameVerifier]]

    val v = system.dynamicAccess
      .createInstanceFor[HostnameVerifier](clazz, Nil)
      .orElse(system.dynamicAccess.createInstanceFor[HostnameVerifier](clazz, List(classOf[LoggerFactory] -> mkLogger)))
      .getOrElse(throw new Exception("Unable to obtain hostname verifier for class: " + clazz))

    log.debug("buildHostnameVerifier: created hostname verifier: {}", v)
    v
  }

  def validateDefaultTrustManager(@nowarn("msg=never used") sslConfig: SSLConfigSettings): Unit = {
    log.warning(
      "validateDefaultTrustManager is not doing anything since akka 2.6.19, it was useful only in Java 7 and below");
  }

  def configureProtocols(existingProtocols: Array[String], sslConfig: SSLConfigSettings): Array[String] = {
    val definedProtocols = sslConfig.enabledProtocols match {
      case Some(configuredProtocols) =>
        // If we are given a specific list of protocols, then return it in exactly that order,
        // assuming that it's actually possible in the SSL context.
        configuredProtocols.filter(existingProtocols.contains).toArray

      case None =>
        // Otherwise, we return the default protocols in the given list.
        Protocols.recommendedProtocols.filter(existingProtocols.contains)
    }

    definedProtocols
  }

  def configureCipherSuites(existingCiphers: Array[String], sslConfig: SSLConfigSettings): Array[String] = {
    val definedCiphers = sslConfig.enabledCipherSuites match {
      case Some(configuredCiphers) =>
        // If we are given a specific list of ciphers, return it in that order.
        configuredCiphers.filter(existingCiphers.contains(_)).toArray

      case None =>
        existingCiphers
    }

    definedCiphers
  }

  // LOOSE SETTINGS //

  private def looseDisableSNI(defaultParams: SSLParameters): Unit = if (config.loose.disableSNI) {
    // this will be logged once for each AkkaSSLConfig
    log.warning(
      "You are using ssl-config.loose.disableSNI=true! " +
      "It is strongly discouraged to disable Server Name Indication, as it is crucial to preventing man-in-the-middle attacks.")

    defaultParams.setServerNames(Collections.emptyList())
    defaultParams.setSNIMatchers(Collections.emptyList())
  }

}
