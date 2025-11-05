/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import com.typesafe.config.{ Config, ConfigFactory }

import akka.actor.{ ActorSystem, Address, BootstrapSetup, RootActorPath }
import akka.actor.setup.ActorSystemSetup
import akka.remote.RARP
import akka.testkit.{ AkkaSpec, SocketUtil }

/**
 * Base class for remoting tests what needs to test interaction between a "local" actor system
 * which is always created (the usual AkkaSpec system), and multiple additional actor systems over artery
 */
abstract class ArteryMultiNodeSpec(config: Config)
    extends AkkaSpec(config.withFallback(ArterySpecSupport.defaultConfig)) {

  def this() = this(ConfigFactory.empty())
  def this(extraConfig: String) = this(ConfigFactory.parseString(extraConfig))

  /** just an alias to make tests more readable */
  def localSystem = system
  def localPort = port(localSystem)
  def port(system: ActorSystem): Int = RARP(system).provider.getDefaultAddress.port.get
  def address(sys: ActorSystem): Address = RARP(sys).provider.getDefaultAddress
  def rootActorPath(sys: ActorSystem) = RootActorPath(address(sys))
  def nextGeneratedSystemName = s"${localSystem.name}-remote-${remoteSystems.size}"
  def freePort(): Int = {
    val udp = ArteryMultiNodeSpec.arteryUdpEnabled(system.settings.config)
    (address(system).host match {
      case Some(host) => SocketUtil.temporaryServerAddress(host, udp)
      case None       => SocketUtil.temporaryServerAddress(udp = udp)
    }).getPort
  }

  private var remoteSystems: Vector[ActorSystem] = Vector.empty

  /**
   * @return A new actor system configured with artery enabled. The system will
   *         automatically be terminated after test is completed to avoid leaks.
   */
  def newRemoteSystem(
      extraConfig: Option[String] = None,
      name: Option[String] = None,
      setup: Option[ActorSystemSetup] = None): ActorSystem = {
    val config =
      extraConfig.fold(localSystem.settings.config)(str =>
        ConfigFactory.parseString(str).withFallback(localSystem.settings.config))
    val sysName = name.getOrElse(nextGeneratedSystemName)

    val remoteSystem = setup match {
      case None    => ActorSystem(sysName, config)
      case Some(s) => ActorSystem(sysName, s.and(BootstrapSetup.apply(config)))
    }

    remoteSystems = remoteSystems :+ remoteSystem

    remoteSystem
  }

  override def afterTermination(): Unit = {
    remoteSystems.foreach(sys => shutdown(sys))
    remoteSystems = Vector.empty
    super.afterTermination()
  }

  def arteryTcpTlsEnabled(system: ActorSystem = system): Boolean = {
    val arterySettings = ArterySettings(system.settings.config.getConfig("akka.remote.artery"))
    arterySettings.Enabled && arterySettings.Transport == ArterySettings.TlsTcp
  }
}

object ArteryMultiNodeSpec {
  def arteryUdpEnabled(systemConfig: Config): Boolean = {
    val arterySettings = ArterySettings(systemConfig.getConfig("akka.remote.artery"))
    arterySettings.Transport == ArterySettings.AeronUpd
  }

  def freePort(systemConfig: Config): Int = {
    SocketUtil.temporaryLocalPort(ArteryMultiNodeSpec.arteryUdpEnabled(systemConfig))
  }
}
