/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery
package tcp

import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLSession

import akka.actor.ExtendedActorSystem
import akka.actor.setup.Setup

trait SSLEngineProvider {

  def createServerSSLEngine(hostname: String, port: Int): SSLEngine

  def createClientSSLEngine(hostname: String, port: Int): SSLEngine

  /**
   * Verification that will be called after every successful handshake
   * to verify additional session information. Return `None` if valid
   * otherwise `Some` with explaining cause.
   */
  def verifyClientSession(hostname: String, session: SSLSession): Option[Throwable]

  /**
   * Verification that will be called after every successful handshake
   * to verify additional session information. Return `None` if valid
   * otherwise `Some` with explaining cause.
   */
  def verifyServerSession(hostname: String, session: SSLSession): Option[Throwable]

}

class SslTransportException(message: String, cause: Throwable) extends RuntimeException(message, cause)

object SSLEngineProviderSetup {

  /**
   * Scala API: factory for defining a `SSLEngineProvider` that is passed in when ActorSystem
   * is created rather than creating one from configured class name.
   */
  def apply(sslEngineProvider: ExtendedActorSystem => SSLEngineProvider): SSLEngineProviderSetup =
    new SSLEngineProviderSetup(sslEngineProvider)

  /**
   * Java API: factory for defining a `SSLEngineProvider` that is passed in when ActorSystem
   * is created rather than creating one from configured class name.
   */
  def create(
      sslEngineProvider: java.util.function.Function[ExtendedActorSystem, SSLEngineProvider]): SSLEngineProviderSetup =
    apply(sys => sslEngineProvider(sys))

}

/**
 * Setup for defining a `SSLEngineProvider` that is passed in when ActorSystem
 * is created rather than creating one from configured class name. That is useful
 * when the SSLEngineProvider implementation require other external constructor parameters
 * or is created before the ActorSystem is created.
 *
 * Constructor is *Internal API*, use factories in [[SSLEngineProviderSetup]]
 */
class SSLEngineProviderSetup private (val sslEngineProvider: ExtendedActorSystem => SSLEngineProvider) extends Setup
