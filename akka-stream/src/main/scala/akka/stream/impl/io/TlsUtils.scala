/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.impl.io

import javax.net.ssl.{ SSLEngine, SSLParameters }

import akka.annotation.InternalApi
import akka.stream.TLSClientAuth
import akka.stream.TLSProtocol.NegotiateNewSession

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TlsUtils {
  def applySessionParameters(engine: SSLEngine, sessionParameters: NegotiateNewSession): Unit = {
    sessionParameters.enabledCipherSuites.foreach(cs => engine.setEnabledCipherSuites(cs.toArray))
    sessionParameters.enabledProtocols.foreach(p => engine.setEnabledProtocols(p.toArray))

    sessionParameters.sslParameters.foreach(engine.setSSLParameters)

    sessionParameters.clientAuth match {
      case Some(TLSClientAuth.None) => engine.setNeedClientAuth(false)
      case Some(TLSClientAuth.Want) => engine.setWantClientAuth(true)
      case Some(TLSClientAuth.Need) => engine.setNeedClientAuth(true)
      case _                        => // do nothing
    }
  }

  def cloneParameters(old: SSLParameters): SSLParameters = {
    val newParameters = new SSLParameters()
    newParameters.setAlgorithmConstraints(old.getAlgorithmConstraints)
    newParameters.setCipherSuites(old.getCipherSuites)
    newParameters.setEndpointIdentificationAlgorithm(old.getEndpointIdentificationAlgorithm)
    newParameters.setNeedClientAuth(old.getNeedClientAuth)
    newParameters.setProtocols(old.getProtocols)
    newParameters.setServerNames(old.getServerNames)
    newParameters.setSNIMatchers(old.getSNIMatchers)
    newParameters.setUseCipherSuitesOrder(old.getUseCipherSuitesOrder)
    newParameters.setWantClientAuth(old.getWantClientAuth)
    newParameters
  }
}
