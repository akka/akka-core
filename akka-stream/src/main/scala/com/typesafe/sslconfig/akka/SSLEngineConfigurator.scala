/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package com.typesafe.sslconfig.akka

import javax.net.ssl.{ SSLContext, SSLEngine }

import com.typesafe.sslconfig.ssl.SSLConfigSettings

/**
 * Gives the chance to configure the SSLContext before it is going to be used.
 * The passed in context will be already set in client mode and provided with hostInfo during initialization.
 */
@deprecated("Use Tcp and TLS with SSLEngine parameters instead. Setup the SSLEngine with needed parameters.", "2.6.0")
trait SSLEngineConfigurator {
  def configure(engine: SSLEngine, sslContext: SSLContext): SSLEngine
}

@deprecated("Use Tcp and TLS with SSLEngine parameters instead. Setup the SSLEngine with needed parameters.", "2.6.0")
final class DefaultSSLEngineConfigurator(
    config: SSLConfigSettings,
    enabledProtocols: Array[String],
    enabledCipherSuites: Array[String])
    extends SSLEngineConfigurator {
  config ne null // @unused unavailable
  def configure(engine: SSLEngine, sslContext: SSLContext): SSLEngine = {
    engine.setSSLParameters(sslContext.getDefaultSSLParameters)
    engine.setEnabledProtocols(enabledProtocols)
    engine.setEnabledCipherSuites(enabledCipherSuites)
    engine
  }
}
