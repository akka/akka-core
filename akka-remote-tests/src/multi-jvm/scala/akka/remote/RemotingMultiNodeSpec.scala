/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import com.typesafe.config.ConfigFactory
import org.scalatest.Suite

import akka.remote.artery.ArterySpecSupport
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec, STMultiNodeSpec }
import akka.testkit.{ DefaultTimeout, ImplicitSender }

object RemotingMultiNodeSpec {

  def commonConfig =
    ConfigFactory.parseString("""
        akka.actor.warn-about-java-serializer-usage = off
      """).withFallback(ArterySpecSupport.tlsConfig) // TLS only used if transport=tls-tcp

}

abstract class RemotingMultiNodeSpec(config: MultiNodeConfig)
    extends MultiNodeSpec(config)
    with Suite
    with STMultiNodeSpec
    with ImplicitSender
    with DefaultTimeout { self: MultiNodeSpec =>

}
