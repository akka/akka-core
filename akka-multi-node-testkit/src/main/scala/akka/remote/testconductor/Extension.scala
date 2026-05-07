/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.testconductor

import com.typesafe.config.Config

import akka.actor.{ ActorContext, ActorSystem, ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import akka.actor.ClassicActorSystemProvider
import akka.dispatch.ThreadPoolConfig
import akka.remote.RemoteActorRefProvider
import akka.util.Timeout

/**
 * Access to the [[akka.remote.testconductor.TestConductorExt]] extension:
 *
 * {{{
 * val tc = TestConductor(system)
 * tc.startController(numPlayers)
 * // OR
 * tc.startClient(conductorPort)
 * }}}
 */
object TestConductor extends ExtensionId[TestConductorExt] with ExtensionIdProvider {

  override def lookup = TestConductor

  override def createExtension(system: ExtendedActorSystem): TestConductorExt = new TestConductorExt(system)

  /**
   * Java API: retrieve the TestConductor extension for the given system.
   */
  override def get(system: ActorSystem): TestConductorExt = super.get(system)
  override def get(system: ClassicActorSystemProvider): TestConductorExt = super.get(system)

  def apply()(implicit ctx: ActorContext): TestConductorExt = apply(ctx.system)

}

/**
 * This binds together the [[akka.remote.testconductor.Conductor]] and
 * [[akka.remote.testconductor.Player]] roles inside an Akka
 * [[akka.actor.Extension]]. Please follow the aforementioned links for
 * more information.
 *
 * ====Note====
 * This extension requires the `akka.actor.provider`
 * to be a [[akka.remote.RemoteActorRefProvider]].
 *
 * To use ``blackhole``, ``passThrough``, and ``throttle`` you must activate the
 * failure injector and throttler transport adapters by specifying `testTransport(on = true)`
 * in your MultiNodeConfig.
 *
 */
class TestConductorExt(val system: ExtendedActorSystem) extends Extension with Conductor with Player {

  object Settings {
    val config = system.settings.config.getConfig("akka.testconductor")
    import akka.util.Helpers.ConfigOps

    val ConnectTimeout = config.getMillisDuration("connect-timeout")
    val ClientReconnects = config.getInt("client-reconnects")
    val ReconnectBackoff = config.getMillisDuration("reconnect-backoff")

    implicit val BarrierTimeout: Timeout = Timeout(config.getMillisDuration("barrier-timeout"))
    implicit val QueryTimeout: Timeout = Timeout(config.getMillisDuration("query-timeout"))
    val PacketSplitThreshold = config.getMillisDuration("packet-split-threshold")

    private def computeWPS(config: Config): Int =
      ThreadPoolConfig.scaledPoolSize(
        config.getInt("pool-size-min"),
        config.getDouble("pool-size-factor"),
        config.getInt("pool-size-max"))

    val ServerSocketWorkerPoolSize = computeWPS(config.getConfig("netty.server-socket-worker-pool"))

    val ClientSocketWorkerPoolSize = computeWPS(config.getConfig("netty.client-socket-worker-pool"))
  }

  /**
   * Remote transport used by the actor ref provider.
   */
  val transport = system.provider.asInstanceOf[RemoteActorRefProvider].transport

  /**
   * Transport address of this Netty-like remote transport.
   */
  val address = transport.defaultAddress

}
