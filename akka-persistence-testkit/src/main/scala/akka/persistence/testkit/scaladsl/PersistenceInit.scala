/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import java.util.UUID

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.persistence.testkit.internal.PersistenceInitImpl
import akka.util.Timeout

/**
 * Test utility to initialize persistence plugins. Useful when initialization order or coordination
 * is needed. For example to avoid creating tables concurrently.
 */
object PersistenceInit {

  /**
   * Initialize the default journal and snapshot plugins.
   *
   * @return a `Future` that is completed when the initialization has completed
   */
  def initializeDefaultPlugins(system: ClassicActorSystemProvider, timeout: FiniteDuration): Future[Done] =
    initializePlugins(system, journalPluginId = "", snapshotPluginId = "", timeout)

  /**
   * Initialize the given journal and snapshot plugins.
   *
   * The `snapshotPluginId` can be empty (`""`) if snapshot plugin isn't used.
   *
   * @return a `Future` that is completed when the initialization has completed
   */
  def initializePlugins(
      system: ClassicActorSystemProvider,
      journalPluginId: String,
      snapshotPluginId: String,
      timeout: FiniteDuration): Future[Done] = {
    val persistenceId: String = s"persistenceInit-${UUID.randomUUID()}"
    val extSystem = system.classicSystem.asInstanceOf[ExtendedActorSystem]
    val ref =
      extSystem.systemActorOf(
        PersistenceInitImpl.props(journalPluginId, snapshotPluginId, persistenceId),
        persistenceId)
    import extSystem.dispatcher

    import akka.pattern.ask
    implicit val askTimeout: Timeout = timeout
    (ref ? "start").map(_ => Done)
  }
}
