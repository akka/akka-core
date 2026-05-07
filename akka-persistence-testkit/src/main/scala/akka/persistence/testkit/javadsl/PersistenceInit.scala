/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import java.time.Duration
import java.util.concurrent.CompletionStage

import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.persistence.testkit.scaladsl

/**
 * Test utility to initialize persistence plugins. Useful when initialization order or coordination
 * is needed. For example to avoid creating tables concurrently.
 */
object PersistenceInit {

  /**
   * Initialize the default journal and snapshot plugins.
   *
   * @return a `CompletionStage` that is completed when the initialization has completed
   */
  def initializeDefaultPlugins(system: ClassicActorSystemProvider, timeout: Duration): CompletionStage[Done] =
    initializePlugins(system, journalPluginId = "", snapshotPluginId = "", timeout)

  /**
   * Initialize the given journal and snapshot plugins.
   *
   * The `snapshotPluginId` can be empty (`""`) if snapshot plugin isn't used.
   *
   * @return a `CompletionStage` that is completed when the initialization has completed
   */
  def initializePlugins(
      system: ClassicActorSystemProvider,
      journalPluginId: String,
      snapshotPluginId: String,
      timeout: Duration): CompletionStage[Done] =
    scaladsl.PersistenceInit.initializePlugins(system, journalPluginId, snapshotPluginId, timeout.toScala).asJava

}
