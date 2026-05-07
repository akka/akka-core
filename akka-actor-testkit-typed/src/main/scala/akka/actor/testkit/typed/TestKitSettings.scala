/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed

import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.util.Timeout

object TestKitSettings {

  /**
   * Reads configuration settings from `akka.actor.testkit.typed` section.
   */
  def apply(system: ActorSystem[_]): TestKitSettings =
    Ext(system).settings

  /**
   * Reads configuration settings from given `Config` that
   * must have the same layout as the `akka.actor.testkit.typed` section.
   */
  def apply(config: Config): TestKitSettings =
    new TestKitSettings(config)

  /**
   * Java API: Reads configuration settings from `akka.actor.testkit.typed` section.
   */
  def create(system: ActorSystem[_]): TestKitSettings =
    apply(system)

  /**
   * Reads configuration settings from given `Config` that
   * must have the same layout as the `akka.actor.testkit.typed` section.
   */
  def create(config: Config): TestKitSettings =
    new TestKitSettings(config)

  private object Ext extends ExtensionId[Ext] {
    override def createExtension(system: ActorSystem[_]): Ext = new Ext(system)
    def get(system: ActorSystem[_]): Ext = Ext.apply(system)
  }

  private class Ext(system: ActorSystem[_]) extends Extension {
    val settings: TestKitSettings = TestKitSettings(system.settings.config.getConfig("akka.actor.testkit.typed"))
  }
}

final class TestKitSettings(val config: Config) {

  import akka.util.Helpers._

  val TestTimeFactor: Double = config
    .getDouble("timefactor")
    .requiring(tf => !tf.isInfinite && tf > 0, "timefactor must be positive finite double")

  /** Dilated with `TestTimeFactor`. */
  val SingleExpectDefaultTimeout: FiniteDuration = dilated(config.getMillisDuration("single-expect-default"))

  /** Dilated with `TestTimeFactor`. */
  val ExpectNoMessageDefaultTimeout: FiniteDuration = dilated(config.getMillisDuration("expect-no-message-default"))

  /** Dilated with `TestTimeFactor`. */
  val DefaultTimeout: Timeout = Timeout(dilated(config.getMillisDuration("default-timeout")))

  /** Dilated with `TestTimeFactor`. */
  val DefaultActorSystemShutdownTimeout: FiniteDuration = dilated(config.getMillisDuration("system-shutdown-default"))

  val ThrowOnShutdownTimeout: Boolean = config.getBoolean("throw-on-shutdown-timeout")

  /** Dilated with `TestTimeFactor`. */
  val FilterLeeway: FiniteDuration = dilated(config.getMillisDuration("filter-leeway"))

  /**
   * Scala API: Scale the `duration` with the configured `TestTimeFactor`
   */
  def dilated(duration: FiniteDuration): FiniteDuration =
    Duration.fromNanos((duration.toNanos * TestTimeFactor + 0.5).toLong)

  /**
   * Java API: Scale the `duration` with the configured `TestTimeFactor`
   */
  def dilated(duration: java.time.Duration): java.time.Duration =
    dilated(duration.toScala).toJava
}
