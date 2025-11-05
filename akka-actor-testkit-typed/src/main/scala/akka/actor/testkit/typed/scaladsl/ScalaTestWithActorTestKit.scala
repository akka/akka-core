/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.{ BeforeAndAfterAll, TestSuite }
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Span

import akka.actor.testkit.typed.TestKitSettings
import akka.actor.typed.ActorSystem

/**
 * A ScalaTest base class for the [[ActorTestKit]], making it possible to have ScalaTest manage the lifecycle of the testkit.
 * The testkit will be automatically shut down when the test completes or fails using ScalaTest's BeforeAndAfterAll trait. If
 * a spec overrides afterAll it must call super.afterAll.
 *
 * Note that ScalaTest is not provided as a transitive dependency of the testkit module but must be added explicitly
 * to your project to use this.
 *
 * By default config is loaded from `application-test.conf` if that exists, otherwise
 * using default configuration from the reference.conf resources that ship with the Akka libraries.
 * The application.conf of your project is not used in this case.
 * A specific configuration can be passed as constructor parameter.
 */
abstract class ScalaTestWithActorTestKit(testKit: ActorTestKit)
    extends ActorTestKitBase(testKit)
    with TestSuite
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures
    with Eventually {

  /**
   * Config loaded from `application-test.conf` if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   * The application.conf of your project is not used in this case.
   */
  def this() = this(ActorTestKit(ActorTestKitBase.testNameFromCallStack()))

  /**
   * Use a custom [[akka.actor.typed.ActorSystem]] for the actor system.
   */
  def this(system: ActorSystem[_]) = this(ActorTestKit(system))

  /**
   * Use a custom config for the actor system.
   */
  def this(config: String) =
    this(ActorTestKit(ActorTestKitBase.testNameFromCallStack(), ConfigFactory.parseString(config)))

  /**
   * Use a custom config for the actor system.
   */
  def this(config: Config) = this(ActorTestKit(ActorTestKitBase.testNameFromCallStack(), config))

  /**
   * Use a custom config for the actor system, and a custom [[akka.actor.testkit.typed.TestKitSettings]].
   */
  def this(config: Config, settings: TestKitSettings) =
    this(ActorTestKit(ActorTestKitBase.testNameFromCallStack(), config, settings))

  /**
   * `PatienceConfig` from [[akka.actor.testkit.typed.TestKitSettings#DefaultTimeout]].
   * `DefaultTimeout` is dilated with [[akka.actor.testkit.typed.TestKitSettings#TestTimeFactor]],
   * which means that the patience is also dilated.
   */
  implicit val patience: PatienceConfig =
    PatienceConfig(testKit.testKitSettings.DefaultTimeout.duration, Span(100, org.scalatest.time.Millis))

  /**
   * Shuts down the ActorTestKit. If override be sure to call super.afterAll
   * or shut down the testkit explicitly with `testKit.shutdownTestKit()`.
   */
  override protected def afterAll(): Unit = {
    super.afterAll()
    testKit.shutdownTestKit()
  }
}
