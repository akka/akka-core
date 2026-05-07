/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import java.time.Duration

import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.DeadLetter
import akka.actor.Dropped
import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.internal.TestKitUtils
import akka.actor.testkit.typed.scaladsl
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.Scheduler
import akka.util.Timeout

object ActorTestKit {

  /**
   * Create a testkit named from the ActorTestKit class.
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   *
   * Config loaded from `application-test.conf` if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   * The application.conf of your project is not used in this case.
   */
  def create(): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(TestKitUtils.testNameFromCallStack(classOf[ActorTestKit])))

  /**
   * Create a testkit from the provided actor system.
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   *
   * Config loaded from the provided actor if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   */
  def create(system: ActorSystem[_]): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(system))

  /**
   * Create a testkit using the provided name.
   *
   * It will create an [[akka.actor.typed.ActorSystem]] with this name,
   * e.g. threads will include the name.
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   *
   * Config loaded from `application-test.conf` if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   * The application.conf of your project is not used in this case.
   */
  def create(name: String): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(name))

  /**
   * Create a testkit named from the ActorTestKit class,
   * and use a custom config for the actor system.
   *
   * It will also used the provided customConfig provided to create the `ActorSystem`
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   */
  def create(customConfig: Config): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(TestKitUtils.testNameFromCallStack(classOf[ActorTestKit]), customConfig))

  /**
   * Create a test kit named based on the provided name,
   * and uses the provided custom config for the actor system.
   *
   * It will create an [[akka.actor.typed.ActorSystem]] with this name,
   * e.g. threads will include the name.
   *
   * It will also used the provided customConfig provided to create the `ActorSystem`
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   */
  def create(name: String, customConfig: Config): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(name, customConfig))

  /**
   * Create an [[akka.actor.typed.ActorSystem]] named based on the provided name,
   * use the provided custom config for the actor system, and the testkit will use the provided setting.
   *
   * It will create an [[akka.actor.typed.ActorSystem]] with this name,
   * e.g. threads will include the name.
   *
   * It will also used the provided customConfig provided to create the `ActorSystem`, and provided setting.
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   */
  def create(name: String, customConfig: Config, settings: TestKitSettings): ActorTestKit =
    new ActorTestKit(scaladsl.ActorTestKit(name, customConfig, settings))

  /**
   * Shutdown the given actor system and wait up to `duration` for shutdown to complete.
   * @param throwIfShutdownTimesOut Fail the test if the system fails to shut down, if false
   *                             an error is printed to stdout when the system did not shutdown but
   *                             no exception is thrown.
   */
  def shutdown(system: ActorSystem[_], duration: Duration, throwIfShutdownTimesOut: Boolean): Unit = {
    TestKitUtils.shutdown(system, duration.toScala, throwIfShutdownTimesOut)
  }

  /**
   * Shutdown the given [[akka.actor.typed.ActorSystem]] and block until it shuts down,
   * if more time than `system-shutdown-default` passes an exception is thrown
   * (can be configured with `throw-on-shutdown-timeout`).
   */
  def shutdown(system: ActorSystem[_], duration: Duration): Unit = {
    val settings = TestKitSettings.create(system)
    shutdown(system, duration, settings.ThrowOnShutdownTimeout)
  }

  /**
   * Shutdown the given [[akka.actor.typed.ActorSystem]] and block until it shuts down,
   * if more time than `system-shutdown-default` passes an exception is thrown
   * (can be configured with `throw-on-shutdown-timeout`).
   */
  def shutdown(system: ActorSystem[_]): Unit = {
    val settings = TestKitSettings.create(system)
    shutdown(system, settings.DefaultActorSystemShutdownTimeout.toJava, settings.ThrowOnShutdownTimeout)
  }

  /**
   * Config loaded from `application-test.conf`, which is used if no specific config is given.
   */
  def applicationTestConfig: Config = scaladsl.ActorTestKit.ApplicationTestConfig

}

/**
 * Java API: Test kit for asynchronous testing of typed actors.
 * Provides a typed actor system started on creation, that can be used for multiple test cases and is
 * shut down when `shutdown` is called.
 *
 * The actor system has a custom guardian that allows for spawning arbitrary actors using the `spawn` methods.
 *
 * Designed to work with any test framework, but framework glue code that calls `shutdownTestKit` after all tests has
 * run needs to be provided by the user or with [[TestKitJunitResource]].
 *
 * Use `TestKit.create` factories to construct manually or [[TestKitJunitResource]] to use together with JUnit tests
 *
 * For synchronous testing of a `Behavior` see [[BehaviorTestKit]]
 */
final class ActorTestKit private[akka] (delegate: akka.actor.testkit.typed.scaladsl.ActorTestKit) {

  /**
   * The default timeout as specified with the config/[[akka.actor.testkit.typed.TestKitSettings]]
   */
  def timeout: Timeout = delegate.timeout

  /**
   * The actor system running for this testkit. Interaction with the user guardian is done through methods on the testkit
   * which is why it is typed to `Void`.
   */
  def system: ActorSystem[Void] = delegate.system.asInstanceOf[ActorSystem[Void]]

  def testKitSettings: TestKitSettings = delegate.testKitSettings

  /**
   * The scheduler of the testkit actor system
   */
  def scheduler: Scheduler = delegate.scheduler

  /**
   * Spawn a new auto-named actor under the testkit user guardian and return the ActorRef for the spawned actor
   */
  def spawn[T](behavior: Behavior[T]): ActorRef[T] = delegate.spawn(behavior)

  /**
   * Spawn a new named actor under the testkit user guardian and return the ActorRef for the spawned actor,
   * note that spawning actors with the same name in multiple test cases will cause failures.
   */
  def spawn[T](behavior: Behavior[T], name: String): ActorRef[T] = delegate.spawn(behavior, name)

  /**
   * Spawn a new auto-named actor under the testkit user guardian with the given props
   * and return the ActorRef for the spawned actor
   */
  def spawn[T](behavior: Behavior[T], props: Props): ActorRef[T] = delegate.spawn(behavior, props)

  /**
   * Spawn a new named actor under the testkit user guardian with the given props and return the ActorRef
   * for the spawned actor, note that spawning actors with the same name in multiple test cases will cause failures.
   */
  def spawn[T](behavior: Behavior[T], name: String, props: Props): ActorRef[T] = delegate.spawn(behavior, name, props)

  /**
   * Stop the actor under test and wait until it terminates.
   * It can only be used for actors that were spawned by this `ActorTestKit`.
   * Other actors will not be stopped by this method.
   */
  def stop[T](ref: ActorRef[T]): Unit = delegate.stop(ref)

  /**
   * Stop the actor under test and wait `max` until it terminates.
   * It can only be used for actors that were spawned by this `ActorTestKit`.
   * Other actors will not be stopped by this method.
   */
  def stop[T](ref: ActorRef[T], max: Duration): Unit = delegate.stop(ref, max.toScala)

  /**
   * Shortcut for creating a new test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](): TestProbe[M] = TestProbe.create(system)

  /**
   * Shortcut for creating a new test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](clazz: Class[M]): TestProbe[M] = TestProbe.create(clazz, system)

  /**
   * Shortcut for creating a new named test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](name: String): TestProbe[M] = TestProbe.create(name, system)

  /**
   * Shortcut for creating a new named test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](name: String, clazz: Class[M]): TestProbe[M] = TestProbe.create(name, clazz, system)

  /**
   * @return A test probe that is subscribed to dropped letters from the system event bus. Subscription
   *         will be completed and verified so any dropped letter after it will be caught by the probe.
   */
  def createDroppedMessageProbe(): TestProbe[Dropped] =
    delegate.createDroppedMessageProbe().asJava

  /**
   * @return A test probe that is subscribed to dead letters from the system event bus. Subscription
   *         will be completed and verified so any dead letter after it will be caught by the probe.
   */
  def createDeadLetterProbe(): TestProbe[DeadLetter] = delegate.createDeadLetterProbe().asJava

  /**
   * @return A test probe that is subscribed to unhandled messages from the system event bus. Subscription
   *         will be completed and verified so any unhandled message after it will be caught by the probe.
   */
  def createUnhandledMessageProbe(): TestProbe[UnhandledMessage] = delegate.createUnhandledMessageProbe().asJava

  // Note that if more methods are added here they should also be added to TestKitJunitResource

  /**
   * Terminate the actor system and the testkit
   */
  def shutdownTestKit(): Unit = delegate.shutdownTestKit()

  /**
   * Additional testing utilities for serialization.
   */
  val serializationTestKit: SerializationTestKit = new SerializationTestKit(delegate.internalSystem)

}
