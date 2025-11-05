/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.scaladsl

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import akka.actor.DeadLetter
import akka.actor.DeadLetterSuppression
import akka.actor.Dropped
import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.internal.ActorTestKitGuardian
import akka.actor.testkit.typed.internal.TestKitUtils
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.Scheduler
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.util.Timeout

object ActorTestKit {

  private val testKitGuardianCounter = new AtomicInteger(0)

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
  def apply(): ActorTestKit = {
    val system = ActorSystem(
      ActorTestKitGuardian.testKitGuardian,
      TestKitUtils.testNameFromCallStack(classOf[ActorTestKit]),
      ApplicationTestConfig)
    new ActorTestKit(system, system, settings = None)
  }

  /**
   * Create a testkit from the provided actor system.
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   *
   * Config loaded from the provided actor if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   */
  def apply(system: ActorSystem[_]): ActorTestKit = {
    val name = testKitGuardianCounter.incrementAndGet() match {
      case 1 => "test"
      case n => s"test-$n"
    }
    val testKitGuardian =
      system.systemActorOf(ActorTestKitGuardian.testKitGuardian, name)
    new ActorTestKit(system, testKitGuardian, settings = None)
  }

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
  def apply(name: String): ActorTestKit = {
    val system =
      ActorSystem(ActorTestKitGuardian.testKitGuardian, TestKitUtils.scrubActorSystemName(name), ApplicationTestConfig)
    new ActorTestKit(system, system, settings = None)
  }

  /**
   * Create a testkit named from the ActorTestKit class,
   * and use a custom config for the actor system.
   *
   * It will also used the provided customConfig provided to create the `ActorSystem`
   *
   * When the test has completed you should terminate the `ActorSystem` and
   * the testkit with [[ActorTestKit#shutdownTestKit]].
   */
  def apply(customConfig: Config): ActorTestKit = {
    val system = ActorSystem(
      ActorTestKitGuardian.testKitGuardian,
      TestKitUtils.testNameFromCallStack(classOf[ActorTestKit]),
      customConfig)
    new ActorTestKit(system, system, settings = None)
  }

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
  def apply(name: String, customConfig: Config): ActorTestKit = {
    val system =
      ActorSystem(ActorTestKitGuardian.testKitGuardian, TestKitUtils.scrubActorSystemName(name), customConfig)
    new ActorTestKit(system, system, settings = None)
  }

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
  def apply(name: String, customConfig: Config, settings: TestKitSettings): ActorTestKit = {
    val system =
      ActorSystem(ActorTestKitGuardian.testKitGuardian, TestKitUtils.scrubActorSystemName(name), customConfig)
    new ActorTestKit(system, system, settings = Some(settings))
  }

  /**
   * Shutdown the given [[akka.actor.typed.ActorSystem]] and block until it shuts down,
   * if more time than `TestKitSettings.DefaultActorSystemShutdownTimeout` passes an exception is thrown
   */
  def shutdown(system: ActorSystem[_]): Unit = {
    val settings = TestKitSettings(system)
    TestKitUtils.shutdown(system, settings.DefaultActorSystemShutdownTimeout, settings.ThrowOnShutdownTimeout)
  }

  /**
   * Shutdown the given [[akka.actor.typed.ActorSystem]] and block until it shuts down
   * or the `duration` hits. If the timeout hits `verifySystemShutdown` decides
   */
  def shutdown(system: ActorSystem[_], timeout: Duration, throwIfShutdownFails: Boolean = false): Unit =
    TestKitUtils.shutdown(system, timeout, throwIfShutdownFails)

  /**
   * Config loaded from `application-test.conf`, which is used if no specific config is given.
   */
  val ApplicationTestConfig: Config = ConfigFactory.load("application-test")

  private val dummyMessage = new DeadLetterSuppression {}
}

/**
 * Testkit for asynchronous testing of typed actors, meant for mixing into the test class.
 *
 * Provides a typed actor system started on creation, used for all test cases and shut down when `shutdown` is called.
 *
 * The actor system has a custom guardian that allows for spawning arbitrary actors using the `spawn` methods.
 *
 * Designed to work with any test framework, but framework glue code that calls shutdown after all tests has
 * run needs to be provided by the user.
 *
 * For synchronous testing of a `Behavior` see [[BehaviorTestKit]]
 */
final class ActorTestKit private[akka] (
    val internalSystem: ActorSystem[_],
    internalTestKitGuardian: ActorRef[ActorTestKitGuardian.TestKitCommand],
    settings: Option[TestKitSettings]) {

  val name = internalSystem.name

  val config = internalSystem.settings.config

  // avoid slf4j noise by touching it first from single thread #28673
  LoggerFactory.getLogger(internalSystem.name).debug("Starting ActorTestKit")

  implicit def testKitSettings: TestKitSettings =
    settings.getOrElse(TestKitSettings(system))

  /**
   * INTERNAL API
   */
  implicit def system: ActorSystem[Nothing] = internalSystem

  private val childName: Iterator[String] = Iterator.from(0).map(_.toString)

  implicit val timeout: Timeout = testKitSettings.DefaultTimeout

  def scheduler: Scheduler = system.scheduler

  def shutdownTestKit(): Unit = {
    ActorTestKit.shutdown(
      system,
      testKitSettings.DefaultActorSystemShutdownTimeout,
      testKitSettings.ThrowOnShutdownTimeout)
  }

  /**
   * Spawn the given behavior. This is created as a child of the test kit
   * guardian
   */
  def spawn[T](behavior: Behavior[T]): ActorRef[T] =
    spawn(behavior, Props.empty)

  /**
   * Spawn the given behavior. This is created as a child of the test kit
   * guardian
   */
  def spawn[T](behavior: Behavior[T], props: Props): ActorRef[T] =
    Await.result(
      internalTestKitGuardian.ask(ActorTestKitGuardian.SpawnActorAnonymous(behavior, _, props)),
      timeout.duration)

  /**
   * Spawn the given behavior. This is created as a child of the test kit
   * guardian
   */
  def spawn[T](behavior: Behavior[T], name: String): ActorRef[T] =
    spawn(behavior, name, Props.empty)

  /**
   * Spawn the given behavior. This is created as a child of the test kit
   * guardian
   */
  def spawn[T](behavior: Behavior[T], name: String, props: Props): ActorRef[T] =
    Await.result(
      internalTestKitGuardian.ask(ActorTestKitGuardian.SpawnActor(name, behavior, _, props)),
      timeout.duration)

  /**
   * Stop the actor under test and wait until it terminates.
   * It can only be used for actors that were spawned by this `ActorTestKit`.
   * Other actors will not be stopped by this method.
   */
  def stop[T](ref: ActorRef[T], max: FiniteDuration = timeout.duration): Unit =
    try {
      Await.result(internalTestKitGuardian.ask { (x: ActorRef[ActorTestKitGuardian.Ack.type]) =>
        ActorTestKitGuardian.StopActor(ref, x)
      }(Timeout(max), scheduler), max)
    } catch {
      case _: TimeoutException =>
        assert(false, s"timeout ($max) during stop() waiting for actor [${ref.path}] to stop")
    }

  /**
   * Shortcut for creating a new test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](): TestProbe[M] = TestProbe()(system)

  /**
   * Shortcut for creating a new named test probe for the testkit actor system
   * @tparam M the type of messages the probe should accept
   */
  def createTestProbe[M](name: String): TestProbe[M] = TestProbe(name)(system)

  /**
   * @return A test probe that is subscribed to unhandled messages from the system event bus. Subscription
   *         will be completed and verified so any unhandled message after it will be caught by the probe.
   */
  def createUnhandledMessageProbe(): TestProbe[UnhandledMessage] =
    subscribeEventBusAndVerifySubscribed[UnhandledMessage](() =>
      UnhandledMessage(ActorTestKit.dummyMessage, system.deadLetters.toClassic, system.deadLetters.toClassic))

  /**
   * @return A test probe that is subscribed to dead letters from the system event bus. Subscription
   *         will be completed and verified so any dead letter after it will be caught by the probe.
   */
  def createDeadLetterProbe(): TestProbe[DeadLetter] =
    subscribeEventBusAndVerifySubscribed[DeadLetter](() =>
      DeadLetter(ActorTestKit.dummyMessage, system.deadLetters.toClassic, system.deadLetters.toClassic))

  /**
   * @return A test probe that is subscribed to dropped letters from the system event bus. Subscription
   *         will be completed and verified so any dropped letter after it will be caught by the probe.
   */
  def createDroppedMessageProbe(): TestProbe[Dropped] =
    subscribeEventBusAndVerifySubscribed[Dropped](() =>
      Dropped(ActorTestKit.dummyMessage, "no reason", system.deadLetters.toClassic, system.deadLetters.toClassic))

  private def subscribeEventBusAndVerifySubscribed[M <: AnyRef: ClassTag](createTestEvent: () => M): TestProbe[M] = {
    val probe = createTestProbe[M]()
    system.eventStream ! EventStream.Subscribe(probe.ref)
    probe.awaitAssert {
      val testEvent = createTestEvent()
      system.eventStream ! EventStream.Publish(testEvent)
      probe.fishForMessage(probe.remainingOrDefault) {
        case m: AnyRef if m eq testEvent => FishingOutcomes.complete
        case _                           => FishingOutcomes.continue
      }
    }
    probe
  }

  /**
   * Additional testing utilities for serialization.
   */
  val serializationTestKit: SerializationTestKit = new SerializationTestKit(internalSystem)

  // FIXME needed for Akka internal tests but, users shouldn't spawn system actors?
  @InternalApi
  private[akka] def systemActor[T](behavior: Behavior[T], name: String): ActorRef[T] =
    system.systemActorOf(behavior, name)

  @InternalApi
  private[akka] def systemActor[T](behavior: Behavior[T]): ActorRef[T] =
    system.systemActorOf(behavior, childName.next())
}
