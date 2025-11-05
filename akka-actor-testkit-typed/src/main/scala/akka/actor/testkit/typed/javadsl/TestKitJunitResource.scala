/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import java.time.Duration

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.Rule
import org.junit.rules.ExternalResource

import akka.actor.DeadLetter
import akka.actor.Dropped
import akka.actor.UnhandledMessage
import akka.actor.testkit.typed.TestKitSettings
import akka.actor.testkit.typed.internal.TestKitUtils
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Props
import akka.actor.typed.Scheduler
import akka.util.Timeout

/**
 * A Junit external resource for the [[ActorTestKit]], making it possible to have Junit manage the lifecycle of the testkit.
 * The testkit will be automatically shut down when the test completes or fails.
 *
 * Note that Junit is not provided as a transitive dependency of the testkit module but must be added explicitly
 * to your project to use this.
 *
 * Example:
 * {{{
 * public class MyActorTest {
 *   @ClassRule
 *   public static final TestKitResource testKit = new TestKitResource();
 *
 *   @Test
 *   public void testBlah() throws Exception {
 * 	   // spawn actors etc using the testKit
 * 	   ActorRef<Message> ref = testKit.spawn(behavior);
 *   }
 * }
 * }}}
 *
 * By default config is loaded from `application-test.conf` if that exists, otherwise
 * using default configuration from the reference.conf resources that ship with the Akka libraries.
 * The application.conf of your project is not used in this case.
 * A specific configuration can be passed as constructor parameter.
 */
final class TestKitJunitResource(_kit: ActorTestKit) extends ExternalResource {

  /**
   * Config loaded from `application-test.conf` if that exists, otherwise
   * using default configuration from the reference.conf resources that ship with the Akka libraries.
   * The application.conf of your project is not used in this case.
   */
  def this() = this(ActorTestKit.create(TestKitUtils.testNameFromCallStack(classOf[TestKitJunitResource])))

  /**
   * Use a custom [[akka.actor.typed.ActorSystem]] for the actor system.
   */
  def this(system: ActorSystem[_]) = this(ActorTestKit.create(system))

  /**
   * Use a custom config for the actor system.
   */
  def this(customConfig: String) =
    this(
      ActorTestKit.create(
        TestKitUtils.testNameFromCallStack(classOf[TestKitJunitResource]),
        ConfigFactory.parseString(customConfig)))

  /**
   * Use a custom config for the actor system.
   */
  def this(customConfig: Config) =
    this(ActorTestKit.create(TestKitUtils.testNameFromCallStack(classOf[TestKitJunitResource]), customConfig))

  /**
   * Use a custom config for the actor system, and a custom [[akka.actor.testkit.typed.TestKitSettings]].
   */
  def this(customConfig: Config, settings: TestKitSettings) =
    this(ActorTestKit.create(TestKitUtils.testNameFromCallStack(classOf[TestKitJunitResource]), customConfig, settings))

  @Rule
  val testKit: ActorTestKit = _kit

  // delegates of the TestKit api for minimum fuss
  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def system: ActorSystem[Void] = testKit.system

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def testKitSettings: TestKitSettings = testKit.testKitSettings

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def timeout: Timeout = testKit.timeout

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def scheduler: Scheduler = testKit.scheduler

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def spawn[T](behavior: Behavior[T]): ActorRef[T] = testKit.spawn(behavior)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def spawn[T](behavior: Behavior[T], name: String): ActorRef[T] = testKit.spawn(behavior, name)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def spawn[T](behavior: Behavior[T], props: Props): ActorRef[T] = testKit.spawn(behavior, props)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def spawn[T](behavior: Behavior[T], name: String, props: Props): ActorRef[T] = testKit.spawn(behavior, name, props)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createTestProbe[M](): TestProbe[M] = testKit.createTestProbe[M]()

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createTestProbe[M](clazz: Class[M]): TestProbe[M] = testKit.createTestProbe(clazz)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createTestProbe[M](name: String, clazz: Class[M]): TestProbe[M] = testKit.createTestProbe(name, clazz)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createTestProbe[M](name: String): TestProbe[M] = testKit.createTestProbe(name)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def stop[T](ref: ActorRef[T], max: Duration): Unit = testKit.stop(ref, max)

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createUnhandledMessageProbe(): TestProbe[UnhandledMessage] = testKit.createUnhandledMessageProbe()

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createDeadLetterProbe(): TestProbe[DeadLetter] = testKit.createDeadLetterProbe()

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def createDroppedMessageProbe(): TestProbe[Dropped] = testKit.createDroppedMessageProbe()

  /**
   * See corresponding method on [[ActorTestKit]]
   */
  def stop[T](ref: ActorRef[T]): Unit = testKit.stop(ref)

  /**
   * Additional testing utilities for serialization.
   */
  def serializationTestKit: SerializationTestKit = testKit.serializationTestKit

  override def after(): Unit = {
    testKit.shutdownTestKit()
  }

}
