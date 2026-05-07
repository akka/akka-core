/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import java.time.LocalDate
import java.util.Optional
import java.util.concurrent.{ CompletionStage, ThreadFactory }

import scala.concurrent.{ ExecutionContextExecutor, Future }

import com.typesafe.config.{ Config, ConfigFactory }
import org.slf4j.Logger

import akka.{ Done, actor => classic }
import akka.actor.{ Address, BootstrapSetup, ClassicActorSystemProvider }
import akka.actor.setup.ActorSystemSetup
import akka.actor.typed.eventstream.EventStream
import akka.actor.typed.internal.{ EventStreamExtension, InternalRecipientRef }
import akka.actor.typed.internal.adapter.{ ActorSystemAdapter, GuardianStartupBehavior, PropsAdapter }
import akka.actor.typed.receptionist.Receptionist
import akka.annotation.DoNotInherit
import akka.util.Helpers.Requiring

/**
 * An ActorSystem is home to a hierarchy of Actors. It is created using
 * [[ActorSystem#apply]] from a [[Behavior]] object that describes the root
 * Actor of this hierarchy and which will create all other Actors beneath it.
 * A system also implements the [[ActorRef]] type, and sending a message to
 * the system directs that message to the root Actor.
 *
 * Not for user extension.
 */
@DoNotInherit
abstract class ActorSystem[-T] extends ActorRef[T] with Extensions with ClassicActorSystemProvider {
  this: InternalRecipientRef[T] =>

  /**
   * The name of this actor system, used to distinguish multiple ones within
   * the same JVM & class loader.
   */
  def name: String

  /**
   * The core settings extracted from the supplied configuration.
   */
  def settings: Settings

  /**
   * Log the configuration.
   */
  def logConfiguration(): Unit

  /**
   * A [[org.slf4j.Logger]] that can be used to emit log messages
   * without specifying a more detailed source. Typically it is desirable to
   * use the dedicated `Logger` available from each Actor’s [[TypedActorContext]]
   * as that ties the log entries to the actor.
   */
  def log: Logger

  /**
   * Start-up time in milliseconds since the epoch.
   */
  def startTime: Long

  /**
   * Up-time of this actor system in seconds.
   */
  def uptime: Long

  /**
   * A ThreadFactory that can be used if the transport needs to create any Threads
   */
  def threadFactory: ThreadFactory

  /**
   * ClassLoader wrapper which is used for reflective accesses internally. This is set
   * to use the context class loader, if one is set, or the class loader which
   * loaded the ActorSystem implementation. The context class loader is also
   * set on all threads created by the ActorSystem, if one was set during
   * creation.
   */
  def dynamicAccess: classic.DynamicAccess

  /**
   * A generic scheduler that can initiate the execution of tasks after some delay.
   * It is recommended to use the ActorContext’s scheduling capabilities for sending
   * messages to actors instead of registering a Runnable for execution using this facility.
   */
  def scheduler: Scheduler

  /**
   * Facilities for lookup up thread-pools from configuration.
   */
  def dispatchers: Dispatchers

  /**
   * The default thread pool of this ActorSystem, configured with settings in `akka.actor.default-dispatcher`.
   */
  implicit def executionContext: ExecutionContextExecutor

  /**
   * Terminates this actor system by running [[akka.actor.CoordinatedShutdown]] with reason
   * [[akka.actor.CoordinatedShutdown.ActorSystemTerminateReason]].
   *
   * If `akka.coordinated-shutdown.run-by-actor-system-terminate` is configured to `off`
   * it will not run `CoordinatedShutdown`, but the `ActorSystem` and its actors
   * will still be terminated.
   *
   * This will stop the guardian actor, which in turn
   * will recursively stop all its child actors, and finally the system guardian
   * (below which the logging actors reside).
   *
   * This is an asynchronous operation and completion of the termination can
   * be observed with [[ActorSystem.whenTerminated]] or [[ActorSystem.getWhenTerminated]].
   */
  def terminate(): Unit

  /**
   * Scala API: Returns a Future which will be completed after the ActorSystem has been terminated.
   * The `ActorSystem` can be stopped with [[ActorSystem.terminate]]
   * or by stopping the guardian actor.
   *
   * Be careful to not schedule any operations, such as `onComplete`, on the dispatchers (`ExecutionContext`)
   * of this actor system as they will have been shut down before this future completes.
   */
  def whenTerminated: Future[Done]

  /**
   * Java API: Returns a CompletionStage which will be completed after the ActorSystem has been terminated
   * and termination hooks have been executed. The `ActorSystem` can be stopped with [[ActorSystem.terminate]]
   * or by stopping the guardian actor.
   *
   * Be careful to not schedule any operations, such as `thenRunAsync`, on the dispatchers (`Executor`) of this
   * actor system as they will have been shut down before this CompletionStage completes.
   */
  def getWhenTerminated: CompletionStage[Done]

  /**
   * The deadLetter address is a destination that will accept (and discard)
   * every message sent to it.
   */
  def deadLetters[U]: ActorRef[U]

  /**
   * An ActorRef that ignores any incoming messages.
   */
  def ignoreRef[U]: ActorRef[U]

  /**
   * Create a string representation of the actor hierarchy within this system
   * for debugging purposes.
   *
   * The format of the string is subject to change, i.e. no stable “API”.
   */
  def printTree: String

  /**
   * Create an actor in the "/system" namespace. This actor will be shut down
   * during system.terminate only after all user actors have terminated.
   *
   * This is only intended to be used by libraries (and Akka itself).
   * Applications should use ordinary `spawn`.
   */
  def systemActorOf[U](behavior: Behavior[U], name: String, props: Props = Props.empty): ActorRef[U]

  /**
   * Return a reference to this system’s [[akka.actor.typed.receptionist.Receptionist]].
   */
  def receptionist: ActorRef[Receptionist.Command] =
    Receptionist(this).ref

  /**
   * Main event bus of this actor system, used for example for logging.
   * Accepts [[akka.actor.typed.eventstream.EventStream.Command]].
   */
  def eventStream: ActorRef[EventStream.Command] =
    EventStreamExtension(this).ref

  /**
   * Obtains the external address of the default transport.
   *
   * Consider differences in clustered and non-clustered ActorSystems.
   * For a non-clustered ActorSystem, this will return an address without host and port.
   * For a clustered ActorSystem, this will return the address that other nodes can use to
   * communicate with this node.
   */
  def address: Address

  /**
   * Scala API: When the license key will expire. `None` for perpetual keys.
   * If a license key is not defined the expiry date will be today's date.
   */
  def licenseKeyExpiry: Option[LocalDate]

  /**
   * Java API: When the license key will expire. `Optional.empty` for perpetual keys.
   * If a license key is not defined the expiry date will be today's date.
   */
  def getLicenseKeyExpiry: Optional[LocalDate]

}

object ActorSystem {

  /**
   * Scala API: Create an ActorSystem
   */
  def apply[T](guardianBehavior: Behavior[T], name: String): ActorSystem[T] =
    createInternal(name, guardianBehavior, Props.empty, ActorSystemSetup.create(BootstrapSetup()))

  /**
   * Scala API: Create an ActorSystem
   */
  def apply[T](guardianBehavior: Behavior[T], name: String, config: Config): ActorSystem[T] =
    createInternal(name, guardianBehavior, Props.empty, ActorSystemSetup.create(BootstrapSetup(config)))

  /**
   * Scala API: Create an ActorSystem
   */
  def apply[T](guardianBehavior: Behavior[T], name: String, config: Config, guardianProps: Props): ActorSystem[T] =
    createInternal(name, guardianBehavior, guardianProps, ActorSystemSetup.create(BootstrapSetup(config)))

  /**
   * Scala API: Creates a new actor system with the specified name and settings
   * The core actor system settings are defined in [[BootstrapSetup]]
   */
  def apply[T](
      guardianBehavior: Behavior[T],
      name: String,
      setup: ActorSystemSetup,
      guardianProps: Props = Props.empty): ActorSystem[T] = {
    createInternal(name, guardianBehavior, guardianProps, setup)
  }

  /**
   * Scala API: Shortcut for creating an actor system with custom bootstrap settings.
   * Same behavior as calling `ActorSystem(name, ActorSystemSetup(bootstrapSetup))`
   */
  def apply[T](guardianBehavior: Behavior[T], name: String, bootstrapSetup: BootstrapSetup): ActorSystem[T] =
    apply(guardianBehavior, name, ActorSystemSetup.create(bootstrapSetup))

  /**
   * Java API: Create an ActorSystem
   */
  def create[T](guardianBehavior: Behavior[T], name: String): ActorSystem[T] =
    apply(guardianBehavior, name)

  /**
   * Java API: Create an ActorSystem
   */
  def create[T](guardianBehavior: Behavior[T], name: String, config: Config): ActorSystem[T] =
    apply(guardianBehavior, name, config)

  /**
   * Java API: Create an ActorSystem
   */
  def create[T](guardianBehavior: Behavior[T], name: String, config: Config, guardianProps: Props): ActorSystem[T] =
    createInternal(name, guardianBehavior, guardianProps, ActorSystemSetup.create(BootstrapSetup(config)))

  /**
   * Java API: Creates a new actor system with the specified name and settings
   * The core actor system settings are defined in [[BootstrapSetup]]
   */
  def create[T](guardianBehavior: Behavior[T], name: String, setups: ActorSystemSetup): ActorSystem[T] =
    apply(guardianBehavior, name, setups)

  /**
   * Java API: Shortcut for creating an actor system with custom bootstrap settings.
   * Same behavior as calling `ActorSystem.create(name, ActorSystemSetup.create(bootstrapSettings))`
   */
  def create[T](guardianBehavior: Behavior[T], name: String, bootstrapSetup: BootstrapSetup): ActorSystem[T] =
    create(guardianBehavior, name, ActorSystemSetup.create(bootstrapSetup))

  /**
   * Create an ActorSystem based on the classic [[akka.actor.ActorSystem]]
   * which runs Akka Typed [[Behavior]] on an emulation layer. In this
   * system typed and classic actors can coexist.
   */
  private def createInternal[T](
      name: String,
      guardianBehavior: Behavior[T],
      guardianProps: Props,
      setup: ActorSystemSetup): ActorSystem[T] = {

    Behavior.validateAsInitial(guardianBehavior)
    require(Behavior.isAlive(guardianBehavior))

    val bootstrapSettings = setup.get[BootstrapSetup]
    val cl = bootstrapSettings.flatMap(_.classLoader).getOrElse(akka.actor.ActorSystem.findClassLoader())
    val appConfig = bootstrapSettings.flatMap(_.config).getOrElse(ConfigFactory.load(cl))
    val executionContext = bootstrapSettings.flatMap(_.defaultExecutionContext)

    val system = new classic.ActorSystemImpl(
      name,
      appConfig,
      cl,
      executionContext,
      Some(
        PropsAdapter[Any](() => GuardianStartupBehavior(guardianBehavior), guardianProps, rethrowTypedFailure = false)),
      setup)
    system.start()

    system.guardian ! GuardianStartupBehavior.Start
    ActorSystemAdapter.AdapterExtension(system).adapter
  }

  /**
   * Wrap a classic [[akka.actor.ActorSystem]] such that it can be used from
   * Akka Typed [[Behavior]].
   */
  def wrap(system: classic.ActorSystem): ActorSystem[Nothing] =
    ActorSystemAdapter.AdapterExtension(system.asInstanceOf[classic.ActorSystemImpl]).adapter
}

/**
 * The configuration settings that were parsed from the config by an [[ActorSystem]].
 * This class is immutable.
 */
final class Settings(val config: Config, val classicSettings: classic.ActorSystem.Settings, val name: String) {

  def this(settings: classic.ActorSystem.Settings) = this(settings.config, settings, settings.name)

  def setup: ActorSystemSetup = classicSettings.setup

  /**
   * Returns the String representation of the Config that this Settings is backed by
   */
  override def toString: String = config.root.render

  private val typedConfig = config.getConfig("akka.actor.typed")

  val RestartStashCapacity: Int =
    typedConfig.getInt("restart-stash-capacity").requiring(_ >= 0, "restart-stash-capacity must be >= 0")

  val PubSubDeadLettersWhenNoSubscribers: Boolean =
    typedConfig.getBoolean("pub-sub.send-to-dead-letters-when-no-subscribers")
}
