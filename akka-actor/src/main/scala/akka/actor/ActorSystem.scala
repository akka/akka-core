/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.io.Closeable
import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.tailrec
import scala.collection.immutable
import scala.jdk.FutureConverters.FutureOps
import scala.jdk.OptionConverters._
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor, Future, Promise }
import scala.concurrent.blocking
import scala.concurrent.duration.{ Duration, DurationInt }
import scala.util.{ Failure, Success, Try }
import scala.util.control.{ ControlThrowable, NonFatal }

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigRenderOptions

import akka.ConfigurationException
import akka.actor.dungeon.ChildrenContainer
import akka.actor.dungeon.LicenseKeySupplier
import akka.actor.setup.{ ActorSystemSetup, Setup }
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.dispatch._
import akka.event._
import akka.event.Logging.DefaultLogger
import akka.japi.Util.immutableSeq
import akka.serialization.SerializationExtension
import akka.util._
import akka.util.Helpers.toRootLowerCase
import java.net.URLDecoder
import java.security.{ KeyFactory, MessageDigest, Signature }
import java.security.spec.X509EncodedKeySpec
import java.time.Instant
import java.time.{ LocalDate, ZoneId }

object BootstrapSetup {

  /**
   * Scala API: Construct a bootstrap settings with default values. Note that passing that to the actor system is the
   * same as not passing any [[BootstrapSetup]] at all. You can use the returned instance to derive
   * one that has other values than defaults using the various `with`-methods.
   */
  def apply(): BootstrapSetup = {
    new BootstrapSetup()
  }

  /**
   * Scala API: Create bootstrap settings needed for starting the actor system
   *
   * @see [[BootstrapSetup]] for description of the properties
   */
  def apply(
      classLoader: Option[ClassLoader],
      config: Option[Config],
      defaultExecutionContext: Option[ExecutionContext]): BootstrapSetup =
    new BootstrapSetup(classLoader, config, defaultExecutionContext)

  /**
   * Scala API: Short for using custom config but keeping default classloader and default execution context
   */
  def apply(config: Config): BootstrapSetup = apply(None, Some(config), None)

  /**
   * Java API: Create bootstrap settings needed for starting the actor system
   *
   * @see [[BootstrapSetup]] for description of the properties
   */
  def create(
      classLoader: Optional[ClassLoader],
      config: Optional[Config],
      defaultExecutionContext: Optional[ExecutionContext]): BootstrapSetup =
    apply(classLoader.toScala, config.toScala, defaultExecutionContext.toScala)

  /**
   * Java  API: Short for using custom config but keeping default classloader and default execution context
   */
  def create(config: Config): BootstrapSetup = apply(config)

  /**
   * Java API: Construct a bootstrap settings with default values. Note that passing that to the actor system is the
   * same as not passing any [[BootstrapSetup]] at all. You can use the returned instance to derive
   * one that has other values than defaults using the various `with`-methods.
   */
  def create(): BootstrapSetup = {
    new BootstrapSetup()
  }

}

/**
 * @param identifier the simple name of the selected provider
 * @param fqcn the fully-qualified class name of the selected provider
 */
abstract class ProviderSelection private (
    private[akka] val identifier: String,
    private[akka] val fqcn: String,
    private[akka] val hasCluster: Boolean)
object ProviderSelection {
  private[akka] val RemoteActorRefProvider = "akka.remote.RemoteActorRefProvider"
  private[akka] val ClusterActorRefProvider = "akka.cluster.ClusterActorRefProvider"

  case object Local extends ProviderSelection("local", classOf[LocalActorRefProvider].getName, hasCluster = false)
  // these two cannot be referenced by class as they may not be on the classpath
  case object Remote extends ProviderSelection("remote", RemoteActorRefProvider, hasCluster = false)
  case object Cluster extends ProviderSelection("cluster", ClusterActorRefProvider, hasCluster = true)
  final case class Custom(override val fqcn: String) extends ProviderSelection("custom", fqcn, hasCluster = false)

  /**
   * JAVA API
   */
  def local(): ProviderSelection = Local

  /**
   * JAVA API
   */
  def remote(): ProviderSelection = Remote

  /**
   * JAVA API
   */
  def cluster(): ProviderSelection = Cluster

  /** INTERNAL API */
  @InternalApi private[akka] def apply(providerClass: String): ProviderSelection =
    providerClass match {
      case "local" => Local
      // additional fqcn for older configs not using 'remote' or 'cluster'
      case "remote" | RemoteActorRefProvider   => Remote
      case "cluster" | ClusterActorRefProvider => Cluster
      case fqcn                                => Custom(fqcn)
    }
}

/**
 * Core bootstrap settings of the actor system, create using one of the factories in [[BootstrapSetup]],
 * constructor is *Internal API*.
 *
 * @param classLoader If no ClassLoader is given, it obtains the current ClassLoader by first inspecting the current
 *                    threads' getContextClassLoader, then tries to walk the stack to find the callers class loader, then
 *                    falls back to the ClassLoader associated with the ActorSystem class.
 * @param config Configuration to use for the actor system. If no Config is given, the default reference config will be obtained from the ClassLoader.
 * @param defaultExecutionContext If defined the ExecutionContext will be used as the default executor inside this ActorSystem.
 *                                If no ExecutionContext is given, the system will fallback to the executor configured under
 *                                "akka.actor.default-dispatcher.default-executor.fallback".
 * @param actorRefProvider Overrides the `akka.actor.provider` setting in config, can be `local` (default), `remote` or
 *                         `cluster`. It can also be a fully qualified class name of a provider.
 */
final class BootstrapSetup private (
    val classLoader: Option[ClassLoader] = None,
    val config: Option[Config] = None,
    val defaultExecutionContext: Option[ExecutionContext] = None,
    val actorRefProvider: Option[ProviderSelection] = None)
    extends Setup {

  def withClassloader(classLoader: ClassLoader): BootstrapSetup =
    new BootstrapSetup(Some(classLoader), config, defaultExecutionContext, actorRefProvider)

  def withConfig(config: Config): BootstrapSetup =
    new BootstrapSetup(classLoader, Some(config), defaultExecutionContext, actorRefProvider)

  def withDefaultExecutionContext(executionContext: ExecutionContext): BootstrapSetup =
    new BootstrapSetup(classLoader, config, Some(executionContext), actorRefProvider)

  def withActorRefProvider(name: ProviderSelection): BootstrapSetup =
    new BootstrapSetup(classLoader, config, defaultExecutionContext, Some(name))

}

object ActorSystem {

  val Version: String = akka.Version.current // generated file

  /**
   * Creates a new ActorSystem with the name "default",
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   * Then it loads the default reference configuration using the ClassLoader.
   */
  def create(): ActorSystem = apply()

  /**
   * Creates a new ActorSystem with the specified name,
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   * Then it loads the default reference configuration using the ClassLoader.
   */
  def create(name: String): ActorSystem = apply(name)

  /**
   * Java API: Creates a new actor system with the specified name and settings
   * The core actor system settings are defined in [[BootstrapSetup]]
   */
  def create(name: String, setups: ActorSystemSetup): ActorSystem = apply(name, setups)

  /**
   * Java API: Shortcut for creating an actor system with custom bootstrap settings.
   * Same behavior as calling `ActorSystem.create(name, ActorSystemSetup.create(bootstrapSettings))`
   */
  def create(name: String, bootstrapSetup: BootstrapSetup): ActorSystem =
    create(name, ActorSystemSetup.create(bootstrapSetup))

  /**
   * Creates a new ActorSystem with the specified name, and the specified Config, then
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def create(name: String, config: Config): ActorSystem = apply(name, config)

  /**
   * Creates a new ActorSystem with the specified name, the specified Config, and specified ClassLoader
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def create(name: String, config: Config, classLoader: ClassLoader): ActorSystem = apply(name, config, classLoader)

  /**
   * Creates a new ActorSystem with the specified name, the specified Config, the specified ClassLoader,
   * and the specified ExecutionContext. The ExecutionContext will be used as the default executor inside this ActorSystem.
   * If `null` is passed in for the Config, ClassLoader and/or ExecutionContext parameters, the respective default value
   * will be used. If no Config is given, the default reference config will be obtained from the ClassLoader.
   * If no ClassLoader is given, it obtains the current ClassLoader by first inspecting the current
   * threads' getContextClassLoader, then tries to walk the stack to find the callers class loader, then
   * falls back to the ClassLoader associated with the ActorSystem class. If no ExecutionContext is given, the
   * system will fallback to the executor configured under "akka.actor.default-dispatcher.default-executor.fallback".
   * Note that the given ExecutionContext will be used by all dispatchers that have been configured with
   * executor = "default-executor", including those that have not defined the executor setting and thereby fallback
   * to the default of "default-dispatcher.executor".
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def create(
      name: String,
      config: Config,
      classLoader: ClassLoader,
      defaultExecutionContext: ExecutionContext): ActorSystem =
    apply(name, Option(config), Option(classLoader), Option(defaultExecutionContext))

  /**
   * Creates a new ActorSystem with the name "default",
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   * Then it loads the default reference configuration using the ClassLoader.
   */
  def apply(): ActorSystem = apply("default")

  /**
   * Creates a new ActorSystem with the specified name,
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   * Then it loads the default reference configuration using the ClassLoader.
   */
  def apply(name: String): ActorSystem = apply(name, None, None, None)

  /**
   * Scala API: Creates a new actor system with the specified name and settings
   * The core actor system settings are defined in [[BootstrapSetup]]
   */
  def apply(name: String, setup: ActorSystemSetup): ActorSystem = {
    val bootstrapSettings = setup.get[BootstrapSetup]
    val cl = bootstrapSettings.flatMap(_.classLoader).getOrElse(findClassLoader())
    val appConfig = bootstrapSettings.flatMap(_.config).getOrElse(ConfigFactory.load(cl))
    val defaultEC = bootstrapSettings.flatMap(_.defaultExecutionContext)

    new ActorSystemImpl(name, appConfig, cl, defaultEC, None, setup).start()
  }

  /**
   * Scala API: Shortcut for creating an actor system with custom bootstrap settings.
   * Same behavior as calling `ActorSystem(name, ActorSystemSetup(bootstrapSetup))`
   */
  def apply(name: String, bootstrapSetup: BootstrapSetup): ActorSystem =
    create(name, ActorSystemSetup.create(bootstrapSetup))

  /**
   * Creates a new ActorSystem with the specified name, and the specified Config, then
   * obtains the current ClassLoader by first inspecting the current threads' getContextClassLoader,
   * then tries to walk the stack to find the callers class loader, then falls back to the ClassLoader
   * associated with the ActorSystem class.
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def apply(name: String, config: Config): ActorSystem = apply(name, Option(config), None, None)

  /**
   * Creates a new ActorSystem with the specified name, the specified Config, and specified ClassLoader
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def apply(name: String, config: Config, classLoader: ClassLoader): ActorSystem =
    apply(name, Option(config), Option(classLoader), None)

  /**
   * Creates a new ActorSystem with the specified name,
   * the specified ClassLoader if given, otherwise obtains the current ClassLoader by first inspecting the current
   * threads' getContextClassLoader, then tries to walk the stack to find the callers class loader, then
   * falls back to the ClassLoader associated with the ActorSystem class.
   * If an ExecutionContext is given, it will be used as the default executor inside this ActorSystem.
   * If no ExecutionContext is given, the system will fallback to the executor configured under "akka.actor.default-dispatcher.default-executor.fallback".
   * The system will use the passed in config, or falls back to the default reference configuration using the ClassLoader.
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  def apply(
      name: String,
      config: Option[Config] = None,
      classLoader: Option[ClassLoader] = None,
      defaultExecutionContext: Option[ExecutionContext] = None): ActorSystem =
    apply(name, ActorSystemSetup(BootstrapSetup(classLoader, config, defaultExecutionContext)))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] object Settings {

    /**
     * INTERNAL API
     *
     * When using Akka Typed the Slf4jLogger should be used by default.
     * Looking for config property `akka.use-slf4j` (defined in akka-actor-typed) and
     * that `Slf4jLogger` (akka-slf4j) is in  classpath.
     * Then adds `Slf4jLogger` to configured loggers and removes `DefaultLogger`.
     */
    @InternalApi private[akka] def amendSlf4jConfig(config: Config, dynamicAccess: DynamicAccess): Config = {
      val slf4jLoggerClassName = "akka.event.slf4j.Slf4jLogger"
      val slf4jLoggingFilterClassName = "akka.event.slf4j.Slf4jLoggingFilter"
      val loggersConfKey = "akka.loggers"
      val loggingFilterConfKey = "akka.logging-filter"
      val configuredLoggers = immutableSeq(config.getStringList(loggersConfKey))
      val configuredLoggingFilter = config.getString(loggingFilterConfKey)

      val loggingFilterAlreadyConfigured =
        configuredLoggingFilter == slf4jLoggingFilterClassName || configuredLoggingFilter != classOf[
            DefaultLoggingFilter].getName

      def newLoggingFilterConfStr = s"""$loggingFilterConfKey = "$slf4jLoggingFilterClassName""""

      if (configuredLoggers.contains(slf4jLoggerClassName)) {
        // already configured explicitly
        if (loggingFilterAlreadyConfigured)
          config
        else
          ConfigFactory.parseString(newLoggingFilterConfStr).withFallback(config)
      } else {
        val confKey = "akka.use-slf4j"
        if (config.hasPath(confKey) && config.getBoolean(confKey) && dynamicAccess.classIsOnClasspath(
              slf4jLoggerClassName)) {
          val newLoggers = slf4jLoggerClassName +: configuredLoggers.filterNot(_ == classOf[DefaultLogger].getName)
          val newLoggersConfStr = s"$loggersConfKey = [${newLoggers.mkString("\"", "\", \"", "\"")}]"
          val newConfStr =
            if (loggingFilterAlreadyConfigured) newLoggersConfStr
            else newLoggersConfStr + "\n" + newLoggingFilterConfStr
          ConfigFactory.parseString(newConfStr).withFallback(config)
        } else
          config
      }
    }
  }

  /**
   * Settings are the overall ActorSystem Settings which also provides a convenient access to the Config object.
   *
   * For more detailed information about the different possible configuration options, look in the Akka Documentation under "Configuration"
   *
   * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
   */
  class Settings(classLoader: ClassLoader, cfg: Config, final val name: String, val setup: ActorSystemSetup) {

    def this(classLoader: ClassLoader, cfg: Config, name: String) = this(classLoader, cfg, name, ActorSystemSetup())

    /**
     * The backing Config of this ActorSystem's Settings
     *
     * @see <a href="https://lightbend.github.io/config/latest/api/index.html" target="_blank">The Typesafe Config Library API Documentation</a>
     */
    final val config: Config = {
      cfg.checkValid(
        ConfigFactory
          .defaultReference(classLoader)
          .withoutPath(Dispatchers.InternalDispatcherId), // allow this to be both string and config object
        "akka")
      cfg
    }

    import config._

    import akka.util.Helpers.ConfigOps

    final val ConfigVersion: String = getString("akka.version")

    private final val providerSelectionSetup = setup
      .get[BootstrapSetup]
      .flatMap(_.actorRefProvider)
      .map(_.identifier)
      .getOrElse(getString("akka.actor.provider"))

    final val ProviderSelectionType: ProviderSelection = ProviderSelection(providerSelectionSetup)

    final val ProviderClass: String = ProviderSelectionType.fqcn

    final val HasCluster: Boolean = ProviderSelectionType.hasCluster

    final val SupervisorStrategyClass: String = getString("akka.actor.guardian-supervisor-strategy")
    final val CreationTimeout: Timeout = Timeout(config.getMillisDuration("akka.actor.creation-timeout"))
    final val UnstartedPushTimeout: Timeout = Timeout(config.getMillisDuration("akka.actor.unstarted-push-timeout"))

    final val AllowJavaSerialization: Boolean = getBoolean("akka.actor.allow-java-serialization")
    @deprecated("Always enabled from Akka 2.6.0", "2.6.0")
    final val EnableAdditionalSerializationBindings: Boolean = true
    final val SerializeAllMessages: Boolean = getBoolean("akka.actor.serialize-messages")
    final val SerializeAllCreators: Boolean = getBoolean("akka.actor.serialize-creators")
    final val NoSerializationVerificationNeededClassPrefix: Set[String] = {
      import scala.jdk.CollectionConverters._
      getStringList("akka.actor.no-serialization-verification-needed-class-prefix").asScala.toSet
    }

    final val LogLevel: String = getString("akka.loglevel")
    final val StdoutLogLevel: String = getString("akka.stdout-loglevel")
    final val Loggers: immutable.Seq[String] = immutableSeq(getStringList("akka.loggers"))
    final val LoggersDispatcher: String = getString("akka.loggers-dispatcher")
    final val LoggingFilter: String = getString("akka.logging-filter")
    final val LoggerStartTimeout: Timeout = Timeout(config.getMillisDuration("akka.logger-startup-timeout"))
    final val LogConfigOnStart: Boolean = config.getBoolean("akka.log-config-on-start")
    final val LogDeadLetters: Int = toRootLowerCase(config.getString("akka.log-dead-letters")) match {
      case "off" | "false" => 0
      case "on" | "true"   => Int.MaxValue
      case _               => config.getInt("akka.log-dead-letters")
    }
    final val LogDeadLettersDuringShutdown: Boolean = config.getBoolean("akka.log-dead-letters-during-shutdown")
    final val LogDeadLettersSuspendDuration: Duration = {
      val key = "akka.log-dead-letters-suspend-duration"
      toRootLowerCase(config.getString(key)) match {
        case "infinite" => Duration.Inf
        case _          => config.getMillisDuration(key)
      }
    }

    final val AddLoggingReceive: Boolean = getBoolean("akka.actor.debug.receive")
    final val DebugAutoReceive: Boolean = getBoolean("akka.actor.debug.autoreceive")
    final val DebugLifecycle: Boolean = getBoolean("akka.actor.debug.lifecycle")
    final val FsmDebugEvent: Boolean = getBoolean("akka.actor.debug.fsm")
    final val DebugEventStream: Boolean = getBoolean("akka.actor.debug.event-stream")
    final val DebugUnhandledMessage: Boolean = getBoolean("akka.actor.debug.unhandled")
    final val DebugRouterMisconfiguration: Boolean = getBoolean("akka.actor.debug.router-misconfiguration")

    final val Home: Option[String] = config.getString("akka.home") match {
      case "" => None
      case x  => Some(x)
    }

    final val SchedulerClass: String = getString("akka.scheduler.implementation")
    final val Daemonicity: Boolean = getBoolean("akka.daemonic")
    final val JvmExitOnFatalError: Boolean = getBoolean("akka.jvm-exit-on-fatal-error")
    final val JvmShutdownHooks: Boolean = getBoolean("akka.jvm-shutdown-hooks")
    final val FailMixedVersions: Boolean = getBoolean("akka.fail-mixed-versions")

    final val CoordinatedShutdownTerminateActorSystem: Boolean = getBoolean(
      "akka.coordinated-shutdown.terminate-actor-system")
    final val CoordinatedShutdownRunByActorSystemTerminate: Boolean = getBoolean(
      "akka.coordinated-shutdown.run-by-actor-system-terminate")
    if (CoordinatedShutdownRunByActorSystemTerminate && !CoordinatedShutdownTerminateActorSystem)
      throw new ConfigurationException(
        "akka.coordinated-shutdown.run-by-actor-system-terminate=on and " +
        "akka.coordinated-shutdown.terminate-actor-system=off is not a supported configuration combination.")

    final val DefaultVirtualNodesFactor: Int = getInt("akka.actor.deployment.default.virtual-nodes-factor")

    if (ConfigVersion != Version)
      throw new akka.ConfigurationException(
        "Akka JAR version [" + Version + "] does not match the provided config version [" + ConfigVersion + "]")

    /**
     * Returns the String representation of the Config that this Settings is backed by
     */
    override def toString: String =
      config.root.render(ConfigRenderOptions.defaults().setShowEnvVariableValues(false))

  }

  private[akka] def findClassLoader(): ClassLoader = Reflect.findClassLoader()
}

/**
 * An actor system is a hierarchical group of actors which share common
 * configuration, e.g. dispatchers, deployments, remote capabilities and
 * addresses. It is also the entry point for creating or looking up actors.
 *
 * There are several possibilities for creating actors (see [[akka.actor.Props]]
 * for details on `props`):
 *
 * {{{
 * // Java or Scala
 * system.actorOf(props, "name")
 * system.actorOf(props)
 *
 * // Scala
 * system.actorOf(Props[MyActor], "name")
 * system.actorOf(Props(classOf[MyActor], arg1, arg2), "name")
 *
 * // Java
 * system.actorOf(Props.create(MyActor.class), "name");
 * system.actorOf(Props.create(MyActor.class, arg1, arg2), "name");
 * }}}
 *
 * Where no name is given explicitly, one will be automatically generated.
 *
 * <b><i>Important Notice:</i></b>
 *
 * This class is not meant to be extended by user code. If you want to
 * actually roll your own Akka, it will probably be better to look into
 * extending [[akka.actor.ExtendedActorSystem]] instead, but beware that you
 * are completely on your own in that case!
 */
abstract class ActorSystem extends ActorRefFactory with ClassicActorSystemProvider {
  import ActorSystem._

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
   * Construct a path below the application guardian to be used with [[ActorSystem#actorSelection]].
   */
  def /(name: String): ActorPath

  /**
   * Java API: Create a new child actor path.
   */
  def child(child: String): ActorPath = /(child)

  /**
   * Construct a path below the application guardian to be used with [[ActorSystem#actorSelection]].
   */
  def /(name: Iterable[String]): ActorPath

  /**
   * Java API: Recursively create a descendant’s path by appending all child names.
   */
  def descendant(names: java.lang.Iterable[String]): ActorPath = /(immutableSeq(names))

  /**
   * Start-up time in milliseconds since the epoch.
   */
  val startTime: Long = System.currentTimeMillis

  /**
   * Up-time of this actor system in seconds.
   */
  def uptime: Long = (System.currentTimeMillis - startTime) / 1000

  /**
   * Main event bus of this actor system, used for example for logging.
   */
  def eventStream: EventStream

  /**
   * Java API: Main event bus of this actor system, used for example for logging.
   */
  def getEventStream: EventStream = eventStream

  /**
   * Convenient logging adapter for logging to the [[ActorSystem#eventStream]].
   */
  def log: LoggingAdapter

  /**
   * Actor reference where messages are re-routed to which were addressed to
   * stopped or non-existing actors. Delivery to this actor is done on a best
   * effort basis and hence not strictly guaranteed.
   */
  def deadLetters: ActorRef

  /**
   * Light-weight scheduler for running asynchronous tasks after some deadline
   * in the future. Not terribly precise but cheap.
   */
  def scheduler: Scheduler

  /**
   * Java API: Light-weight scheduler for running asynchronous tasks after some deadline
   * in the future. Not terribly precise but cheap.
   */
  def getScheduler: Scheduler = scheduler

  /**
   * Helper object for looking up configured dispatchers.
   */
  def dispatchers: Dispatchers

  /**
   * Default dispatcher as configured. This dispatcher is used for all actors
   * in the actor system which do not have a different dispatcher configured
   * explicitly.
   * Importing this member will place the default MessageDispatcher in scope.
   */
  implicit def dispatcher: ExecutionContextExecutor

  /**
   * Java API: Default dispatcher as configured. This dispatcher is used for all actors
   * in the actor system which do not have a different dispatcher configured
   * explicitly.
   * Importing this member will place the default MessageDispatcher in scope.
   */
  def getDispatcher: ExecutionContextExecutor = dispatcher

  /**
   * Helper object for looking up configured mailbox types.
   */
  def mailboxes: Mailboxes

  /**
   * Register a block of code (callback) to run after [[ActorSystem.terminate]] has been issued and
   * all actors in this actor system have been stopped.
   * Multiple code blocks may be registered by calling this method multiple times.
   * The callbacks will be run sequentially in reverse order of registration, i.e.
   * last registration is run first.
   * Note that ActorSystem will not terminate until all the registered callbacks are finished.
   *
   * Throws a RejectedExecutionException if the System has already been terminated or if termination has been initiated.
   *
   * Scala API
   */
  def registerOnTermination[T](code: => T): Unit

  /**
   * Java API: Register a block of code (callback) to run after [[ActorSystem.terminate]] has been issued and
   * all actors in this actor system have been stopped.
   * Multiple code blocks may be registered by calling this method multiple times.
   * The callbacks will be run sequentially in reverse order of registration, i.e.
   * last registration is run first.
   * Note that ActorSystem will not terminate until all the registered callbacks are finished.
   *
   * Throws a RejectedExecutionException if the System has already been terminated or if termination has been initiated.
   */
  def registerOnTermination(code: Runnable): Unit

  /**
   * Terminates this actor system by running [[CoordinatedShutdown]] with reason
   * [[CoordinatedShutdown.ActorSystemTerminateReason]].
   *
   * If `akka.coordinated-shutdown.run-by-actor-system-terminate` is configured to `off`
   * it will not run `CoordinatedShutdown`, but the `ActorSystem` and its actors
   * will still be terminated.
   *
   * This will stop the guardian actor, which in turn
   * will recursively stop all its child actors, and finally the system guardian
   * (below which the logging actors reside) and then execute all registered
   * termination handlers (see [[ActorSystem#registerOnTermination]]).
   * Be careful to not schedule any operations on completion of the returned future
   * using the dispatcher of this actor system as it will have been shut down before the
   * future completes.
   */
  def terminate(): Future[Terminated]

  /**
   * Returns a Future which will be completed after the ActorSystem has been terminated
   * and termination hooks have been executed. If you registered any callback with
   * [[ActorSystem#registerOnTermination]], the returned Future from this method will not complete
   * until all the registered callbacks are finished. Be careful to not schedule any operations,
   * such as `onComplete`, on the dispatchers (`ExecutionContext`) of this actor system as they
   * will have been shut down before this future completes.
   */
  def whenTerminated: Future[Terminated]

  /**
   * Returns a CompletionStage which will be completed after the ActorSystem has been terminated
   * and termination hooks have been executed. If you registered any callback with
   * [[ActorSystem#registerOnTermination]], the returned CompletionStage from this method will not complete
   * until all the registered callbacks are finished. Be careful to not schedule any operations,
   * such as `thenRunAsync`, on the dispatchers (`Executor`) of this actor system as they
   * will have been shut down before this CompletionStage completes.
   */
  def getWhenTerminated: CompletionStage[Terminated]

  /**
   * Registers the provided extension and creates its payload, if this extension isn't already registered
   * This method has putIfAbsent-semantics, this method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def registerExtension[T <: Extension](ext: ExtensionId[T]): T

  /**
   * Returns the payload that is associated with the provided extension
   * throws an IllegalStateException if it is not registered.
   * This method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def extension[T <: Extension](ext: ExtensionId[T]): T

  /**
   * Returns whether the specified extension is already registered, this method can potentially block, waiting for the initialization
   * of the payload, if is in the process of registration from another Thread of execution
   */
  def hasExtension(ext: ExtensionId[_ <: Extension]): Boolean

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

/**
 * More powerful interface to the actor system’s implementation which is presented to extensions (see [[akka.actor.Extension]]).
 *
 * <b><i>Important Notice:</i></b>
 *
 * This class is not meant to be extended by user code. If you want to
 * actually roll your own Akka, beware that you are completely on your own in
 * that case!
 */
@DoNotInherit
abstract class ExtendedActorSystem extends ActorSystem {

  /**
   * The ActorRefProvider is the only entity which creates all actor references within this actor system.
   */
  def provider: ActorRefProvider

  /**
   * The top-level supervisor of all actors created using system.actorOf(...).
   */
  def guardian: InternalActorRef

  /**
   * The top-level supervisor of all system-internal services like logging.
   */
  def systemGuardian: InternalActorRef

  /**
   * Create an actor in the "/system" namespace. This actor will be shut down
   * during system.terminate only after all user actors have terminated.
   *
   * This is only intended to be used by libraries (and Akka itself).
   * Applications should use ordinary `actorOf`.
   */
  def systemActorOf(props: Props, name: String): ActorRef

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
  def dynamicAccess: DynamicAccess

  /**
   * Filter of log events that is used by the LoggingAdapter before
   * publishing log events to the eventStream
   */
  def logFilter: LoggingFilter

  /**
   * For debugging: traverse actor hierarchy and make string representation.
   * Careful, this may OOM on large actor systems, and it is only meant for
   * helping debugging in case something already went terminally wrong.
   */
  private[akka] def printTree: String

  /**
   * Random uid assigned at ActorSystem startup. When using Akka Cluster this
   * uid is used together with an [[akka.actor.Address]] to be able to distinguish restarted
   * actor system using the same host and port.
   */
  def uid: Long

  /**
   * INTERNAL API: final step of `terminate()`
   */
  @InternalApi private[akka] def finalTerminate(): Unit

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def isTerminating(): Boolean

}

/**
 * Internal API
 */
@InternalApi
private[akka] class ActorSystemImpl(
    val name: String,
    applicationConfig: Config,
    classLoader: ClassLoader,
    defaultExecutionContext: Option[ExecutionContext],
    val guardianProps: Option[Props],
    setup: ActorSystemSetup)
    extends ExtendedActorSystem {

  if (!name.matches("""^[a-zA-Z0-9][a-zA-Z0-9-_]*$"""))
    throw new IllegalArgumentException(
      "invalid ActorSystem name [" + name +
      "], must contain only word characters (i.e. [a-zA-Z0-9] plus non-leading '-' or '_')")

  import ActorSystem._

  @volatile private var logDeadLetterListener: Option[ActorRef] = None

  private val _dynamicAccess: DynamicAccess = createDynamicAccess()

  /**
   * Optimistic optimization: Tries to avoid going through the extension infrastructure if possible when using
   * the typed system. Will contain a akka.actor.typed.ActorSystem if set, should not be touched by anything but
   * the typed ActorSystemAdapter.
   */
  private[akka] var typedSystem: OptionVal[AnyRef] = OptionVal.None

  final val settings: Settings = {
    val config = Settings.amendSlf4jConfig(
      applicationConfig.withFallback(ConfigFactory.defaultReference(classLoader)),
      _dynamicAccess)
    new Settings(classLoader, config, name, setup)
  }

  // initialized from `start()`
  @volatile private var _licenseKeyExpiry: Option[LocalDate] = None

  override def licenseKeyExpiry: Option[LocalDate] = _licenseKeyExpiry

  override def getLicenseKeyExpiry: Optional[LocalDate] = _licenseKeyExpiry.toJava

  private def setLicenseKeyExpiry(expiry: LocalDate): Unit = {
    _licenseKeyExpiry match {
      case None =>
        _licenseKeyExpiry = Some(expiry)
      case Some(exp) =>
        if (expiry.isBefore(exp))
          _licenseKeyExpiry = Some(expiry)
    }
  }

  override def uid: Long = {
    try {
      provider.systemUid
    } catch {
      case NonFatal(exc) =>
        // could be NPE in RARP if transport not initialized yet
        throw new IllegalStateException(
          "uid accessed before provider has been initialized. " +
          "This is a bug, please report at https://github.com/akka/akka-core/issues",
          exc)
    }
  }

  protected def uncaughtExceptionHandler: Thread.UncaughtExceptionHandler =
    new Thread.UncaughtExceptionHandler() {
      def uncaughtException(thread: Thread, cause: Throwable): Unit = {
        cause match {
          case NonFatal(_) | _: InterruptedException | _: NotImplementedError | _: ControlThrowable =>
            log.error(cause, "Uncaught error from thread [{}]", thread.getName)
          case _ =>
            if (cause.isInstanceOf[IncompatibleClassChangeError] && cause.getMessage.startsWith("akka"))
              System.err.println(
                s"""Detected ${cause.getClass.getName} error, which MAY be caused by incompatible Akka versions on the classpath.
                   | Please note that a given Akka version MUST be the same across all modules of Akka that you are using,
                   | e.g. if you use akka-actor [${akka.Version.current} (resolved from current classpath)] all other core
                   | Akka modules MUST be of the same version. External projects like Alpakka, Persistence plugins or Akka
                   | HTTP etc. have their own version numbers - please make sure you're using a compatible set of libraries.
                 """.stripMargin.replaceAll("[\r\n]", ""))

            if (settings.JvmExitOnFatalError)
              try logFatalError("shutting down JVM since 'akka.jvm-exit-on-fatal-error' is enabled for", cause, thread)
              finally System.exit(-1)
            else
              try logFatalError("shutting down", cause, thread)
              finally terminate()
        }
      }

      @inline
      private def logFatalError(message: String, cause: Throwable, thread: Thread): Unit = {
        // First log to stderr as this has the best chance to get through in an 'emergency panic' situation:
        import System.err
        err.print("Uncaught error from thread [")
        err.print(thread.getName)
        err.print("]: ")
        err.print(cause.getMessage)
        err.print(", ")
        err.print(message)
        err.print(" ActorSystem[")
        err.print(name)
        err.println("]")
        System.err.flush()
        cause.printStackTrace(System.err)
        System.err.flush()

        // Also log using the normal infrastructure - hope for the best:
        markerLogging.error(
          LogMarker.Security,
          cause,
          "Uncaught error from thread [{}]: " + cause.getMessage + ", " + message + " ActorSystem[{}]",
          thread.getName,
          name)
      }
    }

  final val threadFactory: MonitorableThreadFactory =
    MonitorableThreadFactory(name, settings.Daemonicity, Option(classLoader), uncaughtExceptionHandler)

  /**
   * This is an extension point: by overriding this method, subclasses can
   * control all reflection activities of an actor system.
   */
  protected def createDynamicAccess(): DynamicAccess = new ReflectiveDynamicAccess(classLoader)

  def dynamicAccess: DynamicAccess = _dynamicAccess

  def logConfiguration(): Unit = log.info(settings.toString)

  protected def systemImpl: ActorSystemImpl = this

  def systemActorOf(props: Props, name: String): ActorRef =
    systemGuardian.underlying.attachChild(props, name, systemService = true)

  def actorOf(props: Props, name: String): ActorRef =
    if (guardianProps.isEmpty) guardian.underlying.attachChild(props, name, systemService = false)
    else {
      val message =
        if (isTypedGuardian)
          s"cannot create top-level actor [$name] from the outside on a typed ActorSystem.  In a typed ActorSystem, " +
          "top-level actors should be spawned as children of the guardian behavior; if this is not possible (e.g. this " +
          "actorOf call is in a library), a classic ActorSystem should be created and used to spawn top-level actors."
        else s"cannot create top-level actor [$name] from the outside on ActorSystem with custom user guardian"

      throw new UnsupportedOperationException(message)
    }

  def actorOf(props: Props): ActorRef =
    if (guardianProps.isEmpty) guardian.underlying.attachChild(props, systemService = false)
    else {
      val message =
        if (isTypedGuardian)
          "cannot create top-level actor from the outside on a typed ActorSystem.  In a typed ActorSystem, " +
          "top-level actors should be spawned as children of the guardian behavior; if this is not possible (e.g. this " +
          "actorOf call is in a library), a classic ActorSystem should be created and used to spawn top-level actors."
        else "cannot create top-level actor from the outside on ActorSystem with custom user guardian"

      throw new UnsupportedOperationException(message)
    }

  def stop(actor: ActorRef): Unit = {
    val path = actor.path
    val guard = guardian.path
    val sys = systemGuardian.path
    path.parent match {
      case `guard` => guardian ! StopChild(actor)
      case `sys`   => systemGuardian ! StopChild(actor)
      case _       => actor.asInstanceOf[InternalActorRef].stop()
    }
  }

  import settings._

  // this provides basic logging (to stdout) until .start() is called below
  val eventStream = new EventStream(this, DebugEventStream)
  eventStream.startStdoutLogger(settings)

  val logFilter: LoggingFilter = {
    val arguments = Vector(classOf[Settings] -> settings, classOf[EventStream] -> eventStream)
    dynamicAccess.createInstanceFor[LoggingFilter](LoggingFilter, arguments).get
  }

  private[this] val markerLogging =
    new MarkerLoggingAdapter(eventStream, getClass.getName + "(" + name + ")", this.getClass, logFilter)
  val log: LoggingAdapter = markerLogging

  val scheduler: Scheduler = createScheduler()

  val provider: ActorRefProvider = try {
    val arguments = Vector(
      classOf[String] -> name,
      classOf[Settings] -> settings,
      classOf[EventStream] -> eventStream,
      classOf[DynamicAccess] -> dynamicAccess)

    dynamicAccess.createInstanceFor[ActorRefProvider](ProviderClass, arguments).get
  } catch {
    case NonFatal(e) =>
      Try(stopScheduler())
      throw e
  }

  def deadLetters: ActorRef = provider.deadLetters

  val mailboxes: Mailboxes = new Mailboxes(settings, eventStream, dynamicAccess, deadLetters)

  val dispatchers: Dispatchers = new Dispatchers(
    settings,
    DefaultDispatcherPrerequisites(
      threadFactory,
      eventStream,
      scheduler,
      dynamicAccess,
      settings,
      mailboxes,
      defaultExecutionContext),
    log)

  val dispatcher: ExecutionContextExecutor = dispatchers.defaultGlobalDispatcher

  private[this] final val terminationCallbacks = new TerminationCallbacks(provider.terminationFuture)(dispatcher)

  override def whenTerminated: Future[Terminated] = terminationCallbacks.terminationFuture
  override def getWhenTerminated: CompletionStage[Terminated] = whenTerminated.asJava
  def lookupRoot: InternalActorRef = provider.rootGuardian
  def guardian: LocalActorRef = provider.guardian
  def systemGuardian: LocalActorRef = provider.systemGuardian

  def /(actorName: String): ActorPath = guardian.path / actorName
  def /(path: Iterable[String]): ActorPath = guardian.path / path

  override def classicSystem: ActorSystem = this

  // Used for ManifestInfo.checkSameVersion
  private def allModules: List[String] =
    List(
      "akka-actor",
      "akka-actor-testkit-typed",
      "akka-actor-typed",
      "akka-cluster",
      "akka-cluster-metrics",
      "akka-cluster-sharding",
      "akka-cluster-sharding-typed",
      "akka-cluster-tools",
      "akka-cluster-typed",
      "akka-coordination",
      "akka-discovery",
      "akka-distributed-data",
      "akka-testkit",
      "akka-multi-node-testkit",
      "akka-persistence",
      "akka-persistence-query",
      "akka-persistence-shared",
      "akka-persistence-testkit",
      "akka-persistence-typed",
      "akka-pki",
      "akka-protobuf",
      "akka-protobuf-v3",
      "akka-remote",
      "akka-serialization-jackson",
      "akka-slf4j",
      "akka-stream",
      "akka-stream-testkit",
      "akka-stream-typed",
      "akka-stream-testkit")

  @volatile private var _initialized = false

  /**
   *  Asserts that the ActorSystem has been fully initialized. Can be used to guard code blocks that might accidentally
   *  be run during initialization but require a fully initialized ActorSystem before proceeding.
   */
  def assertInitialized(): Unit =
    if (!_initialized)
      throw new IllegalStateException(
        "The calling code expected that the ActorSystem was initialized but it wasn't yet. " +
        "This is probably a bug in the ActorSystem initialization sequence often related to initialization of extensions. " +
        "Please report at https://github.com/akka/akka-core/issues.")
  private lazy val _start: this.type = try {

    registerOnTermination(stopScheduler())
    // the provider is expected to start default loggers, LocalActorRefProvider does this
    provider.init(this)
    // at this point it should be initialized "enough" for most extensions that we might want to guard against otherwise
    _initialized = true

    if (settings.LogDeadLetters > 0)
      logDeadLetterListener = Some(systemActorOf(Props(new DeadLetterListener), "deadLetterListener"))
    eventStream.startUnsubscriber()
    checkLicenseKey()
    ManifestInfo(this).checkSameVersion("Akka", allModules, logWarning = true)
    if (!terminating)
      loadExtensions()
    if (LogConfigOnStart) logConfiguration()
    this
  } catch {
    case NonFatal(e) =>
      try terminate()
      catch { case NonFatal(_) => Try(stopScheduler()) }
      throw e
  }

  def start(): this.type = _start
  def registerOnTermination[T](code: => T): Unit = { registerOnTermination(new Runnable { def run() = code }) }
  def registerOnTermination(code: Runnable): Unit = { terminationCallbacks.add(code) }

  @volatile private var terminating = false

  override def terminate(): Future[Terminated] = {
    terminating = true
    if (settings.CoordinatedShutdownRunByActorSystemTerminate && !aborting) {
      // Note that the combination CoordinatedShutdownRunByActorSystemTerminate==true &&
      // CoordinatedShutdownTerminateActorSystem==false is disallowed, checked in Settings.
      // It's not a combination that is valuable to support and it would be complicated to
      // protect against concurrency race conditions between calls to ActorSystem.terminate()
      // and CoordinateShutdown.run()

      // it will call finalTerminate() at the end
      CoordinatedShutdown(this).run(CoordinatedShutdown.ActorSystemTerminateReason)
    } else {
      finalTerminate()
    }
    whenTerminated
  }

  override private[akka] def finalTerminate(): Unit = {
    terminating = true
    // these actions are idempotent
    if (!settings.LogDeadLettersDuringShutdown) logDeadLetterListener.foreach(stop)
    guardian.stop()
  }

  override private[akka] def isTerminating(): Boolean = {
    terminating || aborting || CoordinatedShutdown(this).shutdownReason().isDefined
  }

  @volatile var aborting = false

  /**
   * This kind of shutdown attempts to bring the system down and release its
   * resources more forcefully than plain shutdown. For example it will not
   * run CoordinatedShutdown and not wait for remote-deployed child actors to
   * terminate before terminating their parents.
   */
  def abort(): Unit = {
    aborting = true
    terminate()
  }

  //#create-scheduler
  /**
   * Create the scheduler service. This one needs one special behavior: if
   * Closeable, it MUST execute all outstanding tasks upon .close() in order
   * to properly shutdown all dispatchers.
   *
   * Furthermore, this timer service MUST throw IllegalStateException if it
   * cannot schedule a task. Once scheduled, the task MUST be executed. If
   * executed upon close(), the task may execute before its timeout.
   */
  protected def createScheduler(): Scheduler =
    dynamicAccess
      .createInstanceFor[Scheduler](
        settings.SchedulerClass,
        immutable.Seq(
          classOf[Config] -> settings.config,
          classOf[LoggingAdapter] -> log,
          classOf[ThreadFactory] -> threadFactory.withName(threadFactory.name + "-scheduler")))
      .get
  //#create-scheduler

  /*
   * This is called after the last actor has signaled its termination, i.e.
   * after the last dispatcher has had its chance to schedule its shutdown
   * action.
   */
  protected def stopScheduler(): Unit = scheduler match {
    case x: Closeable => x.close()
    case _            =>
  }

  // For each ExtensionId, either:
  // 1) a CountDownLatch (if it's still in the process of being registered),
  // 2) a Throwable (if it failed initializing), or
  // 3) the registered extension.
  private val extensions = new ConcurrentHashMap[ExtensionId[_], AnyRef]

  /**
   * Returns any extension registered to the specified Extension or returns null if not registered
   */
  @tailrec
  private def findExtension[T <: Extension](ext: ExtensionId[T]): T = extensions.get(ext) match {
    case c: CountDownLatch =>
      blocking {
        val awaitMillis = settings.CreationTimeout.duration.toMillis
        if (!c.await(awaitMillis, TimeUnit.MILLISECONDS))
          throw new IllegalStateException(
            s"Initialization of [$ext] took more than [$awaitMillis ms]. " +
            (if (ext == SerializationExtension)
               "A serializer must not access the SerializationExtension from its constructor. Use lazy init."
             else "Could be deadlock due to cyclic initialization of extensions."))
      }
      findExtension(ext) //Registration in process, await completion and retry
    case t: Throwable => throw t //Initialization failed, throw same again
    case other =>
      other.asInstanceOf[T] //could be a T or null, in which case we return the null as T
  }

  @tailrec
  final def registerExtension[T <: Extension](ext: ExtensionId[T]): T = {
    findExtension(ext) match {
      case null => //Doesn't already exist, commence registration
        val inProcessOfRegistration = new CountDownLatch(1)
        extensions.putIfAbsent(ext, inProcessOfRegistration) match { // Signal that registration is in process
          case null =>
            try { // Signal was successfully sent
              ext.createExtension(this) match { // Create and initialize the extension
                case null =>
                  throw new IllegalStateException(s"Extension instance created as 'null' for extension [$ext]")
                case instance =>
                  extensions.replace(ext, inProcessOfRegistration, instance) //Replace our in process signal with the initialized extension
                  instance //Profit!
              }
            } catch {
              case t: Throwable =>
                extensions.replace(ext, inProcessOfRegistration, t) //In case shit hits the fan, remove the inProcess signal
                throw t //Escalate to caller
            } finally {
              inProcessOfRegistration.countDown() //Always notify listeners of the inProcess signal
            }
          case _ =>
            registerExtension(ext) //Someone else is in process of registering an extension for this Extension, retry
        }
      case existing => existing.asInstanceOf[T]
    }
  }

  def extension[T <: Extension](ext: ExtensionId[T]): T = findExtension(ext) match {
    case null => throw new IllegalArgumentException(s"Trying to get non-registered extension [$ext]")
    case some => some.asInstanceOf[T]
  }

  def hasExtension(ext: ExtensionId[_ <: Extension]): Boolean = findExtension(ext) != null

  private def loadExtensions(): Unit = {

    /*
     * @param throwOnLoadFail
     *  Throw exception when an extension fails to load (needed for backwards compatibility.
     *    when the extension cannot be found at all we throw regardless of this setting)
     */
    def loadExtensions(key: String, throwOnLoadFail: Boolean): Unit = {

      immutableSeq(settings.config.getStringList(key)).foreach { fqcn =>
        dynamicAccess.getObjectFor[AnyRef](fqcn).recoverWith {
          case firstProblem =>
            dynamicAccess.createInstanceFor[AnyRef](fqcn, Nil).recoverWith { case _ => Failure(firstProblem) }
        } match {
          case Success(p: ExtensionIdProvider) =>
            registerExtension(p.lookup)
          case Success(p: ExtensionId[_]) =>
            registerExtension(p)
          case Success(_) =>
            if (!throwOnLoadFail) log.error("[{}] is not an 'ExtensionIdProvider' or 'ExtensionId', skipping...", fqcn)
            else throw new RuntimeException(s"[$fqcn] is not an 'ExtensionIdProvider' or 'ExtensionId'")
          case Failure(problem) =>
            if (!throwOnLoadFail) log.error(problem, "While trying to load extension [{}], skipping...", fqcn)
            else throw new RuntimeException(s"While trying to load extension [$fqcn]", problem)
        }
      }
    }

    loadExtensions("akka.library-extensions", throwOnLoadFail = true)
    loadExtensions("akka.extensions", throwOnLoadFail = false)
  }

  override def toString: String = lookupRoot.path.root.address.toString

  override def printTree: String = {
    def printNode(node: ActorRef, indent: String): String = {
      node match {
        case wc: ActorRefWithCell =>
          val cell = wc.underlying
          (if (indent.isEmpty) "-> " else indent.dropRight(1) + "⌊-> ") +
          node.path.name + " " + Logging.simpleName(node) + " " +
          (cell match {
            case real: ActorCell =>
              val realActor = real.actor
              if (realActor ne null) realActor.getClass else "null"
            case _ => Logging.simpleName(cell)
          }) +
          (cell match {
            case real: ActorCell => " status=" + real.mailbox.currentStatus
            case _               => ""
          }) +
          " " + (cell.childrenRefs match {
            case ChildrenContainer.TerminatingChildrenContainer(_, toDie, reason) =>
              "Terminating(" + reason + ")" +
              (toDie.toSeq.sorted.mkString("\n" + indent + "   |    toDie: ", "\n" + indent + "   |           ", ""))
            case x @ (ChildrenContainer.TerminatedChildrenContainer | ChildrenContainer.EmptyChildrenContainer) =>
              x.toString
            case n: ChildrenContainer.NormalChildrenContainer => n.c.size.toString + " children"
            case x                                            => Logging.simpleName(x)
          }) +
          (if (cell.childrenRefs.children.isEmpty) "" else "\n") +
          ({
            val children = cell.childrenRefs.children.toSeq.sorted
            val bulk = children.dropRight(1).map(printNode(_, indent + "   |"))
            bulk ++ (children.lastOption.map(printNode(_, indent + "    ")))
          }.mkString("\n"))
        case _ =>
          indent + node.path.name + " " + Logging.simpleName(node)
      }
    }
    printNode(lookupRoot, "")
  }

  final class TerminationCallbacks[T](upStreamTerminated: Future[T])(implicit ec: ExecutionContext) {
    private[this] final val done = Promise[T]()
    private[this] final val ref = new AtomicReference(done)

    // onComplete never fires twice so safe to avoid null check
    upStreamTerminated.onComplete { t =>
      ref.getAndSet(null).complete(t)
    }

    /**
     * Adds a Runnable that will be executed on ActorSystem termination.
     * Note that callbacks are executed in reverse order of insertion.
     *
     * @param r The callback to be executed on ActorSystem termination
     * Throws RejectedExecutionException if called after ActorSystem has been terminated.
     */
    final def add(r: Runnable): Unit = {
      @tailrec def addRec(r: Runnable, p: Promise[T]): Unit = ref.get match {
        case null                               => throw new RejectedExecutionException("ActorSystem already terminated.")
        case some if ref.compareAndSet(some, p) => some.completeWith(p.future.andThen { case _ => r.run() })
        case _                                  => addRec(r, p)
      }
      addRec(r, Promise[T]())
    }

    /**
     * Returns a Future which will be completed once all registered callbacks have been executed.
     */
    def terminationFuture: Future[T] = done.future
  }

  private def isTypedGuardian: Boolean =
    guardianProps.exists(_.clazz.getName.startsWith("akka.actor.typed"))

  private def checkLicenseKey(): Unit = {
    val maybeSupplier = Option(LicenseKeySupplier.instance.get())
    val key = maybeSupplier match {
      case Some(supplier) => supplier.get(config)
      case None           => config.getString("akka.license-key")
    }
    val akkaVersion = akka.Version.Current
    val buildDate = LocalDate.ofInstant(Instant.ofEpochMilli(akka.Version.BuildDate), ZoneId.systemDefault())
    val today = LocalDate.now()
    val revokedLicenseKeyIds = Seq(
      // This actually isn't a revoked license key id, but is here as an example of what one looks like
      "ece608e4a2cc927c3d31d7e1ac0b3b641c1cf569548c5462e659cce412cfcd6c")

    if (today.isAfter(buildDate.plusYears(3))) {
      log.info(s"Akka $akkaVersion is more than 3 years old, license check skipped as Apache license is in use.")
    } else if (key == "") {
      setLicenseKeyExpiry(today)
      if (config.getBoolean("akka.warn-on-no-license-key")) {
        log.warning("Dev use only. Free keys at https://akka.io/key")
      }
      scheduler.scheduleOnce(15.minutes) {
        log.error("Akka terminated. Obtain free keys at https://akka.io/key")
        terminate()
      }(dispatcher)
    } else {
      val publicKeyEncoded =
        "49M9dBWX5VO6FXdkO6KvIgBWn97ANHFad5fJdkVkhy0DG0jzE5l4SZY6I5am2DmnaHJ4cIx54umRoCy1q3SzjLHbBMcczLIaO2p2CQKUNgwK8y4KITrUTYn6U0EqUb"
      val publicKeySpec = new X509EncodedKeySpec(Base62.decode(publicKeyEncoded))
      val publicKey = KeyFactory.getInstance("EC").generatePublic(publicKeySpec)

      val licenseBytes = try Base62.decode(key)
      catch {
        case NonFatal(e) => failInvalidLicenseKey(s"Error decoding license key: ${e.getMessage}")
      }
      if (licenseBytes.length < 3) {
        failInvalidLicenseKey("Invalid license key")
      }
      if (licenseBytes(0) != 0x25 || licenseBytes(1) != 0x52) {
        failInvalidLicenseKey("Invalid license key")
      }
      val version = licenseBytes(2)
      if (version != 0) {
        failInvalidLicenseKey(s"Unrecognized license key version: $version")
      }
      val hashSep = licenseBytes.indexOf('#', 2)
      if (hashSep == -1) {
        failInvalidLicenseKey("Invalid license key")
      }
      val signedContent = licenseBytes.take(hashSep)
      val sigBytes = licenseBytes.drop(hashSep + 1)
      val sig = Signature.getInstance("SHA256withECDSA")
      sig.initVerify(publicKey)
      sig.update(signedContent)
      if (!sig.verify(sigBytes)) {
        failInvalidLicenseKey("Failed to validate license signature")
      }

      val licenseKeyId = MessageDigest
        .getInstance("SHA-256")
        .digest(sigBytes)
        // Not efficient, would use HexFormat.of(), but that requires JDK17
        .map(byte => "%02X".format(byte))
        .mkString
      if (revokedLicenseKeyIds.contains(licenseKeyId)) {
        failInvalidLicenseKey("The license key has been revoked")
      }

      val claimsStr = new String(licenseBytes, 3, hashSep - 3, "utf-8")
      val claims = claimsStr.split('&').map { claim =>
        claim.split("=", 2) match {
          case Array(key, value) => key -> URLDecoder.decode(value, "utf-8")
          case _                 => failInvalidLicenseKey("Invalid license key claim")
        }
      }

      val epoch = LocalDate.of(2024, 8, 22)

      def opt(key: String): Option[String] = claims.collectFirst {
        case (`key`, value) => value
      }

      def optDate(key: String): Option[LocalDate] = opt(key).map(v => epoch.plusDays(Integer.parseInt(v, 16)))

      val user = opt("s").getOrElse("unknown user")
      val issuer = opt("i") match {
        case Some("k") =>
          "Kalix"
        case _ =>
          "Akka"
      }

      val buildExpiry = optDate("b")
      val expiry = optDate("e")
      val warnOnExpiry = opt("w").contains("1")
      val supplierClaim = opt("supplier")
      val buildVersion = opt("v")

      supplierClaim.foreach { expectedSupplierClass =>
        maybeSupplier match {
          case Some(supplier) if expectedSupplierClass != supplier.getClass.getName =>
            failInvalidLicenseKey("Not compatible with this distribution of Akka. Wrong supplier.")
          case Some(_) =>
          // ok, right supplier
          case None =>
            failInvalidLicenseKey("Not compatible with this distribution of Akka. Missing supplier.")
        }
      }

      buildVersion.foreach { version =>
        if (version != akkaVersion) {
          failInvalidLicenseKey("Not permitted for use with this version of Akka")
        }
      }

      buildExpiry match {
        case Some(expired) if expired.isBefore(buildDate) =>
          setLicenseKeyExpiry(expired)
          failExpiredLicenseKey(
            warnOnExpiry,
            s"The key licensed to $issuer user $user is expired for all Akka builds released after $expired, but Akka $akkaVersion was released on $buildDate.")
        case Some(valid) =>
          // no setLicenseKeyExpiry because for currently used Akka version the key will not expire
          log.info(
            s"License check succeeded for $issuer user $user. License is valid for all Akka builds released prior to $valid.")
        case None =>
      }
      expiry match {
        case Some(expired) if expired.isBefore(today) =>
          setLicenseKeyExpiry(expired)
          failExpiredLicenseKey(warnOnExpiry, s"The key licensed to $issuer user $user expired on $expired.")
        case Some(valid) =>
          setLicenseKeyExpiry(valid)
          log.info(s"License check succeeded for $issuer user $user. License is valid until $valid.")
        case None =>
      }

      if (buildExpiry.isEmpty && expiry.isEmpty) {
        val versionString = if (buildVersion.isDefined) s" only for an Akka version of ${buildVersion.get}" else ""
        log.info(s"License check succeeded for $issuer user $user. License is perpetual$versionString.")
      }
    }
  }

  private def failInvalidLicenseKey(detailedMsg: String): Nothing = {
    log.error("Invalid key. Free keys at https://akka.io/key")
    log.debug(s"The supplied license key is not valid: $detailedMsg")
    throw new RuntimeException(s"Invalid key: $detailedMsg")
  }

  private def failExpiredLicenseKey(warnOnExpiry: Boolean, detailedMsg: String): Unit = {
    log.error(
      "Expired key. {}Obtain refresh key at https://akka.io/key",
      if (warnOnExpiry) "" else "Akka will terminate in 15 mins. ")
    log.debug(s"The supplied license key has expired: $detailedMsg")
    if (!warnOnExpiry) {
      val terminateAfter = 15.minutes + ThreadLocalRandom.current().nextInt(5 * 60).seconds
      scheduler.scheduleOnce(terminateAfter) {
        log.error("Akka terminated due to expired key. Obtain refresh key at https://akka.io/key")
        terminate()
      }(dispatcher)
    }
  }
}
