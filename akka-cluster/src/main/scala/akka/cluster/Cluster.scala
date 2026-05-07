/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import java.io.Closeable
import java.util.concurrent.CompletionStage
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicBoolean

import scala.annotation.nowarn
import scala.annotation.varargs
import scala.collection.immutable
import scala.concurrent.{ Await, ExecutionContext }
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

import com.typesafe.config.{ Config, ConfigFactory }

import akka.ConfigurationException
import akka.actor._
import akka.annotation.InternalApi
import akka.cluster.ClusterSettings.DataCenter
import akka.dispatch.MonitorableThreadFactory
import akka.event.{ Logging, LoggingAdapter }
import akka.event.LogMarker
import akka.event.Logging.LogLevel
import akka.event.MarkerLoggingAdapter
import akka.japi.Util
import akka.pattern._
import akka.remote.{ UniqueAddress => _, _ }
import akka.util.Version

/**
 * Cluster Extension Id and factory for creating Cluster extension.
 */
object Cluster extends ExtensionId[Cluster] with ExtensionIdProvider {
  override def get(system: ActorSystem): Cluster = super.get(system)

  override def get(system: ClassicActorSystemProvider): Cluster = super.get(system)

  override def lookup = Cluster

  override def createExtension(system: ExtendedActorSystem): Cluster = new Cluster(system)

  /**
   * INTERNAL API
   */
  private[cluster] final val isAssertInvariantsEnabled: Boolean =
    System.getProperty("akka.cluster.assert", "off").toLowerCase match {
      case "on" | "true" => true
      case _             => false
    }
}

/**
 * This module is responsible cluster membership information. Changes to the cluster
 * information is retrieved through [[#subscribe]]. Commands to operate the cluster is
 * available through methods in this class, such as [[#join]], [[#down]] and [[#leave]].
 *
 * Each cluster [[Member]] is identified by its [[akka.actor.Address]], and
 * the cluster address of this actor system is [[#selfAddress]]. A member also has a status;
 * initially [[MemberStatus]] `Joining` followed by [[MemberStatus]] `Up`.
 */
class Cluster(val system: ExtendedActorSystem) extends Extension {

  import ClusterEvent._

  val settings = new ClusterSettings(system.settings.config, system.name)
  import ClusterLogger._
  import settings._

  private val joinConfigCompatChecker: JoinConfigCompatChecker = JoinConfigCompatChecker.load(system, settings)

  /**
   * The address including a `uid` of this cluster member.
   * The `uid` is needed to be able to distinguish different
   * incarnations of a member with same hostname and port.
   */
  val selfUniqueAddress: UniqueAddress = system.provider match {
    case c: ClusterActorRefProvider =>
      UniqueAddress(c.transport.defaultAddress, system.uid)
    case other =>
      throw new ConfigurationException(
        s"ActorSystem [${system}] needs to have 'akka.actor.provider' set to 'cluster' in the configuration, currently uses [${other.getClass.getName}]")
  }

  /**
   * The address of this cluster member.
   */
  def selfAddress: Address = selfUniqueAddress.address

  /** Data center to which this node belongs to (defaults to "default" if not configured explicitly) */
  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def selfDataCenter: DataCenter = settings.SelfDataCenter

  /**
   * roles that this member has
   */
  def selfRoles: Set[String] = settings.Roles

  /**
   * Java API: roles that this member has
   */
  def getSelfRoles: java.util.Set[String] = selfRoles.asJava

  private val _isTerminated = new AtomicBoolean(false)
  private val log = Logging.withMarker(system, ClusterLogClass.ClusterCore)
  // ClusterJmx is initialized as the last thing in the constructor
  private var clusterJmx: Option[ClusterJmx] = None

  logInfo("Starting up, Akka version [{}] ...", system.settings.ConfigVersion)

  val failureDetector: FailureDetectorRegistry[Address] = {
    val createFailureDetector = () =>
      FailureDetectorLoader.load(settings.FailureDetectorImplementationClass, settings.FailureDetectorConfig, system)

    new DefaultFailureDetectorRegistry(createFailureDetector)
  }

  val crossDcFailureDetector: FailureDetectorRegistry[Address] = {
    val createFailureDetector = () =>
      FailureDetectorLoader.load(
        settings.MultiDataCenter.CrossDcFailureDetectorSettings.ImplementationClass,
        settings.MultiDataCenter.CrossDcFailureDetectorSettings.config,
        system)

    new DefaultFailureDetectorRegistry(createFailureDetector)
  }

  // needs to be lazy to allow downing provider impls to access Cluster (if not we get deadlock)
  lazy val downingProvider: DowningProvider = {
    checkAutoDownUsage()
    DowningProvider.load(settings.DowningProviderClassName, system)
  }

  private def checkAutoDownUsage(): Unit = {
    if (settings.DowningProviderClassName == "akka.cluster.AutoDowning" ||
        (settings.config.hasPath("auto-down-unreachable-after") && settings.config.getString(
          "auto-down-unreachable-after") != "off"))
      logWarning(
        "auto-down has been removed in Akka 2.6.0. See " +
        "https://doc.akka.io/libraries/akka-core/current/typed/cluster.html#downing for alternatives.")
  }

  // ========================================================
  // ===================== WORK DAEMONS =====================
  // ========================================================

  /**
   * INTERNAL API
   */
  private[cluster] val scheduler: Scheduler = {
    if (system.scheduler.maxFrequency < 1.second / SchedulerTickDuration) {
      logInfo(
        "Using a dedicated scheduler for cluster. Default scheduler can be used if configured " +
        "with 'akka.scheduler.tick-duration' [{} ms] <=  'akka.cluster.scheduler.tick-duration' [{} ms].",
        (1000 / system.scheduler.maxFrequency).toInt,
        SchedulerTickDuration.toMillis)

      val cfg = ConfigFactory
        .parseString(s"akka.scheduler.tick-duration=${SchedulerTickDuration.toMillis}ms")
        .withFallback(system.settings.config)
      val threadFactory = system.threadFactory match {
        case tf: MonitorableThreadFactory => tf.withName(tf.name + "-cluster-scheduler")
        case tf                           => tf
      }
      system.dynamicAccess
        .createInstanceFor[Scheduler](
          system.settings.SchedulerClass,
          immutable
            .Seq(classOf[Config] -> cfg, classOf[LoggingAdapter] -> log, classOf[ThreadFactory] -> threadFactory))
        .get
    } else {
      // delegate to system.scheduler, but don't close over system
      val systemScheduler = system.scheduler
      new Scheduler with Closeable {
        override def close(): Unit = () // we are using system.scheduler, which we are not responsible for closing

        override def maxFrequency: Double = systemScheduler.maxFrequency

        @nowarn("msg=deprecated")
        override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(
            implicit executor: ExecutionContext): Cancellable =
          systemScheduler.schedule(initialDelay, interval, runnable)

        override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(
            implicit executor: ExecutionContext): Cancellable =
          systemScheduler.scheduleOnce(delay, runnable)
      }
    }
  }

  // create supervisor for daemons under path "/system/cluster"
  private val clusterDaemons: ActorRef = {
    system.systemActorOf(
      Props(new ClusterDaemon(joinConfigCompatChecker)).withDispatcher(UseDispatcher).withDeploy(Deploy.local),
      name = "cluster")
  }

  /**
   * INTERNAL API
   */
  private[cluster] val clusterCore: ActorRef = {
    implicit val timeout = system.settings.CreationTimeout
    try {
      Await.result((clusterDaemons ? InternalClusterAction.GetClusterCoreRef).mapTo[ActorRef], timeout.duration)
    } catch {
      case NonFatal(e) =>
        log.error(e, "Failed to startup Cluster. You can try to increase 'akka.actor.creation-timeout'.")
        shutdown()
        // don't re-throw, that would cause the extension to be re-recreated
        // from shutdown() or other places, which may result in
        // InvalidActorNameException: actor name [cluster] is not unique
        system.deadLetters
    }
  }

  private[cluster] val readView: ClusterReadView = new ClusterReadView(this)

  system.registerOnTermination(shutdown())

  if (JmxEnabled)
    clusterJmx = {
      val jmx = new ClusterJmx(this, log)
      jmx.createMBean()
      Some(jmx)
    }

  logInfo("Started up successfully")

  // ======================================================
  // ===================== PUBLIC API =====================
  // ======================================================

  /**
   * Returns true if this cluster instance has be shutdown.
   */
  def isTerminated: Boolean = _isTerminated.get

  /**
   * Current snapshot state of the cluster.
   */
  def state: CurrentClusterState = readView.state

  /**
   * Current snapshot of the member itself
   */
  def selfMember: Member = readView.self

  /**
   * Subscribe to one or more cluster domain events.
   * The `to` classes can be [[akka.cluster.ClusterEvent.ClusterDomainEvent]]
   * or subclasses.
   *
   * A snapshot of [[akka.cluster.ClusterEvent.CurrentClusterState]]
   * will be sent to the subscriber as the first message.
   */
  @varargs def subscribe(subscriber: ActorRef, to: Class[_]*): Unit =
    subscribe(subscriber, initialStateMode = InitialStateAsSnapshot, to: _*)

  /**
   * Subscribe to one or more cluster domain events.
   * The `to` classes can be [[akka.cluster.ClusterEvent.ClusterDomainEvent]]
   * or subclasses.
   *
   * If `initialStateMode` is `ClusterEvent.InitialStateAsEvents` the events corresponding
   * to the current state will be sent to the subscriber to mimic what you would
   * have seen if you were listening to the events when they occurred in the past.
   *
   * If `initialStateMode` is `ClusterEvent.InitialStateAsSnapshot` a snapshot of
   * [[akka.cluster.ClusterEvent.CurrentClusterState]] will be sent to the subscriber as the
   * first message.
   *
   * Note that for large clusters it is more efficient to use `InitialStateAsSnapshot`.
   */
  @varargs def subscribe(subscriber: ActorRef, initialStateMode: SubscriptionInitialStateMode, to: Class[_]*): Unit = {
    require(to.length > 0, "at least one `ClusterDomainEvent` class is required")
    require(
      to.forall(classOf[ClusterDomainEvent].isAssignableFrom),
      s"subscribe to `akka.cluster.ClusterEvent.ClusterDomainEvent` or subclasses, was [${to.map(_.getName).mkString(", ")}]")
    clusterCore ! InternalClusterAction.Subscribe(subscriber, initialStateMode, to.toSet)
  }

  /**
   * Unsubscribe to all cluster domain events.
   */
  def unsubscribe(subscriber: ActorRef): Unit =
    clusterCore ! InternalClusterAction.Unsubscribe(subscriber, None)

  /**
   * Unsubscribe to a specific type of cluster domain events,
   * matching previous `subscribe` registration.
   */
  def unsubscribe(subscriber: ActorRef, to: Class[_]): Unit =
    clusterCore ! InternalClusterAction.Unsubscribe(subscriber, Some(to))

  /**
   * Send current (full) state of the cluster to the specified
   * receiver. If you want this to happen periodically you need to schedule
   * a call to this method yourself. Note that you can also retrieve the current
   * state with [[#state]].
   */
  def sendCurrentClusterState(receiver: ActorRef): Unit =
    clusterCore ! InternalClusterAction.SendCurrentClusterState(receiver)

  /**
   * Try to join this cluster node with the node specified by 'address'.
   * A 'Join(selfAddress)' command is sent to the node to join.
   *
   * An actor system can only join a cluster once. Additional attempts will be ignored.
   * When it has successfully joined it must be restarted to be able to join another
   * cluster or to join the same cluster again.
   *
   * The name of the [[akka.actor.ActorSystem]] must be the same for all members of a
   * cluster.
   */
  def join(address: Address): Unit = {
    address.checkHostCharacters()
    clusterCore ! ClusterUserAction.JoinTo(fillLocal(address))
  }

  /**
   * Change the state of every member in preparation for a full cluster shutdown.
   */
  def prepareForFullClusterShutdown(): Unit = {
    clusterCore ! ClusterUserAction.PrepareForShutdown
  }

  private def fillLocal(address: Address): Address = {
    // local address might be used if grabbed from actorRef.path.address
    if (address.hasLocalScope && address.system == selfAddress.system) selfAddress
    else address
  }

  /**
   * Join the specified seed nodes without defining them in config.
   * Especially useful from tests when Addresses are unknown before startup time.
   *
   * An actor system can only join a cluster once. Additional attempts will be ignored.
   * When it has successfully joined it must be restarted to be able to join another
   * cluster or to join the same cluster again.
   */
  def joinSeedNodes(seedNodes: immutable.Seq[Address]): Unit = {
    seedNodes.foreach(_.checkHostCharacters())
    clusterCore ! InternalClusterAction.JoinSeedNodes(seedNodes.toVector.map(fillLocal))
  }

  /**
   * Java API
   *
   * Join the specified seed nodes without defining them in config.
   * Especially useful from tests when Addresses are unknown before startup time.
   *
   * An actor system can only join a cluster once. Additional attempts will be ignored.
   * When it has successfully joined it must be restarted to be able to join another
   * cluster or to join the same cluster again.
   */
  def joinSeedNodes(seedNodes: java.util.List[Address]): Unit =
    joinSeedNodes(Util.immutableSeq(seedNodes))

  /**
   * Scala API: If the `appVersion` is read from an external system (e.g. Kubernetes) it can be defined after
   * system startup but before joining by completing the `appVersion` `Future`. In that case, `setAppVersionLater`
   * should be called before calling `join` or `joinSeedNodes`. It's fine to call `join` or `joinSeedNodes`
   * immediately afterwards (before the `Future` is completed. The join will then wait for the `appVersion`
   * to be completed.
   */
  def setAppVersionLater(appVersion: Future[Version]): Unit = {
    clusterCore ! ClusterUserAction.SetAppVersionLater
    import system.dispatcher
    appVersion.onComplete {
      case Success(version) =>
        clusterCore ! ClusterUserAction.SetAppVersion(version)
      case Failure(exc) =>
        logWarning("Later appVersion failed. Fallback to configured appVersion [{}]. {}", settings.AppVersion, exc)
        clusterCore ! ClusterUserAction.SetAppVersion(settings.AppVersion)
    }
  }

  /**
   * Java API: If the `appVersion` is read from an external system (e.g. Kubernetes) it can be defined after
   * system startup but before joining by completing the `appVersion` `CompletionStage`. In that case,
   * `setAppVersionLater` should be called before calling `join` or `joinSeedNodes`. It's fine to call
   * `join` or `joinSeedNodes` immediately afterwards (before the `CompletionStage` is completed. The join will
   * then wait for the `appVersion` to be completed.
   */
  def setAppVersionLater(appVersion: CompletionStage[Version]): Unit = {
    import scala.jdk.FutureConverters._
    setAppVersionLater(appVersion.asScala)
  }

  /**
   * Send command to issue state transition to LEAVING for the node specified by 'address'.
   * The member will go through the status changes [[MemberStatus]] `Leaving` (not published to
   * subscribers) followed by [[MemberStatus]] `Exiting` and finally [[MemberStatus]] `Removed`.
   *
   * Note that this command can be issued to any member in the cluster, not necessarily the
   * one that is leaving. The cluster extension, but not the actor system or JVM, of the
   * leaving member will be shutdown after the leader has changed status of the member to
   * Exiting. Thereafter the member will be removed from the cluster. Normally this is
   * handled automatically, but in case of network failures during this process it might
   * still be necessary to set the node’s status to Down in order to complete the removal.
   */
  def leave(address: Address): Unit =
    clusterCore ! ClusterUserAction.Leave(fillLocal(address))

  /**
   * Send command to DOWN the node specified by 'address'.
   *
   * When a member is considered by the failure detector to be unreachable the leader is not
   * allowed to perform its duties, such as changing status of new joining members to 'Up'.
   * The status of the unreachable member must be changed to 'Down', which can be done with
   * this method.
   */
  def down(address: Address): Unit =
    clusterCore ! ClusterUserAction.Down(fillLocal(address))

  /**
   * The supplied thunk will be run, once, when current cluster member is `Up`.
   * Typically used together with configuration option `akka.cluster.min-nr-of-members`
   * to defer some action, such as starting actors, until the cluster has reached
   * a certain size.
   */
  def registerOnMemberUp[T](code: => T): Unit =
    registerOnMemberUp(new Runnable { def run() = code })

  /**
   * Java API: The supplied callback will be run, once, when current cluster member is `Up`.
   * Typically used together with configuration option `akka.cluster.min-nr-of-members`
   * to defer some action, such as starting actors, until the cluster has reached
   * a certain size.
   */
  def registerOnMemberUp(callback: Runnable): Unit =
    clusterDaemons ! InternalClusterAction.AddOnMemberUpListener(callback)

  /**
   * The supplied thunk will be run, once, when current cluster member is `Removed`.
   * If the cluster has already been shutdown the thunk will run on the caller thread immediately.
   * If this is called "at the same time" as `shutdown()` there is a possibility that the the thunk
   * is not invoked. It's often better to use [[akka.actor.CoordinatedShutdown]] for this purpose.
   */
  def registerOnMemberRemoved[T](code: => T): Unit =
    registerOnMemberRemoved(new Runnable { override def run(): Unit = code })

  /**
   * Java API: The supplied thunk will be run, once, when current cluster member is `Removed`.
   * If the cluster has already been shutdown the thunk will run on the caller thread immediately.
   * If this is called "at the same time" as `shutdown()` there is a possibility that the the thunk
   * is not invoked. It's often better to use [[akka.actor.CoordinatedShutdown]] for this purpose.
   */
  def registerOnMemberRemoved(callback: Runnable): Unit = {
    if (_isTerminated.get())
      callback.run()
    else
      clusterDaemons ! InternalClusterAction.AddOnMemberRemovedListener(callback)
  }

  /**
   * Generate the remote actor path by replacing the Address in the RootActor Path for the given
   * ActorRef with the cluster's `selfAddress`, unless address' host is already defined
   */
  def remotePathOf(actorRef: ActorRef): ActorPath = {
    val path = actorRef.path
    if (path.address.host.isDefined) {
      path
    } else {
      (path.root.copy(selfAddress) / path.elements).withUid(path.uid)
    }
  }

  // ========================================================
  // ===================== INTERNAL API =====================
  // ========================================================

  /**
   * INTERNAL API.
   *
   * Shuts down all connections to other members, the cluster daemon and the periodic gossip and cleanup tasks.
   *
   * Should not called by the user. The user can issue a LEAVE command which will tell the node
   * to go through graceful handoff process `LEAVE -&gt; EXITING -&gt; REMOVED -&gt; SHUTDOWN`.
   */
  @InternalApi private[cluster] def shutdown(): Unit = {
    if (_isTerminated.compareAndSet(false, true)) {
      logInfo("Shutting down...")

      system.stop(clusterDaemons)

      // readView might be null if init fails before it is created
      if (readView != null)
        readView.close()

      closeScheduler()

      clusterJmx.foreach { _.unregisterMBean() }

      logInfo("Successfully shut down")
    }
  }

  private def closeScheduler(): Unit = scheduler match {
    case x: Closeable => x.close()
    case _            => // ignore, this is fine
  }

  /**
   * INTERNAL API
   */
  private[cluster] object ClusterLogger extends ClusterLogger(log)

  /**
   * INTERNAL API
   */
  private[cluster] class ClusterLogger(log: MarkerLoggingAdapter) {
    def isDebugEnabled: Boolean =
      log.isDebugEnabled

    def logDebug(message: String): Unit =
      if (settings.LogInfo && log.isDebugEnabled)
        logAtLevel(Logging.DebugLevel, message)

    def logDebug(template: String, arg1: Any): Unit =
      if (settings.LogInfo && log.isDebugEnabled)
        logAtLevel(Logging.DebugLevel, log.format(template, arg1))

    def logDebug(template: String, arg1: Any, arg2: Any): Unit =
      if (settings.LogInfo && log.isDebugEnabled)
        logAtLevel(Logging.DebugLevel, log.format(template, arg1, arg2))

    def logDebug(template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      if (settings.LogInfo && log.isDebugEnabled)
        logAtLevel(Logging.DebugLevel, log.format(template, arg1, arg2, arg3))

    def logInfo(message: String): Unit =
      if (settings.LogInfo && log.isInfoEnabled)
        logAtLevel(Logging.InfoLevel, message)

    def logInfo(marker: LogMarker, message: String): Unit =
      if (settings.LogInfo && log.isInfoEnabled(marker))
        logAtLevel(marker, Logging.InfoLevel, message)

    def logInfo(template: String, arg1: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled)
        logAtLevel(Logging.InfoLevel, log.format(template, arg1))

    def logInfo(marker: LogMarker, template: String, arg1: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled(marker))
        logAtLevel(marker, Logging.InfoLevel, log.format(template, arg1))

    def logInfo(template: String, arg1: Any, arg2: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled)
        logAtLevel(Logging.InfoLevel, log.format(template, arg1, arg2))

    def logInfo(marker: LogMarker, template: String, arg1: Any, arg2: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled(marker))
        logAtLevel(marker, Logging.InfoLevel, log.format(template, arg1, arg2))

    def logInfo(template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled)
        logAtLevel(Logging.InfoLevel, log.format(template, arg1, arg2, arg3))

    def logInfo(marker: LogMarker, template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      if (settings.LogInfo && log.isInfoEnabled(marker))
        logAtLevel(marker, Logging.InfoLevel, log.format(template, arg1, arg2, arg3))

    def logWarning(message: String): Unit =
      if (log.isWarningEnabled)
        logAtLevel(Logging.WarningLevel, message)

    def logWarning(template: String, arg1: Any): Unit =
      if (log.isWarningEnabled)
        logAtLevel(Logging.WarningLevel, log.format(template, arg1))

    def logWarning(marker: LogMarker, template: String, arg1: Any): Unit =
      if (log.isWarningEnabled(marker))
        logAtLevel(marker, Logging.WarningLevel, log.format(template, arg1))

    def logWarning(template: String, arg1: Any, arg2: Any): Unit =
      if (log.isWarningEnabled)
        logAtLevel(Logging.WarningLevel, log.format(template, arg1, arg2))

    def logWarning(marker: LogMarker, template: String, arg1: Any, arg2: Any): Unit =
      if (log.isWarningEnabled(marker))
        logAtLevel(marker, Logging.WarningLevel, log.format(template, arg1, arg2))

    def logWarning(template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      if (log.isWarningEnabled)
        logAtLevel(Logging.WarningLevel, log.format(template, arg1, arg2, arg3))

    def logError(message: String): Unit =
      if (log.isErrorEnabled)
        logAtLevel(Logging.ErrorLevel, message)

    def logError(marker: LogMarker, message: String): Unit =
      if (log.isErrorEnabled(marker))
        logAtLevel(marker, Logging.ErrorLevel, message)

    def logError(template: String, arg1: Any): Unit =
      if (log.isErrorEnabled)
        logAtLevel(Logging.ErrorLevel, log.format(template, arg1))

    def logError(template: String, arg1: Any, arg2: Any): Unit =
      if (log.isErrorEnabled)
        logAtLevel(Logging.ErrorLevel, log.format(template, arg1, arg2))

    def logError(template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      if (log.isErrorEnabled)
        logAtLevel(Logging.ErrorLevel, log.format(template, arg1, arg2, arg3))

    def logError(cause: Throwable, message: String): Unit = {
      if (settings.SelfDataCenter == ClusterSettings.DefaultDataCenter)
        log.error(cause, "Cluster Node [{}] - {}", selfAddress, message)
      else
        log.error(cause, "Cluster Node [{}] dc [{}] - {}", selfAddress, settings.SelfDataCenter, message)
    }

    def logError(cause: Throwable, template: String, arg1: Any): Unit =
      logError(cause, log.format(template, arg1))

    def logError(cause: Throwable, template: String, arg1: Any, arg2: Any): Unit =
      logError(cause, log.format(template, arg1, arg2))

    def logError(cause: Throwable, template: String, arg1: Any, arg2: Any, arg3: Any): Unit =
      logError(cause, log.format(template, arg1, arg2, arg3))

    private def logAtLevel(logLevel: LogLevel, message: String): Unit = {
      if (settings.SelfDataCenter == ClusterSettings.DefaultDataCenter)
        log.log(logLevel, "Cluster Node [{}] - {}", selfAddress, message)
      else
        log.log(logLevel, "Cluster Node [{}] dc [{}] - {}", selfAddress, settings.SelfDataCenter, message)
    }

    private def logAtLevel(marker: LogMarker, logLevel: LogLevel, message: String): Unit = {
      if (settings.SelfDataCenter == ClusterSettings.DefaultDataCenter)
        log.log(marker, logLevel, log.format("Cluster Node [{}] - {}", selfAddress, message))
      else
        log.log(
          marker,
          logLevel,
          log.format("Cluster Node [{}] dc [{}] - {}", selfAddress, settings.SelfDataCenter, message))
    }

  }

}
