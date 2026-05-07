/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.util.Optional
import java.util.concurrent._
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier

import scala.annotation.tailrec
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._
import scala.concurrent.{ Await, ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.control.NonFatal

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.Done
import akka.annotation.InternalApi
import akka.event.Logging
import akka.pattern.after
import akka.util.OptionVal
import akka.util.TopologicalSort

object CoordinatedShutdown extends ExtensionId[CoordinatedShutdown] with ExtensionIdProvider {

  /**
   * The first pre-defined phase that applications can add tasks to.
   * Note that more phases can be added in the application's
   * configuration by overriding this phase with an additional
   * depends-on.
   */
  val PhaseBeforeServiceUnbind = "before-service-unbind"

  /**
   * Stop accepting new incoming requests in for example HTTP.
   */
  val PhaseServiceUnbind = "service-unbind"

  /**
   * Wait for requests that are in progress to be completed.
   */
  val PhaseServiceRequestsDone = "service-requests-done"

  /**
   * Final shutdown of service endpoints.
   */
  val PhaseServiceStop = "service-stop"

  /**
   * Phase for custom application tasks that are to be run
   * after service shutdown and before cluster shutdown.
   */
  val PhaseBeforeClusterShutdown = "before-cluster-shutdown"

  /**
   * Graceful shutdown of the Cluster Sharding regions.
   */
  val PhaseClusterShardingShutdownRegion = "cluster-sharding-shutdown-region"

  /**
   * Emit the leave command for the node that is shutting down.
   */
  val PhaseClusterLeave = "cluster-leave"

  /**
   * Shutdown cluster singletons
   */
  val PhaseClusterExiting = "cluster-exiting"

  /**
   * Wait until exiting has been completed
   */
  val PhaseClusterExitingDone = "cluster-exiting-done"

  /**
   * Shutdown the cluster extension
   */
  val PhaseClusterShutdown = "cluster-shutdown"

  /**
   * Phase for custom application tasks that are to be run
   * after cluster shutdown and before ActorSystem termination.
   */
  val PhaseBeforeActorSystemTerminate = "before-actor-system-terminate"

  /**
   * Last phase. See terminate-actor-system and exit-jvm above.
   * Don't add phases that depends on this phase because the
   * dispatcher and scheduler of the ActorSystem have been shutdown.
   */
  val PhaseActorSystemTerminate = "actor-system-terminate"

  /**
   * Reason for the shutdown, which can be used by tasks in case they need to do
   * different things depending on what caused the shutdown. There are some
   * predefined reasons, but external libraries applications may also define
   * other reasons.
   */
  trait Reason

  /**
   * Scala API: The reason for the shutdown was unknown. Needed for backwards compatibility.
   */
  case object UnknownReason extends Reason

  /**
   * Java API: The reason for the shutdown was unknown. Needed for backwards compatibility.
   */
  def unknownReason: Reason = UnknownReason

  /**
   * Scala API: The shutdown was initiated by ActorSystem.terminate.
   */
  case object ActorSystemTerminateReason extends Reason

  /**
   * Java API: The shutdown was initiated by ActorSystem.terminate.
   */
  def actorSystemTerminateReason: Reason = ActorSystemTerminateReason

  /**
   * Scala API: The shutdown was initiated by a JVM shutdown hook, e.g. triggered by SIGTERM.
   */
  case object JvmExitReason extends Reason

  /**
   * Java API: The shutdown was initiated by a JVM shutdown hook, e.g. triggered by SIGTERM.
   */
  def jvmExitReason: Reason = JvmExitReason

  /**
   * Scala API: The shutdown was initiated by Cluster downing.
   */
  case object ClusterDowningReason extends Reason

  /**
   * Java API: The shutdown was initiated by Cluster downing.
   */
  def clusterDowningReason: Reason = ClusterDowningReason

  /**
   * Scala API: The shutdown was initiated by a failure to join a seed node.
   */
  case object ClusterJoinUnsuccessfulReason extends Reason

  /**
   * Java API: The shutdown was initiated by a failure to join a seed node.
   */
  def clusterJoinUnsuccessfulReason: Reason = ClusterJoinUnsuccessfulReason

  /**
   * Scala API: The shutdown was initiated by a configuration clash within the existing cluster and the joining node
   */
  case object IncompatibleConfigurationDetectedReason extends Reason

  /**
   * Java API: The shutdown was initiated by a configuration clash within the existing cluster and the joining node
   */
  def incompatibleConfigurationDetectedReason: Reason = IncompatibleConfigurationDetectedReason

  /**
   * Scala API: The shutdown was initiated by Cluster leaving.
   */
  case object ClusterLeavingReason extends Reason

  /**
   * Java API: The shutdown was initiated by Cluster leaving.
   */
  def clusterLeavingReason: Reason = ClusterLeavingReason

  @volatile private var runningJvmHook = false

  override def get(system: ActorSystem): CoordinatedShutdown = super.get(system)

  override def get(system: ClassicActorSystemProvider): CoordinatedShutdown = super.get(system)

  override def lookup = CoordinatedShutdown

  override def createExtension(system: ExtendedActorSystem): CoordinatedShutdown = {
    val conf = system.settings.config.getConfig("akka.coordinated-shutdown")
    val phases = phasesFromConfig(conf)
    val coord = new CoordinatedShutdown(system, phases)
    init(system, conf, coord)
    coord
  }

  // locate reason-specific overrides and merge with defaults.
  @InternalApi private[akka] def confWithOverrides(conf: Config, reason: Option[Reason]): Config = {
    reason
      .flatMap { r =>
        val basePath = s"""reason-overrides."${r.getClass.getName}""""
        if (conf.hasPath(basePath)) Some(conf.getConfig(basePath).withFallback(conf)) else None
      }
      .getOrElse(conf)
  }

  /** INTERNAL API */
  @InternalApi
  private[akka] def init(system: ExtendedActorSystem, conf: Config, coord: CoordinatedShutdown): Unit = {
    // separated for testability
    initPhaseActorSystemTerminate(system, conf, coord)
    initJvmHook(system, conf, coord)
    // Avoid leaking actor system references when system is terminated before JVM is #23384
    // Catching RejectedExecutionException in case extension is accessed first time when
    // system is already terminated, see #25592. The extension is eagerly loaded when ActorSystem
    // is started but it might be a race between (failing?) startup and shutdown.
    def cleanupActorSystemJvmHook(): Unit = {
      coord.actorSystemJvmHook match {
        case OptionVal.Some(cancellable) if !runningJvmHook && !cancellable.isCancelled =>
          cancellable.cancel()
          coord.actorSystemJvmHook = OptionVal.None
        case _ =>
      }
    }
    try system.registerOnTermination(cleanupActorSystemJvmHook())
    catch {
      case _: RejectedExecutionException => cleanupActorSystemJvmHook()
    }
  }

  private def initPhaseActorSystemTerminate(
      system: ExtendedActorSystem,
      conf: Config,
      coord: CoordinatedShutdown): Unit = {
    coord.addTask(PhaseActorSystemTerminate, "terminate-system") { () =>
      val confForReason = confWithOverrides(conf, coord.shutdownReason())
      val terminateActorSystem = confForReason.getBoolean("terminate-actor-system")
      val exitJvm = confForReason.getBoolean("exit-jvm")
      val exitCode = confForReason.getInt("exit-code")

      if (exitJvm && terminateActorSystem) {
        // In case ActorSystem shutdown takes longer than the phase timeout,
        // exit the JVM forcefully anyway.
        // We must spawn a separate thread to not block current thread,
        // since that would have blocked the shutdown of the ActorSystem.
        val timeout = coord.timeout(PhaseActorSystemTerminate)
        val t = new Thread {
          override def run(): Unit = {
            if (Try(Await.ready(system.whenTerminated, timeout)).isFailure && !runningJvmHook)
              System.exit(exitCode)
          }
        }
        t.setName("CoordinatedShutdown-exit")
        t.start()
      }

      if (terminateActorSystem) {
        system.finalTerminate()
        system.whenTerminated.map { _ =>
          if (exitJvm && !runningJvmHook) System.exit(exitCode)
          Done
        }(ExecutionContext.parasitic)
      } else if (exitJvm) {
        System.exit(exitCode)
        Future.successful(Done)
      } else
        Future.successful(Done)
    }
  }

  private def initJvmHook(system: ActorSystem, conf: Config, coord: CoordinatedShutdown): Unit = {
    val runByJvmShutdownHook = system.settings.JvmShutdownHooks && conf.getBoolean("run-by-jvm-shutdown-hook")
    if (runByJvmShutdownHook) {
      coord.actorSystemJvmHook = OptionVal.Some(coord.addCancellableJvmShutdownHook {
        runningJvmHook = true // avoid System.exit from PhaseActorSystemTerminate task
        if (!system.whenTerminated.isCompleted) {
          coord.log.debug("Starting coordinated shutdown from JVM shutdown hook")
          try {
            // totalTimeout will be 0 when no tasks registered, so at least 3.seconds
            val totalTimeout = coord.totalTimeout().max(3.seconds)
            Await.ready(coord.run(JvmExitReason), totalTimeout)
          } catch {
            case NonFatal(e) =>
              coord.log.warning("CoordinatedShutdown from JVM shutdown failed: {}", e.getMessage)
          }
        }
      })
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final case class Phase(
      dependsOn: Set[String],
      timeout: FiniteDuration,
      recover: Boolean,
      enabled: Boolean)

  /**
   * INTERNAL API
   */
  private[akka] def phasesFromConfig(conf: Config): Map[String, Phase] = {
    import scala.jdk.CollectionConverters._
    val defaultPhaseTimeout = conf.getString("default-phase-timeout")
    val phasesConf = conf.getConfig("phases")
    val defaultPhaseConfig = ConfigFactory.parseString(s"""
      timeout = $defaultPhaseTimeout
      recover = true
      enabled = true
      depends-on = []
    """)
    phasesConf.root.unwrapped.asScala.toMap.map {
      case (k, _: java.util.Map[_, _]) =>
        val c = phasesConf.getConfig(k).withFallback(defaultPhaseConfig)
        val dependsOn = c.getStringList("depends-on").asScala.toSet
        val timeout = c.getDuration("timeout", MILLISECONDS).millis
        val recover = c.getBoolean("recover")
        val enabled = c.getBoolean("enabled")
        k -> Phase(dependsOn, timeout, recover, enabled)
      case (k, v) =>
        throw new IllegalArgumentException(s"Expected object value for [$k], got [$v]")
    }
  }

  /**
   * INTERNAL API: https://en.wikipedia.org/wiki/Topological_sorting
   */
  private[akka] def topologicalSort(phases: Map[String, Phase]): List[String] = {
    val all = (phases.keySet ++ phases.values.flatMap(_.dependsOn)).toList // in case phase is not defined as key
    TopologicalSort
      .topologicalSort[String](all, phases.get(_) match {
        case Some(phase) => phase.dependsOn
        case None        => Set.empty
      })
      .toList
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait JVMShutdownHooks {
  def addHook(t: Thread): Unit
  def removeHook(t: Thread): Boolean
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object JVMShutdownHooks extends JVMShutdownHooks {
  override def addHook(t: Thread): Unit = Runtime.getRuntime.addShutdownHook(t)
  override def removeHook(t: Thread): Boolean = Runtime.getRuntime.removeShutdownHook(t)
}

/**
 * Not for user instantiation, use the extension to access
 */
final class CoordinatedShutdown private[akka] (
    system: ExtendedActorSystem,
    phases: Map[String, CoordinatedShutdown.Phase],
    jvmShutdownHooks: JVMShutdownHooks = JVMShutdownHooks)
    extends Extension {
  import CoordinatedShutdown.Reason

  /** INTERNAL API */
  private[akka] val log = Logging(system, classOf[CoordinatedShutdown])
  private val knownPhases = phases.keySet ++ phases.values.flatMap(_.dependsOn)

  /** INTERNAL API */
  private[akka] val orderedPhases = CoordinatedShutdown.topologicalSort(phases)

  private trait PhaseDefinition {
    def size: Int
    def run(recoverEnabled: Boolean)(implicit ec: ExecutionContext): Future[Done]
  }

  private object tasks {
    private val registeredPhases = new ConcurrentHashMap[String, StrictPhaseDefinition]()
    private trait TaskDefinition extends Cancellable {
      private[tasks] def run(recoverEnabled: Boolean)(implicit ec: ExecutionContext): Future[Done]
    }
    private object TaskDefinition {
      def apply(phaseName: String, task: () => Future[Done], name: String): TaskDefinition = new TaskDefinition {
        // This is a vanilla class instead of a case class to avoid default implementations of .hashCode() and .equals
        // Different code paths could register the same task under the same name multiple times; in this case we want to
        // run that task as many times as it was registered (minus the number of times those were cancelled), so they must
        // be distinct in a Set[TaskDefinition].

        private sealed trait TaskState
        private case object Pending extends TaskState
        private case object Cancelled extends TaskState
        private case class Running(job: Promise[Done]) extends TaskState
        private val taskState = new AtomicReference[TaskState](Pending)

        @tailrec
        override private[tasks] def run(recoverEnabled: Boolean)(implicit ec: ExecutionContext): Future[Done] = {
          val job = Promise[Done]()
          val nextTaskState = taskState.updateAndGet {
            case Pending           => Running(job)
            case Cancelled         => Cancelled
            case Running(otherJob) => Running(otherJob)
          }
          nextTaskState match {
            case Running(runningJob) if runningJob == job =>
              // only start the job if atomic update succeeds and we were the winner of any race
              if (log.isDebugEnabled) {
                log.debug("Performing task [{}] in CoordinatedShutdown phase [{}]", name, phaseName)
              }
              job.completeWith(try {
                task.apply().recover {
                  case NonFatal(exc) if recoverEnabled =>
                    log.warning("Task [{}] failed in phase [{}]: {}", name, phaseName, exc.getMessage)
                    Done
                }
              } catch { // in case task.apply() throws
                case NonFatal(exc) if recoverEnabled =>
                  log.warning(
                    "Task [{}] in phase [{}] threw an exception before its future could be constructed: {}",
                    name,
                    phaseName,
                    exc.getMessage)
                  Future.successful(Done)
                case NonFatal(exc) =>
                  Future.failed(exc)
              })
              job.future
            case Running(otherJob) =>
              log.warning("Task [{}] in phase [{}] was invoked multiple times and deduplicated.", name, phaseName)
              otherJob.future
            case Cancelled =>
              Future.successful(Done)
            case Pending =>
              log.error("Atomic update produced an impossible value; this should never happen!")
              run(recoverEnabled)
          }
        }

        override def cancel(): Boolean = {
          val nextTaskState = taskState.updateAndGet {
            case Pending => Cancelled
            case other   => other
          }
          nextTaskState match {
            case Cancelled =>
              registeredPhases
                .merge(phaseName, StrictPhaseDefinition.empty, (previous, incoming) => previous.merge(incoming))
              if (log.isDebugEnabled) {
                log.debug("Successfully cancelled CoordinatedShutdown task [{}] from phase [{}].", name, phaseName)
              }
              true
            case _ =>
              false
          }
        }

        // must be side-effect free
        override def isCancelled: Boolean = {
          taskState.get() == Cancelled
        }
      }
    }

    private case class StrictPhaseDefinition(tasks: Set[TaskDefinition]) extends PhaseDefinition {
      // This is a case class so that the update methods on ConcurrentHashMap can correctly deal with equality
      override val size: Int = tasks.size

      override def run(recoverEnabled: Boolean)(implicit ec: ExecutionContext): Future[Done] = {
        Future.sequence(tasks.map(_.run(recoverEnabled))).map(_ => Done)(ExecutionContext.parasitic)
      }

      // This method may be run multiple times during the compare-and-set loop of ConcurrentHashMap, so it must be side-effect-free
      def merge(other: StrictPhaseDefinition): StrictPhaseDefinition = {
        val nextTasks = (tasks ++ other.tasks).filterNot(_.isCancelled)
        copy(tasks = nextTasks)
      }
    }

    private object StrictPhaseDefinition {
      def single(taskDefinition: TaskDefinition): StrictPhaseDefinition = {
        StrictPhaseDefinition(Set(taskDefinition))
      }
      val empty: StrictPhaseDefinition = StrictPhaseDefinition(Set.empty)
    }

    def get(phaseName: String): Option[PhaseDefinition] = Option(registeredPhases.get(phaseName))

    def totalDuration(): FiniteDuration = {
      import scala.jdk.CollectionConverters._
      registeredPhases.keySet.asScala.foldLeft(Duration.Zero) {
        case (acc, phase) =>
          acc + timeout(phase)
      }
    }

    def register(phaseName: String, task: () => Future[Done], name: String): Cancellable = {
      val cancellable: TaskDefinition = TaskDefinition(phaseName, task, name)

      registeredPhases.merge(
        phaseName,
        StrictPhaseDefinition.single(cancellable),
        (previous, incoming) => previous.merge(incoming))

      cancellable
    }
  }
  private val runStarted = new AtomicReference[Option[Reason]](None)
  private val runPromise = Promise[Done]()

  private val _jvmHooksLatch = new AtomicReference[CountDownLatch](new CountDownLatch(0))
  @volatile private var actorSystemJvmHook: OptionVal[Cancellable] = OptionVal.None

  /**
   * INTERNAL API
   */
  private[akka] lazy val terminationWatcher =
    system.systemActorOf(CoordinatedShutdownTerminationWatcher.props, "coordinatedShutdownTerminationWatcher")

  /**
   * INTERNAL API
   */
  private[akka] def jvmHooksLatch: CountDownLatch = _jvmHooksLatch.get

  /**
   * Scala API: Add a task to a phase, returning an object which will cancel it
   * on demand and remove it from the task pool (so long as the same task has not
   * been added elsewhere). Tasks in a phase are run concurrently, with no ordering
   * assumed.
   *
   * Adding a task to a phase does not remove any other tasks from the phase.
   *
   * If the same task is added multiple times, each addition will be run unless cancelled.
   *
   * Tasks should typically be registered as early as possible -- once coordinated
   * shutdown begins, tasks may be added without ever being run. A task may add tasks
   * to a later stage with confidence that they will be run.
   */
  def addCancellableTask(phase: String, taskName: String)(task: () => Future[Done]): Cancellable = {
    require(
      knownPhases(phase),
      s"Unknown phase [$phase], known phases [$knownPhases]. All phases (along with their optional dependencies) must be defined in configuration")
    require(
      taskName.nonEmpty,
      "Set a task name when adding tasks to the Coordinated Shutdown. " +
      "Try to use unique, self-explanatory names.")
    tasks.register(phase, task, taskName)
  }

  /**
   * Java API: Add a task to a phase, returning an object which will cancel it
   * on demand and remove it from the task pool (so long as the same task has not
   * been added elsewhere). Tasks in a phase are run concurrently, with no ordering
   * assumed.
   *
   * Adding a task to a phase does not remove any other tasks from the phase.
   *
   * If the same task is added multiple times, each addition will be run unless cancelled.
   *
   * Tasks should typically be registered as early as possible -- once coordinated
   * shutdown begins, tasks may be added without ever being run. A task may add tasks
   * to a later stage with confidence that they will be run.
   */
  def addCancellableTask(phase: String, taskName: String, task: Supplier[CompletionStage[Done]]): Cancellable = {
    addCancellableTask(phase, taskName)(() => task.get().asScala)
  }

  /**
   * Scala API: Add a task to a phase. It doesn't remove previously added tasks.
   * Tasks added to the same phase are executed in parallel without any
   * ordering assumptions. Next phase will not start until all tasks of
   * previous phase have been completed.
   *
   * Tasks should typically be registered as early as possible after system
   * startup. When running the coordinated shutdown tasks that have been registered
   * will be performed but tasks that are added too late will not be run.
   * It is possible to add a task to a later phase by a task in an earlier phase
   * and it will be performed.
   */
  def addTask(phase: String, taskName: String)(task: () => Future[Done]): Unit = {
    require(
      knownPhases(phase),
      s"Unknown phase [$phase], known phases [$knownPhases]. " +
      "All phases (along with their optional dependencies) must be defined in configuration")
    require(
      taskName.nonEmpty,
      "Set a task name when adding tasks to the Coordinated Shutdown. " +
      "Try to use unique, self-explanatory names.")
    tasks.register(phase, task, taskName)
  }

  /**
   * Java API: Add a task to a phase. It doesn't remove previously added tasks.
   * Tasks added to the same phase are executed in parallel without any
   * ordering assumptions. Next phase will not start until all tasks of
   * previous phase have been completed.
   *
   * Tasks should typically be registered as early as possible after system
   * startup. When running the coordinated shutdown tasks that have been registered
   * will be performed but tasks that are added too late will not be run.
   * It is possible to add a task to a later phase by a task in an earlier phase
   * and it will be performed.
   */
  def addTask(phase: String, taskName: String, task: Supplier[CompletionStage[Done]]): Unit =
    addTask(phase, taskName)(() => task.get().asScala)

  /**
   * Scala API: Add an actor termination task to a phase. It doesn't remove
   * previously added tasks. Tasks added to the same phase are executed in
   * parallel without any ordering assumptions. Next phase will not start until
   * all tasks of previous phase have been completed.
   *
   * When executed, this task will first send the given stop message, if defined,
   * to the actor, then it will watch the actor, and complete when the actor
   * terminates.
   *
   * Tasks should typically be registered as early as possible after system
   * startup. When running the coordinated shutdown tasks that have been registered
   * will be performed but tasks that are added too late will not be run.
   * It is possible to add a task to a later phase by a task in an earlier phase
   * and it will be performed.
   */
  def addActorTerminationTask(phase: String, taskName: String, actor: ActorRef, stopMsg: Option[Any]): Unit =
    addTask(phase, taskName) { () =>
      stopMsg.foreach(msg => actor ! msg)
      // addTask will verify that phases(phase) exists, so this should be safe
      val deadline = phases(phase).timeout.fromNow
      // Note: we cannot use ask because we cannot spawn actors during shutdown
      val completionPromise = Promise[Done]()
      terminationWatcher ! CoordinatedShutdownTerminationWatcher.Watch(actor, deadline, completionPromise)
      completionPromise.future
    }

  /**
   * Java API: Add an actor termination task to a phase. It doesn't remove
   * previously added tasks. Tasks added to the same phase are executed in
   * parallel without any ordering assumptions. Next phase will not start until
   * all tasks of previous phase have been completed.
   *
   * When executed, this task will first send the given stop message, if defined,
   * to the actor, then it will watch the actor, and complete when the actor
   * terminates.
   *
   * Tasks should typically be registered as early as possible after system
   * startup. When running the coordinated shutdown tasks that have been registered
   * will be performed but tasks that are added too late will not be run.
   * It is possible to add a task to a later phase by a task in an earlier phase
   * and it will be performed.
   */
  def addActorTerminationTask(phase: String, taskName: String, actor: ActorRef, stopMsg: Optional[Any]): Unit =
    addActorTerminationTask(phase, taskName, actor, stopMsg.toScala)

  /**
   * The `Reason` for the shutdown as passed to the `run` method. `None` if the shutdown
   * has not been started.
   */
  def shutdownReason(): Option[Reason] = runStarted.get()

  /**
   * The `Reason` for the shutdown as passed to the `run` method. `Optional.empty` if the shutdown
   * has not been started.
   */
  def getShutdownReason(): Optional[Reason] = shutdownReason().toJava

  /**
   * Scala API: Run tasks of all phases. The returned
   * `Future` is completed when all tasks have been completed,
   * or there is a failure when recovery is disabled.
   *
   * It's safe to call this method multiple times. It will only run the shutdown sequence once.
   */
  def run(reason: Reason): Future[Done] = run(reason, None)

  /**
   * Java API: Run tasks of all phases. The returned
   * `CompletionStage` is completed when all tasks have been completed,
   * or there is a failure when recovery is disabled.
   *
   * It's safe to call this method multiple times. It will only run the shutdown sequence once.
   */
  def runAll(reason: Reason): CompletionStage[Done] = run(reason).asJava

  /**
   * Scala API: Run tasks of all phases including and after the given phase.
   * The returned `Future` is completed when all such tasks have been completed,
   * or there is a failure when recovery is disabled.
   *
   * It's safe to call this method multiple times. It will only run shutdown sequence once.
   */
  def run(reason: Reason, fromPhase: Option[String]): Future[Done] = {
    if (runStarted.compareAndSet(None, Some(reason))) {
      implicit val ec: ExecutionContext = system.dispatchers.internalDispatcher
      val debugEnabled = log.isDebugEnabled
      log.info("Running CoordinatedShutdown with reason [{}]", reason)
      def loop(remainingPhases: List[String]): Future[Done] = {
        remainingPhases match {
          case Nil => Future.successful(Done)
          case phaseName :: remaining if !phases(phaseName).enabled =>
            tasks.get(phaseName).foreach { phaseDef =>
              log.info(s"Phase [{}] disabled through configuration, skipping [{}] tasks.", phaseName, phaseDef.size)
            }
            loop(remaining)
          case phaseName :: remaining =>
            val phaseResult = tasks.get(phaseName) match {
              case None =>
                if (debugEnabled) log.debug("Performing phase [{}] with [0] tasks", phaseName)
                Future.successful(Done)
              case Some(phaseDef) =>
                if (debugEnabled) {
                  log.debug("Performing phase [{}] with [{}] tasks.", phaseName, phaseDef.size)
                }
                val recoverEnabled = phases(phaseName).recover
                val result = phaseDef.run(recoverEnabled)
                val timeout = phases(phaseName).timeout
                val deadline = Deadline.now + timeout
                val timeoutFut = try {
                  after(timeout, system.scheduler) {
                    if (phaseName == CoordinatedShutdown.PhaseActorSystemTerminate && deadline.hasTimeLeft()) {
                      // too early, i.e. triggered by system termination
                      result
                    } else if (result.isCompleted)
                      Future.successful(Done)
                    else if (recoverEnabled) {
                      log.warning("Coordinated shutdown phase [{}] timed out after {}", phaseName, timeout)
                      Future.successful(Done)
                    } else
                      Future.failed(
                        new TimeoutException(s"Coordinated shutdown phase [$phaseName] timed out after $timeout"))
                  }
                } catch {
                  case _: IllegalStateException =>
                    // The call to `after` threw IllegalStateException, triggered by system termination
                    result
                }
                Future.firstCompletedOf(List(result, timeoutFut))
            }
            if (remaining.isEmpty)
              phaseResult // avoid flatMap when system terminated in last phase
            else
              phaseResult.flatMap(_ => loop(remaining))
        }
      }

      val remainingPhases = fromPhase match {
        case None    => orderedPhases // all
        case Some(p) => orderedPhases.dropWhile(_ != p)
      }
      val done = loop(remainingPhases)
      runPromise.completeWith(done)
    }
    runPromise.future
  }

  /**
   * Java API: Run tasks of all phases including and after the given phase.
   * The returned `CompletionStage` is completed when all such tasks have been completed,
   * or there is a failure when recovery is disabled.
   *
   * It's safe to call this method multiple times. It will only run the shutdown sequence once.
   */
  def run(reason: Reason, fromPhase: Optional[String]): CompletionStage[Done] =
    run(reason, fromPhase.toScala).asJava

  /**
   * The configured timeout for a given `phase`.
   * For example useful as timeout when actor `ask` requests
   * is used as a task.
   */
  def timeout(phase: String): FiniteDuration =
    phases.get(phase) match {
      case Some(p) => p.timeout
      case None =>
        throw new IllegalArgumentException(s"Unknown phase [$phase]. All phases must be defined in configuration")
    }

  /**
   * Sum of timeouts of all phases that have some task.
   */
  def totalTimeout(): FiniteDuration = {
    tasks.totalDuration()
  }

  /**
   * Scala API: Add a JVM shutdown hook that will be run when the JVM process
   * begins its shutdown sequence. Added hooks may run in any order
   * concurrently, but they are running before Akka internal shutdown
   * hooks, e.g. those shutting down Artery.
   */
  def addJvmShutdownHook[T](hook: => T): Unit = addCancellableJvmShutdownHook(hook)

  /**
   * Scala API: Add a JVM shutdown hook that will be run when the JVM process
   * begins its shutdown sequence. Added hooks may run in any order
   * concurrently, but they are running before Akka internal shutdown
   * hooks, e.g. those shutting down Artery.
   *
   * The returned ``Cancellable`` makes it possible to de-register the hook. For example
   * on actor system shutdown to avoid leaking references to the actor system in tests.
   *
   * For shutdown hooks that does not have any requirements on running before the Akka
   * shutdown hooks the standard library JVM shutdown hooks APIs are better suited.
   */
  @tailrec def addCancellableJvmShutdownHook[T](hook: => T): Cancellable = {
    if (runStarted.get.isEmpty) {
      val currentLatch = _jvmHooksLatch.get
      val newLatch = new CountDownLatch(currentLatch.getCount.toInt + 1)
      if (_jvmHooksLatch.compareAndSet(currentLatch, newLatch)) {
        val thread = new Thread {
          override def run(): Unit = {
            try hook
            finally _jvmHooksLatch.get.countDown()
          }
        }
        thread.setName(s"${system.name}-shutdown-hook-${newLatch.getCount}")
        try {
          jvmShutdownHooks.addHook(thread)
          new Cancellable {
            @volatile var cancelled = false
            def cancel(): Boolean = {
              try {
                if (jvmShutdownHooks.removeHook(thread)) {
                  cancelled = true
                  _jvmHooksLatch.get.countDown()
                  true
                } else {
                  false
                }
              } catch {
                case _: IllegalStateException =>
                  // shutdown already in progress
                  false
              }
            }
            def isCancelled: Boolean = cancelled
          }
        } catch {
          case e: IllegalStateException =>
            // Shutdown in progress, if CoordinatedShutdown is created via a JVM shutdown hook (Artery)
            log.warning("Could not addJvmShutdownHook, due to: {}", e.getMessage)
            _jvmHooksLatch.get.countDown()
            Cancellable.alreadyCancelled
        }
      } else
        addCancellableJvmShutdownHook(hook) // lost CAS, retry
    } else {
      Cancellable.alreadyCancelled
    }
  }

  /**
   * Java API: Add a JVM shutdown hook that will be run when the JVM process
   * begins its shutdown sequence. Added hooks may run in any order
   * concurrently, but they are running before Akka internal shutdown
   * hooks, e.g. those shutting down Artery.
   */
  def addJvmShutdownHook(hook: Runnable): Unit =
    addJvmShutdownHook(hook.run())

  /**
   * Java API: Add a JVM shutdown hook that will be run when the JVM process
   * begins its shutdown sequence. Added hooks may run in an order
   * concurrently, but they are running before Akka internal shutdown
   * hooks, e.g. those shutting down Artery.
   *
   * The returned ``Cancellable`` makes it possible to de-register the hook. For example
   * on actor system shutdown to avoid leaking references to the actor system in tests.
   *
   * For shutdown hooks that does not have any requirements on running before the Akka
   * shutdown hooks the standard library JVM shutdown hooks APIs are better suited.
   */
  def addCancellableJvmShutdownHook(hook: Runnable): Cancellable =
    addCancellableJvmShutdownHook(hook.run())

}

/** INTERNAL API */
@InternalApi
private[akka] object CoordinatedShutdownTerminationWatcher {
  final case class Watch(actor: ActorRef, deadline: Deadline, completionPromise: Promise[Done])
  private final case class WatchedTimedOut(actor: ActorRef, completionPromise: Promise[Done], timeout: FiniteDuration)

  def props: Props = Props(new CoordinatedShutdownTerminationWatcher)
}

/** INTERNAL API */
@InternalApi
private[akka] class CoordinatedShutdownTerminationWatcher extends Actor with Timers {

  private var waitingForActor: Map[ActorRef, Set[Promise[Done]]] = Map.empty
  private var alreadySeenTerminated = Set.empty[ActorRef]

  import CoordinatedShutdownTerminationWatcher._

  override def receive: Receive = {

    case Watch(actor, deadline, completionPromise) =>
      if (alreadySeenTerminated(actor)) completionPromise.trySuccess(Done)
      else {
        val newWaiting =
          waitingForActor.get(actor) match {
            case Some(alreadyWaiting) =>
              alreadyWaiting + completionPromise
            case None =>
              context.watch(actor)
              Set(completionPromise)
          }
        waitingForActor = waitingForActor.updated(actor, newWaiting)
        timers.startSingleTimer(
          completionPromise,
          WatchedTimedOut(actor, completionPromise, deadline.time),
          deadline.timeLeft)
      }

    case Terminated(actor) =>
      for {
        waitingPromises <- waitingForActor.get(actor)
        waitingPromise <- waitingPromises
      } {
        if (waitingPromise.trySuccess(Done)) {
          timers.cancel(waitingPromise)
        }
      }
      waitingForActor = waitingForActor - actor
      alreadySeenTerminated += actor

    case WatchedTimedOut(actor, completionPromise, timeout) =>
      if (!completionPromise.isCompleted)
        completionPromise.tryFailure(
          new TimeoutException(s"Actor [$actor] termination timed out after [$timeout] during coordinated shutdown"))

  }

}
