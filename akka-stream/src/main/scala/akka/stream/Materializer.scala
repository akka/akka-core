/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import scala.annotation.{ implicitNotFound, nowarn }
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Cancellable
import akka.actor.ClassicActorContextProvider
import akka.actor.ClassicActorSystemProvider
import akka.actor.Props
import akka.stream.impl.PhasedFusingActorMaterializer
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.event.LoggingAdapter

/**
 * The Materializer is the component responsible for turning a stream blueprint into a running stream.
 * In general the system wide materializer should be preferred over creating instances manually.
 *
 * Not for user extension
 */
@implicitNotFound("A Materializer is required.  You may want to have the ActorSystem in implicit scope")
@nowarn("msg=deprecated") // Name(symbol) is deprecated but older Scala versions don't have a string signature, since "2.5.8"
@DoNotInherit
abstract class Materializer {

  /**
   * The `namePrefix` shall be used for deriving the names of processing
   * entities that are created during materialization. This is meant to aid
   * logging and failure reporting both during materialization and while the
   * stream is running.
   */
  def withNamePrefix(name: String): Materializer

  /**
   * This method interprets the given Flow description and creates the running
   * stream. The result can be highly implementation specific, ranging from
   * local actor chains to remote-deployed processing networks.
   */
  def materialize[Mat](runnable: Graph[ClosedShape, Mat]): Mat

  /**
   * This method interprets the given Flow description and creates the running
   * stream using an explicitly provided [[Attributes]] as top level (least specific) attributes that
   * will be defaults for the materialized stream.
   * The result can be highly implementation specific, ranging from local actor chains to remote-deployed
   * processing networks.
   */
  def materialize[Mat](
      runnable: Graph[ClosedShape, Mat],
      @deprecatedName(Symbol("initialAttributes")) defaultAttributes: Attributes): Mat

  /**
   * Running a flow graph will require execution resources, as will computations
   * within Sources, Sinks, etc. This [[scala.concurrent.ExecutionContextExecutor]]
   * can be used by parts of the flow to submit processing jobs for execution,
   * run Future callbacks, etc.
   *
   * Note that this is not necessarily the same execution context the stream operator itself is running on.
   */
  implicit def executionContext: ExecutionContextExecutor

  /**
   * Interface for operators that need timer services for their functionality. Schedules a
   * single task with the given delay.
   *
   * @return A [[akka.actor.Cancellable]] that allows cancelling the timer. Cancelling is best effort, if the event
   *         has been already enqueued it will not have an effect.
   */
  def scheduleOnce(delay: FiniteDuration, task: Runnable): Cancellable

  /**
   * Interface for operators that need timer services for their functionality.
   *
   * Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a fixed `delay` between subsequent executions.
   *
   * It will not compensate the delay between tasks if the execution takes a long time or if
   * scheduling is delayed longer than specified for some reason. The delay between subsequent
   * execution will always be (at least) the given `delay`. In the long run, the
   * frequency of execution will generally be slightly lower than the reciprocal of the specified
   * `delay`.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   *   supported by the `Scheduler`.
   *
   * @return A [[akka.actor.Cancellable]] that allows cancelling the timer. Cancelling is best effort, if the event
   *         has been already enqueued it will not have an effect.
   */
  def scheduleWithFixedDelay(initialDelay: FiniteDuration, delay: FiniteDuration, task: Runnable): Cancellable

  /**
   * Interface for operators that need timer services for their functionality.
   *
   * Schedules a `Runnable` to be run repeatedly with an initial delay and
   * a frequency. E.g. if you would like the function to be run after 2
   * seconds and thereafter every 100ms you would set `delay=Duration(2, TimeUnit.SECONDS)`
   * and `interval=Duration(100, TimeUnit.MILLISECONDS)`.
   *
   * It will compensate the delay for a subsequent task if the previous tasks took
   * too long to execute. In such cases, the actual execution interval will differ from
   * the interval passed to the method.
   *
   * If the execution of the tasks takes longer than the `interval`, the subsequent
   * execution will start immediately after the prior one completes (there will be
   * no overlap of executions). This also has the consequence that after long garbage
   * collection pauses or other reasons when the JVM was suspended all "missed" tasks
   * will execute when the process wakes up again.
   *
   * In the long run, the frequency of execution will be exactly the reciprocal of the
   * specified `interval`.
   *
   * Warning: `scheduleAtFixedRate` can result in bursts of scheduled tasks after long
   * garbage collection pauses, which may in worst case cause undesired load on the system.
   * Therefore `scheduleWithFixedDelay` is often preferred.
   *
   * If the `Runnable` throws an exception the repeated scheduling is aborted,
   * i.e. the function will not be invoked any more.
   *
   * @throws IllegalArgumentException if the given delays exceed the maximum
   *   supported by the `Scheduler`.
   *
   * @return A [[akka.actor.Cancellable]] that allows cancelling the timer. Cancelling is best effort, if the event
   *         has been already enqueued it will not have an effect.
   */
  def scheduleAtFixedRate(initialDelay: FiniteDuration, interval: FiniteDuration, task: Runnable): Cancellable

  /**
   * Interface for operators that need timer services for their functionality. Schedules a
   * repeated task with the given interval between invocations.
   *
   * @return A [[akka.actor.Cancellable]] that allows cancelling the timer. Cancelling is best effort, if the event
   *         has been already enqueued it will not have an effect.
   */
  @deprecated(
    "Use scheduleWithFixedDelay or scheduleAtFixedRate instead. This has the same semantics as " +
    "scheduleAtFixedRate, but scheduleWithFixedDelay is often preferred.",
    since = "2.6.0")
  def schedulePeriodically(initialDelay: FiniteDuration, interval: FiniteDuration, task: Runnable): Cancellable

  /**
   * Shuts down this materializer and all the operators that have been materialized through this materializer. After
   * having shut down, this materializer cannot be used again. Any attempt to materialize operators after having
   * shut down will result in an IllegalStateException being thrown at materialization time.
   */
  def shutdown(): Unit

  /**
   * Indicates if the materializer has been shut down.
   */
  def isShutdown: Boolean

  /**
   * The classic actor system this materializer is backed by (and in which the streams materialized with the
   * materializer will run)
   */
  def system: ActorSystem

  /**
   * INTERNAL API
   *
   * Custom [[GraphStage]]s that needs logging should use [[akka.stream.stage.StageLogging]] (Scala) or
   * [[akka.stream.stage.GraphStageLogicWithLogging]] (Java) instead.
   */
  @InternalApi
  private[akka] def logger: LoggingAdapter

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def supervisor: ActorRef

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def actorOf(context: MaterializationContext, props: Props): ActorRef

  @deprecated("Use attributes to access settings from stages", "2.6.0")
  def settings: ActorMaterializerSettings
}

object Materializer {

  /**
   * Implicitly provides the system wide materializer from a classic or typed `ActorSystem`
   */
  implicit def matFromSystem(implicit provider: ClassicActorSystemProvider): Materializer =
    SystemMaterializer(provider.classicSystem).materializer

  /**
   * Scala API: Create a materializer whose lifecycle will be tied to the one of the passed actor context.
   * When the actor stops the materializer will stop and all streams created with it will be failed with an [[AbruptTerminationExeption]]
   *
   * You can pass either a classic actor context or a typed actor context.
   */
  @nowarn("msg=deprecated")
  def apply(contextProvider: ClassicActorContextProvider): Materializer =
    ActorMaterializer(None, None)(contextProvider.classicActorContext)

  /**
   * Java API: Create a materializer whose lifecycle will be tied to the one of the passed actor context.
   * When the actor stops the materializer will stop and all streams created with it will be failed with an [[AbruptTerminationExeption]]
   *
   * You can pass either a classic actor context or a typed actor context.
   */
  def createMaterializer(contextProvider: ClassicActorContextProvider): Materializer = apply(contextProvider)

  /**
   * Scala API: Create a materializer whose lifecycle will be tied to the one of the passed actor context.
   * When the actor stops the materializer will stop and all streams created with it will be failed with an [[AbruptTerminationExeption]]
   *
   * You can pass either a classic actor context or a typed actor context.
   */
  def apply(contextProvider: ClassicActorContextProvider, defaultAttributes: Attributes): Materializer = {
    val context = contextProvider.classicActorContext
    val settings = SystemMaterializer(context.system).materializerSettings

    PhasedFusingActorMaterializer(context, "flow", settings, settings.toAttributes and defaultAttributes)
  }

  /**
   * Java API: Create a materializer whose lifecycle will be tied to the one of the passed actor context.
   * When the actor stops the materializer will stop and all streams created with it will be failed with an [[AbruptTerminationExeption]]
   *
   * You can pass either a classic actor context or a typed actor context.
   */
  def createMaterializer(contextProvider: ClassicActorContextProvider, defaultAttributes: Attributes): Materializer =
    apply(contextProvider, defaultAttributes)

  /**
   * Scala API: Create a new materializer that will stay alive as long as the system does or until it is explicitly stopped.
   *
   * *Note* prefer using the default [[SystemMaterializer]] that is implicitly available if you have an implicit
   * `ActorSystem` in scope. Only create new system level materializers if you have specific
   * needs or want to test abrupt termination of a custom graph stage. If you want to tie the lifecycle
   * of the materializer to an actor, use the factory that takes an [[ActorContext]] instead.
   */
  def apply(systemProvider: ClassicActorSystemProvider): Materializer =
    SystemMaterializer(systemProvider.classicSystem).createAdditionalSystemMaterializer()

  /**
   * Java API: Create a new materializer that will stay alive as long as the system does or until it is explicitly stopped.
   *
   * *Note* prefer using the default [[SystemMaterializer]] by passing the `ActorSystem` to the various `run`
   * methods on the streams. Only create new system level materializers if you have specific
   * needs or want to test abrupt termination of a custom graph stage. If you want to tie the
   * lifecycle of the materializer to an actor, use the factory that takes an [[ActorContext]] instead.
   */
  def createMaterializer(systemProvider: ClassicActorSystemProvider): Materializer =
    apply(systemProvider)

  /**
   * Scala API: Create a new materializer that will stay alive as long as the system does or until it is explicitly stopped.
   *
   * It is generally advised to limit the number of system level materializers created.
   */
  def apply(systemProvider: ClassicActorSystemProvider, defaultAttributes: Attributes): Materializer =
    SystemMaterializer(systemProvider.classicSystem).createAdditionalSystemMaterializer(defaultAttributes)

  /**
   * Java API: Create a new materializer that will stay alive as long as the system does or until it is explicitly stopped.
   *
   * It is generally advised to limit the number of system level materializers created.
   */
  def createMaterializer(systemProvider: ClassicActorSystemProvider, defaultAttributes: Attributes): Materializer =
    apply(systemProvider, defaultAttributes)
}

/**
 * Context parameter to the `create` methods of sources and sinks.
 *
 * INTERNAL API
 */
@InternalApi
private[akka] case class MaterializationContext(
    materializer: Materializer,
    effectiveAttributes: Attributes,
    islandName: String)
