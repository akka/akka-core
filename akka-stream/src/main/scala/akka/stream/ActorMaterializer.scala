/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.ActorRefFactory
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Props
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.event.LoggingAdapter
import akka.japi.function
import akka.stream.impl._
import akka.stream.stage.GraphStageLogic
import akka.util.Helpers.toRootLowerCase

object ActorMaterializer {

  /**
   * Scala API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]] (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create one actor that in turn creates actors for the transformation steps.
   *
   * The materializer's [[akka.stream.ActorMaterializerSettings]] will be obtained from the
   * configuration of the `context`'s underlying [[akka.actor.ActorSystem]].
   *
   * The `namePrefix` is used as the first part of the names of the actors running
   * the processing steps. The default `namePrefix` is `"flow"`. The actor names are built up of
   * `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def apply(materializerSettings: Option[ActorMaterializerSettings] = None, namePrefix: Option[String] = None)(
      implicit context: ActorRefFactory): ActorMaterializer = {
    val system = actorSystemOf(context)

    val settings = materializerSettings.getOrElse(SystemMaterializer(system).materializerSettings)
    apply(settings, namePrefix.getOrElse("flow"))(context)
  }

  /**
   * Scala API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create these actors, therefore it is *forbidden* to pass this object
   * to another actor if the factory is an ActorContext.
   *
   * The `namePrefix` is used as the first part of the names of the actors running
   * the processing steps. The default `namePrefix` is `"flow"`. The actor names are built up of
   * `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def apply(materializerSettings: ActorMaterializerSettings, namePrefix: String)(
      implicit context: ActorRefFactory): ActorMaterializer = {

    context match {
      case system: ActorSystem =>
        // system level materializer, defer to the system materializer extension
        SystemMaterializer(system)
          .createAdditionalLegacySystemMaterializer(namePrefix, materializerSettings)
          .asInstanceOf[ActorMaterializer]

      case context: ActorContext =>
        // actor context level materializer, will live as a child of this actor
        PhasedFusingActorMaterializer(context, namePrefix, materializerSettings, materializerSettings.toAttributes)

      case other =>
        throw new IllegalArgumentException(s"Unexpected type of context: ${other}")
    }
  }

  /**
   * Scala API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create these actors, therefore it is *forbidden* to pass this object
   * to another actor if the factory is an ActorContext.
   *
   * The `namePrefix` is used as the first part of the names of the actors running
   * the processing steps. The default `namePrefix` is `"flow"`. The actor names are built up of
   * `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer or Materializer.apply(actorContext) with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def apply(materializerSettings: ActorMaterializerSettings)(implicit context: ActorRefFactory): ActorMaterializer =
    apply(Some(materializerSettings), None)

  /**
   * Java API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create these actors, therefore it is *forbidden* to pass this object
   * to another actor if the factory is an ActorContext.
   *
   * Defaults the actor name prefix used to name actors running the processing steps to `"flow"`.
   * The actor names are built up of `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer or Materializer.create(actorContext) with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def create(context: ActorRefFactory): ActorMaterializer =
    apply()(context)

  /**
   * Java API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create these actors, therefore it is *forbidden* to pass this object
   * to another actor if the factory is an ActorContext.
   *
   * The `namePrefix` is used as the first part of the names of the actors running
   * the processing steps. The default `namePrefix` is `"flow"`. The actor names are built up of
   * `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer or Materializer.create(actorContext) with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def create(context: ActorRefFactory, namePrefix: String): ActorMaterializer = {
    val system = actorSystemOf(context)
    val settings = ActorMaterializerSettings(system)
    apply(settings, namePrefix)(context)
  }

  /**
   * Java API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create one actor that in turn creates actors for the transformation steps.
   */
  @deprecated(
    "Use the system wide materializer or Materializer.create(actorContext) with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def create(settings: ActorMaterializerSettings, context: ActorRefFactory): ActorMaterializer =
    apply(Option(settings), None)(context)

  /**
   * Java API: Creates an ActorMaterializer that can materialize stream blueprints as running streams.
   *
   * The required [[akka.actor.ActorRefFactory]]
   * (which can be either an [[akka.actor.ActorSystem]] or an [[akka.actor.ActorContext]])
   * will be used to create these actors, therefore it is *forbidden* to pass this object
   * to another actor if the factory is an ActorContext.
   *
   * The `namePrefix` is used as the first part of the names of the actors running
   * the processing steps. The default `namePrefix` is `"flow"`. The actor names are built up of
   * `namePrefix-flowNumber-flowStepNumber-stepName`.
   */
  @deprecated(
    "Use the system wide materializer or Materializer.create(actorContext) with stream attributes or configuration settings to change defaults",
    "2.6.0")
  def create(settings: ActorMaterializerSettings, context: ActorRefFactory, namePrefix: String): ActorMaterializer =
    apply(Option(settings), Option(namePrefix))(context)

  private def actorSystemOf(context: ActorRefFactory): ActorSystem = {
    val system = context match {
      case s: ExtendedActorSystem => s
      case c: ActorContext        => c.system
      case null                   => throw new IllegalArgumentException("ActorRefFactory context must be defined")
      case _ =>
        throw new IllegalArgumentException(
          s"ActorRefFactory context must be an ActorSystem or ActorContext, got [${context.getClass.getName}]")
    }
    system
  }
}

/**
 * INTERNAL API
 */
private[akka] object ActorMaterializerHelper {

  /**
   * INTERNAL API
   */
  @deprecated("The Materializer now has all methods the ActorMaterializer used to have", "2.6.0")
  private[akka] def downcast(materializer: Materializer): ActorMaterializer =
    materializer match {
      case m: ActorMaterializer => m
      case _ =>
        throw new IllegalArgumentException(
          s"required [${classOf[ActorMaterializer].getName}] " +
          s"but got [${materializer.getClass.getName}]")
    }
}

/**
 * An ActorMaterializer takes a stream blueprint and turns it into a running stream.
 */
@deprecated("The Materializer now has all methods the ActorMaterializer used to have", "2.6.0")
abstract class ActorMaterializer extends Materializer with MaterializerLoggingProvider {

  @deprecated(
    "Use attributes to access settings from stages, see https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def settings: ActorMaterializerSettings

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
   * INTERNAL API
   */
  private[akka] def actorOf(context: MaterializationContext, props: Props): ActorRef

  /**
   * INTERNAL API
   */
  def system: ActorSystem

  /**
   * INTERNAL API
   */
  private[akka] def logger: LoggingAdapter

  /**
   * INTERNAL API
   */
  private[akka] def supervisor: ActorRef

}

/**
 * This exception or subtypes thereof should be used to signal materialization
 * failures.
 */
class MaterializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

/**
 * A base exception for abrupt stream termination.
 *
 * Not for user extension
 */
@DoNotInherit
sealed class AbruptStreamTerminationException(msg: String, cause: Throwable)
    extends RuntimeException(msg, cause)
    with NoStackTrace

/**
 * This exception signals that an actor implementing a Reactive Streams Subscriber, Publisher or Processor
 * has been terminated without being notified by an onError, onComplete or cancel signal. This usually happens
 * when an ActorSystem is shut down while stream processing actors are still running.
 */
final case class AbruptTerminationException(actor: ActorRef)
    extends AbruptStreamTerminationException(s"Processor actor [$actor] terminated abruptly", cause = null)

/**
 * Signal that the operator was abruptly terminated, usually seen as a call to `postStop` of the `GraphStageLogic` without
 * any of the handler callbacks seeing completion or failure from upstream or cancellation from downstream. This can happen when
 * the actor running the graph is killed, which happens when the materializer or actor system is terminated.
 */
final class AbruptStageTerminationException(logic: GraphStageLogic)
    extends AbruptStreamTerminationException(
      s"GraphStage [$logic] terminated abruptly, caused by for example materializer or actor system termination.",
      cause = null)

object ActorMaterializerSettings {

  /**
   * Create [[ActorMaterializerSettings]] from individual settings (Scala).
   *
   * Prefer using either config for defaults or attributes for per-stream config.
   * See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html"
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def apply(
      initialInputBufferSize: Int,
      maxInputBufferSize: Int,
      dispatcher: String,
      supervisionDecider: Supervision.Decider,
      subscriptionTimeoutSettings: StreamSubscriptionTimeoutSettings,
      debugLogging: Boolean,
      outputBurstLimit: Int,
      fuzzingMode: Boolean,
      autoFusing: Boolean,
      maxFixedBufferSize: Int) = {
    // these sins were committed in the name of bin comp:
    val config = ConfigFactory.defaultReference
    new ActorMaterializerSettings(
      initialInputBufferSize,
      maxInputBufferSize,
      dispatcher,
      supervisionDecider,
      subscriptionTimeoutSettings,
      debugLogging,
      outputBurstLimit,
      fuzzingMode,
      autoFusing,
      maxFixedBufferSize,
      1000,
      IOSettings(tcpWriteBufferSize = 16 * 1024),
      StreamRefSettings(config.getConfig("akka.stream.materializer.stream-ref")),
      config.getString(ActorAttributes.IODispatcher.dispatcher))
  }

  /**
   * Create [[ActorMaterializerSettings]] from the settings of an [[akka.actor.ActorSystem]] (Scala).
   *
   * Prefer using either config for defaults or attributes for per-stream config.
   * See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html"
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def apply(system: ActorSystem): ActorMaterializerSettings =
    apply(system.settings.config.getConfig("akka.stream.materializer"))

  /**
   * Create [[ActorMaterializerSettings]] from a Config subsection (Scala).
   *
   * Prefer using either config for defaults or attributes for per-stream config.
   * See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html"
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def apply(config: Config): ActorMaterializerSettings =
    new ActorMaterializerSettings(
      initialInputBufferSize = config.getInt("initial-input-buffer-size"),
      maxInputBufferSize = config.getInt("max-input-buffer-size"),
      dispatcher = config.getString("dispatcher"),
      supervisionDecider = Supervision.stoppingDecider,
      subscriptionTimeoutSettings = StreamSubscriptionTimeoutSettings(config),
      debugLogging = config.getBoolean("debug-logging"),
      outputBurstLimit = config.getInt("output-burst-limit"),
      fuzzingMode = config.getBoolean("debug.fuzzing-mode"),
      autoFusing = config.getBoolean("auto-fusing"),
      maxFixedBufferSize = config.getInt("max-fixed-buffer-size"),
      syncProcessingLimit = config.getInt("sync-processing-limit"),
      ioSettings = IOSettings(config.getConfig("io")),
      streamRefSettings = StreamRefSettings(config.getConfig("stream-ref")),
      blockingIoDispatcher = config.getString("blocking-io-dispatcher"))

  /**
   * Create [[ActorMaterializerSettings]] from individual settings (Java).
   *
   * Prefer using either config for defaults or attributes for per-stream config.
   * See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html"
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def create(
      initialInputBufferSize: Int,
      maxInputBufferSize: Int,
      dispatcher: String,
      supervisionDecider: Supervision.Decider,
      subscriptionTimeoutSettings: StreamSubscriptionTimeoutSettings,
      debugLogging: Boolean,
      outputBurstLimit: Int,
      fuzzingMode: Boolean,
      autoFusing: Boolean,
      maxFixedBufferSize: Int) = {
    // these sins were committed in the name of bin comp:
    val config = ConfigFactory.defaultReference
    new ActorMaterializerSettings(
      initialInputBufferSize,
      maxInputBufferSize,
      dispatcher,
      supervisionDecider,
      subscriptionTimeoutSettings,
      debugLogging,
      outputBurstLimit,
      fuzzingMode,
      autoFusing,
      maxFixedBufferSize,
      1000,
      IOSettings(tcpWriteBufferSize = 16 * 1024),
      StreamRefSettings(config.getConfig("akka.stream.materializer.stream-ref")),
      config.getString(ActorAttributes.IODispatcher.dispatcher))
  }

  /**
   * Create [[ActorMaterializerSettings]] from the settings of an [[akka.actor.ActorSystem]] (Java).
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def create(system: ActorSystem): ActorMaterializerSettings =
    apply(system)

  /**
   * Create [[ActorMaterializerSettings]] from a Config subsection (Java).
   *
   * Prefer using either config for defaults or attributes for per-stream config.
   * See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html"
   */
  @deprecated(
    "Use config or attributes to configure the materializer. See migration guide for details https://doc.akka.io/libraries/akka-core/2.6/project/migration-guide-2.5.x-2.6.x.html",
    "2.6.0")
  def create(config: Config): ActorMaterializerSettings =
    apply(config)

}

/**
 * This class describes the configurable properties of the [[ActorMaterializer]].
 * Please refer to the `withX` methods for descriptions of the individual settings.
 *
 * The constructor is not public API, use create or apply on the [[ActorMaterializerSettings]] companion instead.
 */
@nowarn("msg=deprecated")
final class ActorMaterializerSettings @InternalApi private (
    /*
     * Important note: `initialInputBufferSize`, `maxInputBufferSize`, `dispatcher` and
     * `supervisionDecider` must not be used as values in the materializer, or anything the materializer phases use
     * since these settings allow for overriding using [[Attributes]]. They must always be gotten from the effective
     * attributes.
     */
    @deprecated("Use attribute 'Attributes.InputBuffer' to read the concrete setting value", "2.6.0")
    val initialInputBufferSize: Int,
    @deprecated("Use attribute 'Attributes.InputBuffer' to read the concrete setting value", "2.6.0")
    val maxInputBufferSize: Int,
    @deprecated("Use attribute 'ActorAttributes.Dispatcher' to read the concrete setting value", "2.6.0")
    val dispatcher: String,
    @deprecated("Use attribute 'ActorAttributes.SupervisionStrategy' to read the concrete setting value", "2.6.0")
    val supervisionDecider: Supervision.Decider,
    val subscriptionTimeoutSettings: StreamSubscriptionTimeoutSettings,
    @deprecated("Use attribute 'ActorAttributes.DebugLogging' to read the concrete setting value", "2.6.0")
    val debugLogging: Boolean,
    @deprecated("Use attribute 'ActorAttributes.OutputBurstLimit' to read the concrete setting value", "2.6.0")
    val outputBurstLimit: Int,
    @deprecated("Use attribute 'ActorAttributes.FuzzingMode' to read the concrete setting value", "2.6.0")
    val fuzzingMode: Boolean,
    @deprecated("No longer has any effect", "2.6.0")
    val autoFusing: Boolean,
    @deprecated("Use attribute 'ActorAttributes.MaxFixedBufferSize' to read the concrete setting value", "2.6.0")
    val maxFixedBufferSize: Int,
    @deprecated("Use attribute 'ActorAttributes.SyncProcessingLimit' to read the concrete setting value", "2.6.0")
    val syncProcessingLimit: Int,
    val ioSettings: IOSettings,
    val streamRefSettings: StreamRefSettings,
    @deprecated("Use attribute 'ActorAttributes.BlockingIoDispatcher' to read the concrete setting value", "2.6.0")
    val blockingIoDispatcher: String) {

  require(initialInputBufferSize > 0, "initialInputBufferSize must be > 0")
  require(syncProcessingLimit > 0, "syncProcessingLimit must be > 0")

  requirePowerOfTwo(maxInputBufferSize, "maxInputBufferSize")
  require(
    initialInputBufferSize <= maxInputBufferSize,
    s"initialInputBufferSize($initialInputBufferSize) must be <= maxInputBufferSize($maxInputBufferSize)")

  private def copy(
      initialInputBufferSize: Int = this.initialInputBufferSize,
      maxInputBufferSize: Int = this.maxInputBufferSize,
      dispatcher: String = this.dispatcher,
      supervisionDecider: Supervision.Decider = this.supervisionDecider,
      subscriptionTimeoutSettings: StreamSubscriptionTimeoutSettings = this.subscriptionTimeoutSettings,
      debugLogging: Boolean = this.debugLogging,
      outputBurstLimit: Int = this.outputBurstLimit,
      fuzzingMode: Boolean = this.fuzzingMode,
      autoFusing: Boolean = this.autoFusing,
      maxFixedBufferSize: Int = this.maxFixedBufferSize,
      syncProcessingLimit: Int = this.syncProcessingLimit,
      ioSettings: IOSettings = this.ioSettings,
      streamRefSettings: StreamRefSettings = this.streamRefSettings,
      blockingIoDispatcher: String = this.blockingIoDispatcher) = {
    new ActorMaterializerSettings(
      initialInputBufferSize,
      maxInputBufferSize,
      dispatcher,
      supervisionDecider,
      subscriptionTimeoutSettings,
      debugLogging,
      outputBurstLimit,
      fuzzingMode,
      autoFusing,
      maxFixedBufferSize,
      syncProcessingLimit,
      ioSettings,
      streamRefSettings,
      blockingIoDispatcher)
  }

  // these are the core stream/materializer settings, ad hoc handling of defaults for the stage specific ones
  // for stream refs and io live with the respective stages
  private val asAttributes =
    Attributes(
      Attributes.InputBuffer(initialInputBufferSize, maxInputBufferSize) ::
      Attributes.CancellationStrategy.Default :: // FIXME: make configurable, see https://github.com/akka/akka-core/issues/28000
      Attributes.NestedMaterializationCancellationPolicy.Default ::
      ActorAttributes.Dispatcher(dispatcher) ::
      ActorAttributes.SupervisionStrategy(supervisionDecider) ::
      ActorAttributes.DebugLogging(debugLogging) ::
      ActorAttributes
        .StreamSubscriptionTimeout(subscriptionTimeoutSettings.timeout, subscriptionTimeoutSettings.mode) ::
      ActorAttributes.OutputBurstLimit(outputBurstLimit) ::
      ActorAttributes.FuzzingMode(fuzzingMode) ::
      ActorAttributes.MaxFixedBufferSize(maxFixedBufferSize) ::
      ActorAttributes.SyncProcessingLimit(syncProcessingLimit) ::
      Nil)

  /**
   * Each asynchronous piece of a materialized stream topology is executed by one Actor
   * that manages an input buffer for all inlets of its shape. This setting configures
   * the default for initial and maximal input buffer in number of elements for each inlet.
   * This can be overridden for individual parts of the
   * stream topology by using [[akka.stream.Attributes#inputBuffer]].
   *
   * FIXME: this is used for all kinds of buffers, not only the stream actor, some use initial some use max,
   *        document and or fix if it should not be like that. Search for get[Attributes.InputBuffer] to see how it is used
   */
  @deprecated("Use attribute 'Attributes.InputBuffer' to change setting value", "2.6.0")
  def withInputBuffer(initialSize: Int, maxSize: Int): ActorMaterializerSettings = {
    if (initialSize == this.initialInputBufferSize && maxSize == this.maxInputBufferSize) this
    else copy(initialInputBufferSize = initialSize, maxInputBufferSize = maxSize)
  }

  /**
   * This setting configures the default dispatcher to be used by streams materialized
   * with the [[ActorMaterializer]]. This can be overridden for individual parts of the
   * stream topology by using [[akka.stream.Attributes#dispatcher]].
   */
  @deprecated("Use attribute 'ActorAttributes.Dispatcher' to change setting value", "2.6.0")
  def withDispatcher(dispatcher: String): ActorMaterializerSettings = {
    if (this.dispatcher == dispatcher) this
    else copy(dispatcher = dispatcher)
  }

  /**
   * Scala API: Decides how exceptions from application code are to be handled, unless
   * overridden for specific flows of the stream operations with
   * [[akka.stream.Attributes#supervisionStrategy]].
   *
   * Note that supervision in streams are implemented on a per operator basis and is not supported
   * by every operator.
   */
  @deprecated("Use attribute 'ActorAttributes.supervisionStrategy' to change setting value", "2.6.0")
  def withSupervisionStrategy(decider: Supervision.Decider): ActorMaterializerSettings = {
    if (decider eq this.supervisionDecider) this
    else copy(supervisionDecider = decider)
  }

  /**
   * Java API: Decides how exceptions from application code are to be handled, unless
   * overridden for specific flows of the stream operations with
   * [[akka.stream.Attributes#supervisionStrategy]].
   *
   * Note that supervision in streams are implemented on a per operator basis and is not supported
   * by every operator.
   */
  @deprecated("Use attribute 'ActorAttributes.SupervisionStrategy' to change setting value", "2.6.0")
  def withSupervisionStrategy(
      decider: function.Function[Throwable, Supervision.Directive]): ActorMaterializerSettings = {
    import Supervision._
    copy(supervisionDecider = decider match {
      case `resumingDecider`   => resumingDecider
      case `restartingDecider` => restartingDecider
      case `stoppingDecider`   => stoppingDecider
      case other               => other.apply _
    })
  }

  /**
   * Test utility: fuzzing mode means that GraphStage events are not processed
   * in FIFO order within a fused subgraph, but randomized.
   */
  @deprecated("Use attribute 'ActorAttributes.FuzzingMode' to change setting value", "2.6.0")
  def withFuzzing(enable: Boolean): ActorMaterializerSettings =
    if (enable == this.fuzzingMode) this
    else copy(fuzzingMode = enable)

  /**
   * Maximum number of elements emitted in batch if downstream signals large demand.
   */
  @deprecated("Use attribute 'ActorAttributes.OutputBurstLimit' to change setting value", "2.6.0")
  def withOutputBurstLimit(limit: Int): ActorMaterializerSettings =
    if (limit == this.outputBurstLimit) this
    else copy(outputBurstLimit = limit)

  /**
   * Limit for number of messages that can be processed synchronously in stream to substream communication
   */
  @deprecated("Use attribute 'ActorAttributes.SyncProcessingLimit' to change setting value", "2.6.0")
  def withSyncProcessingLimit(limit: Int): ActorMaterializerSettings =
    if (limit == this.syncProcessingLimit) this
    else copy(syncProcessingLimit = limit)

  /**
   * Enable to log all elements that are dropped due to failures (at DEBUG level).
   */
  @deprecated("Use attribute 'ActorAttributes.DebugLogging' to change setting value", "2.6.0")
  def withDebugLogging(enable: Boolean): ActorMaterializerSettings =
    if (enable == this.debugLogging) this
    else copy(debugLogging = enable)

  /**
   * Configure the maximum buffer size for which a FixedSizeBuffer will be preallocated.
   * This defaults to a large value because it is usually better to fail early when
   * system memory is not sufficient to hold the buffer.
   */
  @deprecated("Use attribute 'ActorAttributes.MaxFixedBufferSize' to change setting value", "2.6.0")
  def withMaxFixedBufferSize(size: Int): ActorMaterializerSettings =
    if (size == this.maxFixedBufferSize) this
    else copy(maxFixedBufferSize = size)

  /**
   * Leaked publishers and subscribers are cleaned up when they are not used within a given
   * deadline, configured by [[StreamSubscriptionTimeoutSettings]].
   */
  def withSubscriptionTimeoutSettings(settings: StreamSubscriptionTimeoutSettings): ActorMaterializerSettings =
    if (settings == this.subscriptionTimeoutSettings) this
    else copy(subscriptionTimeoutSettings = settings)

  def withIOSettings(ioSettings: IOSettings): ActorMaterializerSettings =
    if (ioSettings == this.ioSettings) this
    else copy(ioSettings = ioSettings)

  /** Change settings specific to [[SourceRef]] and [[SinkRef]]. */
  def withStreamRefSettings(streamRefSettings: StreamRefSettings): ActorMaterializerSettings =
    if (streamRefSettings == this.streamRefSettings) this
    else copy(streamRefSettings = streamRefSettings)

  @deprecated("Use attribute 'ActorAttributes.BlockingIoDispatcher' to change setting value", "2.6.0")
  def withBlockingIoDispatcher(newBlockingIoDispatcher: String): ActorMaterializerSettings =
    if (newBlockingIoDispatcher == blockingIoDispatcher) this
    else copy(blockingIoDispatcher = newBlockingIoDispatcher)

  private def requirePowerOfTwo(n: Integer, name: String): Unit = {
    require(n > 0, s"$name must be > 0")
    require((n & (n - 1)) == 0, s"$name must be a power of two")
  }

  override def equals(other: Any): Boolean = other match {
    case s: ActorMaterializerSettings =>
      s.initialInputBufferSize == initialInputBufferSize &&
      s.maxInputBufferSize == maxInputBufferSize &&
      s.dispatcher == dispatcher &&
      s.supervisionDecider == supervisionDecider &&
      s.subscriptionTimeoutSettings == subscriptionTimeoutSettings &&
      s.debugLogging == debugLogging &&
      s.outputBurstLimit == outputBurstLimit &&
      s.syncProcessingLimit == syncProcessingLimit &&
      s.fuzzingMode == fuzzingMode &&
      s.autoFusing == autoFusing &&
      s.ioSettings == ioSettings &&
      s.blockingIoDispatcher == blockingIoDispatcher
    case _ => false
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def toAttributes: Attributes = asAttributes

  override def toString: String =
    s"ActorMaterializerSettings($initialInputBufferSize,$maxInputBufferSize," +
    s"$dispatcher,$supervisionDecider,$subscriptionTimeoutSettings,$debugLogging,$outputBurstLimit," +
    s"$syncProcessingLimit,$fuzzingMode,$autoFusing,$ioSettings)"
}

object IOSettings {
  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def apply(system: ActorSystem): IOSettings =
    apply(system.settings.config.getConfig("akka.stream.materializer.io"))

  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def apply(config: Config): IOSettings =
    new IOSettings(
      tcpWriteBufferSize = math.min(Int.MaxValue, config.getBytes("tcp.write-buffer-size")).toInt,
      coalesceWrites = config.getInt("tcp.coalesce-writes"))

  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def apply(tcpWriteBufferSize: Int): IOSettings =
    new IOSettings(tcpWriteBufferSize)

  /** Java API */
  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def create(config: Config) = apply(config)

  /** Java API */
  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def create(system: ActorSystem) = apply(system)

  /** Java API */
  @deprecated(
    "Use setting 'akka.stream.materializer.io.tcp.write-buffer-size' or attribute TcpAttributes.writeBufferSize instead",
    "2.6.0")
  def create(tcpWriteBufferSize: Int): IOSettings =
    apply(tcpWriteBufferSize)
}

@nowarn("msg=deprecated")
final class IOSettings private (
    @deprecated("Use attribute 'TcpAttributes.TcpWriteBufferSize' to read the concrete setting value", "2.6.0")
    val tcpWriteBufferSize: Int,
    val coalesceWrites: Int) {

  // constructor for binary compatibility with version 2.6.15 and earlier
  @deprecated("Use attribute 'TcpAttributes.TcpWriteBufferSize' to read the concrete setting value", "2.6.0")
  def this(tcpWriteBufferSize: Int) = this(tcpWriteBufferSize, coalesceWrites = 10)

  def withTcpWriteBufferSize(value: Int): IOSettings = copy(tcpWriteBufferSize = value)

  def withCoalesceWrites(value: Int): IOSettings = copy(coalesceWrites = value)

  private def copy(tcpWriteBufferSize: Int = tcpWriteBufferSize, coalesceWrites: Int = coalesceWrites): IOSettings =
    new IOSettings(tcpWriteBufferSize, coalesceWrites)

  override def equals(other: Any): Boolean = other match {
    case s: IOSettings => s.tcpWriteBufferSize == tcpWriteBufferSize && s.coalesceWrites == coalesceWrites
    case _             => false
  }

  override def hashCode(): Int =
    31 * tcpWriteBufferSize + coalesceWrites

  override def toString =
    s"""IoSettings($tcpWriteBufferSize,$coalesceWrites)"""
}

object StreamSubscriptionTimeoutSettings {
  import akka.stream.StreamSubscriptionTimeoutTerminationMode._

  /**
   * Create settings from individual values (Java).
   */
  def create(
      mode: StreamSubscriptionTimeoutTerminationMode,
      timeout: FiniteDuration): StreamSubscriptionTimeoutSettings =
    new StreamSubscriptionTimeoutSettings(mode, timeout)

  /**
   * Create settings from individual values (Scala).
   */
  def apply(
      mode: StreamSubscriptionTimeoutTerminationMode,
      timeout: FiniteDuration): StreamSubscriptionTimeoutSettings =
    new StreamSubscriptionTimeoutSettings(mode, timeout)

  /**
   * Create settings from a Config subsection (Java).
   */
  def create(config: Config): StreamSubscriptionTimeoutSettings =
    apply(config)

  /**
   * Create settings from a Config subsection (Scala).
   */
  def apply(config: Config): StreamSubscriptionTimeoutSettings = {
    val c = config.getConfig("subscription-timeout")
    StreamSubscriptionTimeoutSettings(mode = toRootLowerCase(c.getString("mode")) match {
      case "no" | "off" | "false" | "noop" => NoopTermination
      case "warn"                          => WarnTermination
      case "cancel"                        => CancelTermination
    }, timeout = c.getDuration("timeout", TimeUnit.MILLISECONDS).millis)
  }
}

/**
 * Leaked publishers and subscribers are cleaned up when they are not used within a given
 * deadline, configured by [[StreamSubscriptionTimeoutSettings]].
 */
@nowarn("msg=deprecated")
final class StreamSubscriptionTimeoutSettings(
    @deprecated(
      "Use attribute 'ActorAttributes.StreamSubscriptionTimeoutMode' to read the concrete setting value",
      "2.6.0")
    val mode: StreamSubscriptionTimeoutTerminationMode,
    @deprecated("Use attribute 'ActorAttributes.StreamSubscriptionTimeout' to read the concrete setting value", "2.6.0")
    val timeout: FiniteDuration) {
  override def equals(other: Any): Boolean = other match {
    case s: StreamSubscriptionTimeoutSettings => s.mode == mode && s.timeout == timeout
    case _                                    => false
  }
  override def toString: String = s"StreamSubscriptionTimeoutSettings($mode,$timeout)"
}

/**
 * This mode describes what shall happen when the subscription timeout expires for
 * substream Publishers created by operations like `prefixAndTail`.
 */
sealed abstract class StreamSubscriptionTimeoutTerminationMode

object StreamSubscriptionTimeoutTerminationMode {
  case object NoopTermination extends StreamSubscriptionTimeoutTerminationMode
  case object WarnTermination extends StreamSubscriptionTimeoutTerminationMode
  case object CancelTermination extends StreamSubscriptionTimeoutTerminationMode

  /**
   * Do not do anything when timeout expires.
   */
  def noop: StreamSubscriptionTimeoutTerminationMode = NoopTermination

  /**
   * Log a warning when the timeout expires.
   */
  def warn: StreamSubscriptionTimeoutTerminationMode = WarnTermination

  /**
   * When the timeout expires attach a Subscriber that will immediately cancel its subscription.
   */
  def cancel: StreamSubscriptionTimeoutTerminationMode = CancelTermination

}
