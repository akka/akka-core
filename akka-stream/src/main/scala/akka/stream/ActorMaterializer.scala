/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.util.control.NoStackTrace
import com.typesafe.config.Config
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.stream.stage.GraphStageLogic
import akka.util.Helpers.toRootLowerCase

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

  /** INTERNAL API */
  @InternalApi
  private[akka] def apply(system: ActorSystem): ActorMaterializerSettings =
    apply(system.settings.config.getConfig("akka.stream.materializer"))

  /** INTERNAL API */
  @InternalApi
  private[akka] def apply(config: Config): ActorMaterializerSettings =
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
      streamRefSettings = akka.stream.impl.streamref.StreamRefSettingsImpl(config.getConfig("stream-ref")),
      blockingIoDispatcher = config.getString("blocking-io-dispatcher"))

}

/**
 * This class describes the configurable properties of the materializer.
 * Please refer to the `withX` methods for descriptions of the individual settings.
 *
 * The constructor is not public API.
 */
final class ActorMaterializerSettings @InternalApi private (
    /*
     * Important note: `initialInputBufferSize`, `maxInputBufferSize`, `dispatcher` and
     * `supervisionDecider` must not be used as values in the materializer, or anything the materializer phases use
     * since these settings allow for overriding using [[Attributes]]. They must always be gotten from the effective
     * attributes.
     */
    private[akka] val initialInputBufferSize: Int,
    private[akka] val maxInputBufferSize: Int,
    private[akka] val dispatcher: String,
    private[akka] val supervisionDecider: Supervision.Decider,
    val subscriptionTimeoutSettings: StreamSubscriptionTimeoutSettings,
    private[akka] val debugLogging: Boolean,
    private[akka] val outputBurstLimit: Int,
    private[akka] val fuzzingMode: Boolean,
    private[akka] val autoFusing: Boolean,
    private[akka] val maxFixedBufferSize: Int,
    private[akka] val syncProcessingLimit: Int,
    val ioSettings: IOSettings,
    val streamRefSettings: StreamRefSettings,
    private[akka] val blockingIoDispatcher: String) {

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

  /** INTERNAL API */
  @InternalApi
  private[akka] def withInputBuffer(initialSize: Int, maxSize: Int): ActorMaterializerSettings =
    if (initialSize == this.initialInputBufferSize && maxSize == this.maxInputBufferSize) this
    else copy(initialInputBufferSize = initialSize, maxInputBufferSize = maxSize)

  /** INTERNAL API */
  @InternalApi
  private[akka] def withDispatcher(dispatcher: String): ActorMaterializerSettings =
    if (this.dispatcher == dispatcher) this
    else copy(dispatcher = dispatcher)

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

  /** INTERNAL API */
  @InternalApi
  private[akka] def apply(config: Config): IOSettings =
    new IOSettings(
      tcpWriteBufferSize = math.min(Int.MaxValue, config.getBytes("tcp.write-buffer-size")).toInt,
      coalesceWrites = config.getInt("tcp.coalesce-writes"))

}

final class IOSettings private (private[akka] val tcpWriteBufferSize: Int, val coalesceWrites: Int) {

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
final class StreamSubscriptionTimeoutSettings(
    private[akka] val mode: StreamSubscriptionTimeoutTerminationMode,
    private[akka] val timeout: FiniteDuration) {
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
