/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.net.InetAddress

import scala.annotation.nowarn
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.NotUsed
import akka.io.dns.internal.AsyncDnsResolver
import akka.japi.Util.immutableSeq
import akka.stream.ActorMaterializerSettings
import akka.util.Helpers.ConfigOps
import akka.util.Helpers.Requiring
import akka.util.Helpers.toRootLowerCase
import akka.util.WildcardIndex
import scala.jdk.CollectionConverters._

/** INTERNAL API */
private[akka] final class ArterySettings private (config: Config) {
  import ArterySettings._
  import config._

  def withDisabledCompression(): ArterySettings =
    ArterySettings(ConfigFactory.parseString("""|akka.remote.artery.advanced.compression {
         |  actor-refs.max = off
         |  manifests.max = off
         |}""".stripMargin).withFallback(config))

  val Enabled: Boolean = getBoolean("enabled")

  object Canonical {
    val config: Config = getConfig("canonical")
    import config._

    val Port: Int = getInt("port").requiring(port => 0 to 65535 contains port, "canonical.port must be 0 through 65535")
    val Hostname: String = getHostname("hostname", config)
  }

  object Bind {
    val config: Config = getConfig("bind")
    import config._

    val Port: Int = getString("port") match {
      case "" => Canonical.Port
      case _  => getInt("port").requiring(port => 0 to 65535 contains port, "bind.port must be 0 through 65535")
    }
    val Hostname: String = getHostname("hostname", config) match {
      case ""    => Canonical.Hostname
      case other => other
    }

    val BindTimeout: FiniteDuration =
      config.getMillisDuration("bind-timeout").requiring(_ > Duration.Zero, "bind-timeout can not be negative")
  }

  val LargeMessageDestinations: WildcardIndex[NotUsed] =
    config.getStringList("large-message-destinations").asScala.foldLeft(WildcardIndex[NotUsed]()) { (tree, entry) =>
      val segments = entry.split('/').tail
      tree.insert(segments, NotUsed)
    }

  val SSLEngineProviderClassName: String = config.getString("ssl.ssl-engine-provider")

  val UntrustedMode: Boolean = getBoolean("untrusted-mode")
  val TrustedSelectionPaths: Set[String] = immutableSeq(getStringList("trusted-selection-paths")).toSet

  val LogReceive: Boolean = getBoolean("log-received-messages")
  val LogSend: Boolean = getBoolean("log-sent-messages")

  val LogFrameSizeExceeding: Option[Int] = {
    if (toRootLowerCase(getString("log-frame-size-exceeding")) == "off") None
    else Some(getBytes("log-frame-size-exceeding").toInt)
  }

  val Transport: Transport = toRootLowerCase(getString("transport")) match {
    case AeronUpd.configName => AeronUpd
    case Tcp.configName      => Tcp
    case TlsTcp.configName   => TlsTcp
    case other =>
      throw new IllegalArgumentException(
        s"Unknown transport [$other], possible values: " +
        s""""${AeronUpd.configName}", "${Tcp.configName}", or "${TlsTcp.configName}"""")
  }

  /**
   * Used version of the header format for outbound messages.
   * To support rolling upgrades this may be a lower version than `ArteryTransport.HighestVersion`,
   * which is the highest supported version on receiving (decoding) side.
   */
  val Version: Byte = ArteryTransport.HighestVersion

  object Advanced {
    val config: Config = getConfig("advanced")
    import config._

    val TestMode: Boolean = getBoolean("test-mode")
    val Dispatcher: String = getString("use-dispatcher")
    val ControlStreamDispatcher: String = getString("use-control-stream-dispatcher")
    @nowarn("msg=deprecated")
    val MaterializerSettings: ActorMaterializerSettings =
      ActorMaterializerSettings(config.getConfig("materializer")).withDispatcher(Dispatcher)
    @nowarn("msg=deprecated")
    val ControlStreamMaterializerSettings: ActorMaterializerSettings =
      ActorMaterializerSettings(config.getConfig("materializer")).withDispatcher(ControlStreamDispatcher)

    val OutboundLanes: Int = getInt("outbound-lanes").requiring(n => n > 0, "outbound-lanes must be greater than zero")
    val InboundLanes: Int = getInt("inbound-lanes").requiring(n => n > 0, "inbound-lanes must be greater than zero")
    val SysMsgBufferSize: Int =
      getInt("system-message-buffer-size").requiring(_ > 0, "system-message-buffer-size must be more than zero")
    val OutboundMessageQueueSize: Int =
      getInt("outbound-message-queue-size").requiring(_ > 0, "outbound-message-queue-size must be more than zero")
    val OutboundControlQueueSize: Int =
      getInt("outbound-control-queue-size").requiring(_ > 0, "outbound-control-queue-size must be more than zero")
    val OutboundLargeMessageQueueSize: Int = getInt("outbound-large-message-queue-size")
      .requiring(_ > 0, "outbound-large-message-queue-size must be more than zero")
    val SystemMessageResendInterval: FiniteDuration =
      config
        .getMillisDuration("system-message-resend-interval")
        .requiring(interval => interval > Duration.Zero, "system-message-resend-interval must be more than zero")
    val HandshakeTimeout: FiniteDuration = config
      .getMillisDuration("handshake-timeout")
      .requiring(interval => interval > Duration.Zero, "handshake-timeout must be more than zero")
    val HandshakeRetryInterval: FiniteDuration =
      config
        .getMillisDuration("handshake-retry-interval")
        .requiring(interval => interval > Duration.Zero, "handshake-retry-interval must be more than zero")
    val InjectHandshakeInterval: FiniteDuration =
      config
        .getMillisDuration("inject-handshake-interval")
        .requiring(interval => interval > Duration.Zero, "inject-handshake-interval must be more than zero")
    val GiveUpSystemMessageAfter: FiniteDuration =
      config
        .getMillisDuration("give-up-system-message-after")
        .requiring(interval => interval > Duration.Zero, "give-up-system-message-after must be more than zero")
    val StopIdleOutboundAfter: FiniteDuration = config
      .getMillisDuration("stop-idle-outbound-after")
      .requiring(interval => interval > Duration.Zero, "stop-idle-outbound-after must be more than zero")
    val QuarantineIdleOutboundAfter: FiniteDuration = config
      .getMillisDuration("quarantine-idle-outbound-after")
      .requiring(
        interval => interval > StopIdleOutboundAfter,
        "quarantine-idle-outbound-after must be greater than stop-idle-outbound-after")
    val StopQuarantinedAfterIdle: FiniteDuration =
      config
        .getMillisDuration("stop-quarantined-after-idle")
        .requiring(interval => interval > Duration.Zero, "stop-quarantined-after-idle must be more than zero")
    val RemoveQuarantinedAssociationAfter: FiniteDuration =
      config
        .getMillisDuration("remove-quarantined-association-after")
        .requiring(interval => interval > Duration.Zero, "remove-quarantined-association-after must be more than zero")
    val ShutdownFlushTimeout: FiniteDuration =
      config
        .getMillisDuration("shutdown-flush-timeout")
        .requiring(timeout => timeout > Duration.Zero, "shutdown-flush-timeout must be more than zero")
    val DeathWatchNotificationFlushTimeout: FiniteDuration = {
      toRootLowerCase(config.getString("death-watch-notification-flush-timeout")) match {
        case "off" => Duration.Zero
        case _ =>
          config
            .getMillisDuration("death-watch-notification-flush-timeout")
            .requiring(
              interval => interval > Duration.Zero,
              "death-watch-notification-flush-timeout must be more than zero, or off")
      }
    }
    val InboundRestartTimeout: FiniteDuration =
      config
        .getMillisDuration("inbound-restart-timeout")
        .requiring(interval => interval > Duration.Zero, "inbound-restart-timeout must be more than zero")
    val InboundMaxRestarts: Int = getInt("inbound-max-restarts")
    val OutboundRestartBackoff: FiniteDuration =
      config
        .getMillisDuration("outbound-restart-backoff")
        .requiring(interval => interval > Duration.Zero, "outbound-restart-backoff must be more than zero")
    val OutboundRestartTimeout: FiniteDuration =
      config
        .getMillisDuration("outbound-restart-timeout")
        .requiring(interval => interval > Duration.Zero, "outbound-restart-timeout must be more than zero")
    val OutboundMaxRestarts: Int = getInt("outbound-max-restarts")
    val Compression = new Compression(getConfig("compression"))

    final val MaximumFrameSize: Int = math
      .min(getBytes("maximum-frame-size"), Int.MaxValue)
      .toInt
      .requiring(_ >= 32 * 1024, "maximum-frame-size must be greater than or equal to 32 KiB")
    final val BufferPoolSize: Int =
      getInt("buffer-pool-size").requiring(_ > 0, "buffer-pool-size must be greater than 0")
    final val InboundHubBufferSize = BufferPoolSize / 2
    final val MaximumLargeFrameSize: Int = math
      .min(getBytes("maximum-large-frame-size"), Int.MaxValue)
      .toInt
      .requiring(_ >= 32 * 1024, "maximum-large-frame-size must be greater than or equal to 32 KiB")
    final val LargeBufferPoolSize: Int =
      getInt("large-buffer-pool-size").requiring(_ > 0, "large-buffer-pool-size must be greater than 0")

    object Aeron {
      val config: Config = getConfig("aeron")

      val LogAeronCounters: Boolean = config.getBoolean("log-aeron-counters")
      val EmbeddedMediaDriver: Boolean = config.getBoolean("embedded-media-driver")
      val AeronDirectoryName: String = config
        .getString("aeron-dir")
        .requiring(
          dir => EmbeddedMediaDriver || dir.nonEmpty,
          "aeron-dir must be defined when using external media driver")
      val DeleteAeronDirectory: Boolean = config.getBoolean("delete-aeron-dir")
      val IdleCpuLevel: Int =
        config
          .getInt("idle-cpu-level")
          .requiring(level => 1 <= level && level <= 10, "idle-cpu-level must be between 1 and 10")
      val GiveUpMessageAfter: FiniteDuration = config
        .getMillisDuration("give-up-message-after")
        .requiring(interval => interval > Duration.Zero, "give-up-message-after must be more than zero")
      val ClientLivenessTimeout: FiniteDuration =
        config
          .getMillisDuration("client-liveness-timeout")
          .requiring(interval => interval > Duration.Zero, "client-liveness-timeout must be more than zero")
      val PublicationUnblockTimeout: FiniteDuration =
        config
          .getMillisDuration("publication-unblock-timeout")
          .requiring(interval => interval > Duration.Zero, "publication-unblock-timeout must be greater than zero")
      val ImageLivenessTimeout: FiniteDuration = config
        .getMillisDuration("image-liveness-timeout")
        .requiring(interval => interval > Duration.Zero, "image-liveness-timeout must be more than zero")
      if (Transport == AeronUpd)
        require(ImageLivenessTimeout < HandshakeTimeout, "image-liveness-timeout must be less than handshake-timeout")
      val DriverTimeout: FiniteDuration = config
        .getMillisDuration("driver-timeout")
        .requiring(interval => interval > Duration.Zero, "driver-timeout must be more than zero")
    }

    object Tcp {
      val config: Config = getConfig("tcp")
      val ConnectionTimeout: FiniteDuration = config
        .getMillisDuration("connection-timeout")
        .requiring(interval => interval > Duration.Zero, "connection-timeout must be more than zero")
      val OutboundClientHostname: Option[String] = {
        config.getString("outbound-client-hostname") match {
          case ""       => None
          case hostname => Some(hostname)
        }
      }
    }

  }
}

/** INTERNAL API */
private[akka] object ArterySettings {
  def apply(config: Config) = new ArterySettings(config)

  /** INTERNAL API */
  private[remote] final class Compression private[ArterySettings] (config: Config) {
    import config._

    private[akka] final val Enabled = ActorRefs.Enabled || Manifests.Enabled

    object ActorRefs {
      val config: Config = getConfig("actor-refs")
      import config._

      val AdvertisementInterval: FiniteDuration = config.getMillisDuration("advertisement-interval")
      val Max: Int = toRootLowerCase(getString("max")) match {
        case "off" => 0
        case _     => getInt("max")
      }
      final val Enabled = Max > 0
    }
    object Manifests {
      val config: Config = getConfig("manifests")
      import config._

      val AdvertisementInterval: FiniteDuration = config.getMillisDuration("advertisement-interval")
      val Max: Int = toRootLowerCase(getString("max")) match {
        case "off" => 0
        case _     => getInt("max")
      }
      final val Enabled = Max > 0
    }
  }
  object Compression {
    // Compile time constants
    final val Debug = false // unlocks additional very verbose debug logging of compression events (to stdout)
  }

  def getHostname(key: String, config: Config): String = config.getString(key) match {
    case "<getHostAddress>" => InetAddress.getLocalHost.getHostAddress
    case "<getHostName>"    => InetAddress.getLocalHost.getHostName
    case other =>
      if (other.startsWith("[") && other.endsWith("]")) other
      else if (AsyncDnsResolver.isIpv6Address(other)) {
        "[" + other + "]"
      } else other
  }

  sealed trait Transport {
    val configName: String
  }
  object AeronUpd extends Transport {
    override val configName: String = "aeron-udp"
    override def toString: String = configName
  }
  object Tcp extends Transport {
    override val configName: String = "tcp"
    override def toString: String = configName
  }
  object TlsTcp extends Transport {
    override val configName: String = "tls-tcp"
    override def toString: String = configName
  }
}
