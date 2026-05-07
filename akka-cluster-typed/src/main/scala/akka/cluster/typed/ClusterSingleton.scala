/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.typed

import scala.annotation.nowarn
import scala.concurrent.duration.{ Duration, FiniteDuration, _ }
import scala.jdk.DurationConverters._

import com.typesafe.config.Config

import akka.actor.typed._
import akka.annotation.{ DoNotInherit, InternalApi }
import akka.cluster.ClusterSettings.DataCenter
import akka.cluster.singleton.{
  ClusterSingletonProxySettings,
  ClusterSingletonManagerSettings => ClassicClusterSingletonManagerSettings
}
import akka.cluster.typed.internal.AdaptedClusterSingletonImpl
import akka.coordination.lease.LeaseUsageSettings
import org.slf4j.LoggerFactory

@nowarn("msg=Use Akka Distributed Cluster")
object ClusterSingletonSettings {
  def apply(system: ActorSystem[_]): ClusterSingletonSettings =
    fromConfig(system.settings.config.getConfig("akka.cluster"))

  /**
   * Java API
   */
  def create(system: ActorSystem[_]): ClusterSingletonSettings = apply(system)

  def fromConfig(config: Config): ClusterSingletonSettings = {
    // TODO introduce a config namespace for typed singleton and read that?
    // currently singleton name is required and then discarded, for example
    val mgrSettings = ClusterSingletonManagerSettings(config.getConfig("singleton"))
    val proxySettings = ClusterSingletonProxySettings(config.getConfig("singleton-proxy"))
    new ClusterSingletonSettings(
      mgrSettings.role,
      proxySettings.dataCenter,
      proxySettings.singletonIdentificationInterval,
      mgrSettings.removalMargin,
      mgrSettings.handOverRetryInterval,
      proxySettings.bufferSize,
      mgrSettings.leaseSettings)
  }
}

@nowarn("msg=Use Akka Distributed Cluster")
final class ClusterSingletonSettings(
    val role: Option[String],
    @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
    val dataCenter: Option[DataCenter],
    val singletonIdentificationInterval: FiniteDuration,
    val removalMargin: FiniteDuration,
    val handOverRetryInterval: FiniteDuration,
    val bufferSize: Int,
    val leaseSettings: Option[LeaseUsageSettings]) {

  // bin compat for 2.6.14
  @deprecated("Use constructor with leaseSettings", "2.6.15")
  def this(
      role: Option[String],
      dataCenter: Option[DataCenter],
      singletonIdentificationInterval: FiniteDuration,
      removalMargin: FiniteDuration,
      handOverRetryInterval: FiniteDuration,
      bufferSize: Int) =
    this(role, dataCenter, singletonIdentificationInterval, removalMargin, handOverRetryInterval, bufferSize, None)

  def withRole(role: String): ClusterSingletonSettings = copy(role = Some(role))

  def withNoRole(): ClusterSingletonSettings = copy(role = None)

  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def withDataCenter(dataCenter: DataCenter): ClusterSingletonSettings = copy(dataCenter = Some(dataCenter))

  @deprecated("Use Akka Distributed Cluster instead", "2.10.0")
  def withNoDataCenter(): ClusterSingletonSettings = copy(dataCenter = None)

  def withRemovalMargin(removalMargin: FiniteDuration): ClusterSingletonSettings = copy(removalMargin = removalMargin)

  def withRemovalMargin(removalMargin: java.time.Duration): ClusterSingletonSettings =
    withRemovalMargin(removalMargin.toScala)

  def withHandoverRetryInterval(handOverRetryInterval: FiniteDuration): ClusterSingletonSettings =
    copy(handOverRetryInterval = handOverRetryInterval)

  def withHandoverRetryInterval(handOverRetryInterval: java.time.Duration): ClusterSingletonSettings =
    withHandoverRetryInterval(handOverRetryInterval.toScala)

  def withBufferSize(bufferSize: Int): ClusterSingletonSettings = copy(bufferSize = bufferSize)

  /**
   * Note that if you define a custom lease name and have several singletons each one must have a unique
   * lease name. If the lease name is undefined it will be derived from ActorSystem name and singleton
   * actor path, but that may result in too long lease names.
   */
  def withLeaseSettings(leaseSettings: LeaseUsageSettings) = copy(leaseSettings = Option(leaseSettings))

  private def copy(
      role: Option[String] = role,
      dataCenter: Option[DataCenter] = dataCenter,
      singletonIdentificationInterval: FiniteDuration = singletonIdentificationInterval,
      removalMargin: FiniteDuration = removalMargin,
      handOverRetryInterval: FiniteDuration = handOverRetryInterval,
      bufferSize: Int = bufferSize,
      leaseSettings: Option[LeaseUsageSettings] = leaseSettings) =
    new ClusterSingletonSettings(
      role,
      dataCenter,
      singletonIdentificationInterval,
      removalMargin,
      handOverRetryInterval,
      bufferSize,
      leaseSettings)

  /**
   * INTERNAL API:
   */
  @InternalApi
  private[akka] def toManagerSettings(singletonName: String): ClassicClusterSingletonManagerSettings =
    new ClassicClusterSingletonManagerSettings(singletonName, role, removalMargin, handOverRetryInterval, leaseSettings)

  /**
   * INTERNAL API:
   */
  @InternalApi
  private[akka] def toProxySettings(singletonName: String): ClusterSingletonProxySettings = {
    new ClusterSingletonProxySettings(singletonName, role, singletonIdentificationInterval, bufferSize)
      .withDataCenter(dataCenter)
  }

  /**
   * INTERNAL API:
   */
  @InternalApi
  private[akka] def shouldRunManager(cluster: Cluster): Boolean = {
    (role.isEmpty || cluster.selfMember.roles(role.get)) &&
    (dataCenter.isEmpty || dataCenter.contains(cluster.selfMember.dataCenter))
  }

  override def toString =
    s"ClusterSingletonSettings($role, $dataCenter, $singletonIdentificationInterval, $removalMargin, $handOverRetryInterval, $bufferSize, $leaseSettings)"
}

object ClusterSingleton extends ExtensionId[ClusterSingleton] {

  override def createExtension(system: ActorSystem[_]): ClusterSingleton = new AdaptedClusterSingletonImpl(system)

  /**
   * Java API:
   */
  def get(system: ActorSystem[_]): ClusterSingleton = apply(system)
}

/**
 * INTERNAL API:
 */
@InternalApi
private[akka] object ClusterSingletonImpl {
  def managerNameFor(singletonName: String) = s"singletonManager$singletonName"
}

object SingletonActor {

  /**
   * @param name Unique name for the singleton
   * @param behavior Behavior for the singleton
   */
  def apply[M](behavior: Behavior[M], name: String): SingletonActor[M] =
    new SingletonActor[M](behavior, name, Props.empty, None, None)

  /**
   * Java API
   *
   * @param name Unique name for the singleton
   * @param behavior Behavior for the singleton
   */
  def of[M](behavior: Behavior[M], name: String): SingletonActor[M] = apply(behavior, name)
}

final class SingletonActor[M] private (
    val behavior: Behavior[M],
    val name: String,
    val props: Props,
    val stopMessage: Option[M],
    val settings: Option[ClusterSingletonSettings]) {

  /**
   * [[akka.actor.typed.Props]] of the singleton actor, such as dispatcher settings.
   */
  def withProps(props: Props): SingletonActor[M] = copy(props = props)

  /**
   * Message sent to the singleton to tell it to stop, e.g. when being migrated.
   * If this is not defined it will be stopped automatically.
   * It can be useful to define a custom stop message if the singleton needs to perform
   * some asynchronous cleanup or interactions before stopping.
   */
  def withStopMessage(msg: M): SingletonActor[M] = copy(stopMessage = Option(msg))

  /**
   * Additional settings, typically loaded from configuration.
   */
  def withSettings(settings: ClusterSingletonSettings): SingletonActor[M] = copy(settings = Option(settings))

  private def copy(
      behavior: Behavior[M] = behavior,
      props: Props = props,
      stopMessage: Option[M] = stopMessage,
      settings: Option[ClusterSingletonSettings] = settings): SingletonActor[M] =
    new SingletonActor[M](behavior, name, props, stopMessage, settings)
}

/**
 * This class is not intended for user extension other than for test purposes (e.g.
 * stub implementation). More methods may be added in the future and that may break
 * such implementations.
 */
@DoNotInherit
abstract class ClusterSingleton extends Extension {

  /**
   * Start if needed and provide a proxy to a named singleton
   *
   * If there already is a manager running for the given `singletonName` on this node, no additional manager is started.
   * If there already is a proxy running for the given `singletonName` on this node, an [[ActorRef]] to that is returned.
   *
   * @return A proxy actor that can be used to communicate with the singleton in the cluster
   */
  def init[M](singleton: SingletonActor[M]): ActorRef[M]
}

object ClusterSingletonManagerSettings {

  /**
   * Create settings from the default configuration
   * `akka.cluster.singleton`.
   */
  def apply(system: ActorSystem[_]): ClusterSingletonManagerSettings =
    apply(system.settings.config.getConfig("akka.cluster.singleton"))
      .withRemovalMargin(akka.cluster.Cluster(system).downingProvider.downRemovalMargin)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.singleton`.
   */
  def apply(config: Config): ClusterSingletonManagerSettings = {

    val lease: Option[LeaseUsageSettings] =
      if (config.hasPath("use-lease") && config.getString("use-lease").nonEmpty) {
        val settings = LeaseUsageSettings(config)
        // intentionally not in config because would be high risk of not using unique names
        if (settings.leaseName != "") {
          LoggerFactory
            .getLogger(classOf[ClusterSingletonManagerSettings])
            .warn(
              "Ignoring singleton lease-name [{}] from configuration, lease names must be programmatically configured through 'LeaseUsageSettings'",
              settings.leaseName)
          Some(settings.withLeaseName(""))
        } else Some(settings)
      } else None

    new ClusterSingletonManagerSettings(
      singletonName = config.getString("singleton-name"),
      role = roleOption(config.getString("role")),
      removalMargin = Duration.Zero, // defaults to ClusterSettings.DownRemovalMargin
      handOverRetryInterval = config.getDuration("hand-over-retry-interval", MILLISECONDS).millis,
      lease)
  }

  /**
   * Java API: Create settings from the default configuration
   * `akka.cluster.singleton`.
   */
  def create(system: ActorSystem[_]): ClusterSingletonManagerSettings = apply(system)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * the default configuration `akka.cluster.singleton`.
   */
  def create(config: Config): ClusterSingletonManagerSettings = apply(config)

  /**
   * INTERNAL API
   */
  private[akka] def roleOption(role: String): Option[String] =
    if (role == "") None else Option(role)

}

/**
 * @param singletonName         The actor name of the child singleton actor.
 * @param role                  Singleton among the nodes tagged with specified role.
 *                              If the role is not specified it's a singleton among all nodes in
 *                              the cluster.
 * @param removalMargin         Margin until the singleton instance that belonged to
 *                              a downed/removed partition is created in surviving partition. The purpose of
 *                              this margin is that in case of a network partition the singleton actors
 *                              in the non-surviving partitions must be stopped before corresponding actors
 *                              are started somewhere else. This is especially important for persistent
 *                              actors.
 * @param handOverRetryInterval When a node is becoming oldest it sends hand-over
 *                              request to previous oldest, that might be leaving the cluster. This is
 *                              retried with this interval until the previous oldest confirms that the hand
 *                              over has started or the previous oldest member is removed from the cluster
 *                              (+ `removalMargin`).
 * @param leaseSettings         LeaseSettings for acquiring before creating the singleton actor.
 *                              Note that if you define a custom lease name and have several singletons each
 *                              one must have a unique lease name. If the lease name is undefined it will be
 *                              derived from ActorSystem name and singleton actor path, but that may result in
 *                              too long lease names.
 */
final class ClusterSingletonManagerSettings(
    val singletonName: String,
    val role: Option[String],
    val removalMargin: FiniteDuration,
    val handOverRetryInterval: FiniteDuration,
    val leaseSettings: Option[LeaseUsageSettings]) {

  // bin compat for 2.6.14
  @deprecated("Use constructor with leaseSettings", "2.6.15")
  def this(
      singletonName: String,
      role: Option[String],
      removalMargin: FiniteDuration,
      handOverRetryInterval: FiniteDuration) =
    this(singletonName, role, removalMargin, handOverRetryInterval, None)

  def withSingletonName(name: String): ClusterSingletonManagerSettings = copy(singletonName = name)

  def withRole(role: String): ClusterSingletonManagerSettings =
    copy(role = ClassicClusterSingletonManagerSettings.roleOption(role))

  def withRole(role: Option[String]): ClusterSingletonManagerSettings = copy(role = role)

  def withRemovalMargin(removalMargin: FiniteDuration): ClusterSingletonManagerSettings =
    copy(removalMargin = removalMargin)

  def withRemovalMargin(removalMargin: java.time.Duration): ClusterSingletonManagerSettings =
    withRemovalMargin(removalMargin.toScala)

  def withHandOverRetryInterval(retryInterval: FiniteDuration): ClusterSingletonManagerSettings =
    copy(handOverRetryInterval = retryInterval)

  def withHandOverRetryInterval(retryInterval: java.time.Duration): ClusterSingletonManagerSettings =
    withHandOverRetryInterval(retryInterval.toScala)

  /**
   * Note that if you define a custom lease name and have several singletons each one must have a unique
   * lease name. If the lease name is undefined it will be derived from ActorSystem name and singleton
   * actor path, but that may result in too long lease names.
   */
  def withLeaseSettings(leaseSettings: LeaseUsageSettings) = copy(leaseSettings = Option(leaseSettings))

  private def copy(
      singletonName: String = singletonName,
      role: Option[String] = role,
      removalMargin: FiniteDuration = removalMargin,
      handOverRetryInterval: FiniteDuration = handOverRetryInterval,
      leaseSettings: Option[LeaseUsageSettings] = leaseSettings): ClusterSingletonManagerSettings =
    new ClusterSingletonManagerSettings(singletonName, role, removalMargin, handOverRetryInterval, leaseSettings)
}

object ClusterSingletonSetup {
  def apply[T <: Extension](createExtension: ActorSystem[_] => ClusterSingleton): ClusterSingletonSetup =
    new ClusterSingletonSetup(createExtension(_))

}

/**
 * Can be used in [[akka.actor.setup.ActorSystemSetup]] when starting the [[ActorSystem]]
 * to replace the default implementation of the [[ClusterSingleton]] extension. Intended
 * for tests that need to replace extension with stub/mock implementations.
 */
final class ClusterSingletonSetup(createExtension: java.util.function.Function[ActorSystem[_], ClusterSingleton])
    extends ExtensionSetup[ClusterSingleton](ClusterSingleton, createExtension)
