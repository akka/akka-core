/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.annotation.nowarn

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.ConfigurationException
import akka.actor.ActorPath
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.DynamicAccess
import akka.actor.NoScopeGiven
import akka.actor.Scope
import akka.annotation.InternalApi
import akka.cluster.routing.ClusterRouterGroup
import akka.cluster.routing.ClusterRouterGroupSettings
import akka.cluster.routing.ClusterRouterPool
import akka.cluster.routing.ClusterRouterPoolSettings
import akka.event.EventStream
import akka.remote.RemoteActorRefProvider
import akka.remote.RemoteDeployer
import akka.remote.routing.RemoteRouterConfig
import akka.routing.Group
import akka.routing.Pool

/**
 * INTERNAL API
 *
 * The `ClusterActorRefProvider` will load the [[akka.cluster.Cluster]]
 * extension, i.e. the cluster will automatically be started when
 * the `ClusterActorRefProvider` is used.
 */
@InternalApi
private[akka] class ClusterActorRefProvider(
    _systemName: String,
    _settings: ActorSystem.Settings,
    _eventStream: EventStream,
    _dynamicAccess: DynamicAccess)
    extends RemoteActorRefProvider(_systemName, _settings, _eventStream, _dynamicAccess) {

  override def init(system: ActorSystemImpl): Unit = {
    super.init(system)

    // initialize/load the Cluster extension
    Cluster(system)
  }

  override protected def warnIfDirectUse(): Unit = ()

  override protected def createRemoteWatcher(system: ActorSystemImpl): ActorRef = {
    // make sure Cluster extension is initialized/loaded from init thread
    Cluster(system)
    system.systemActorOf(
      ClusterRemoteWatcher.props(createRemoteWatcherFailureDetector(system), remoteSettings),
      "remote-watcher")
  }

  /**
   * Factory method to make it possible to override deployer in subclass
   * Creates a new instance every time
   */
  override protected def createDeployer: ClusterDeployer = new ClusterDeployer(settings, dynamicAccess)

  override protected def shouldCreateRemoteActorRef(system: ActorSystem, address: Address): Boolean =
    Cluster(system).state.members.exists(_.address == address) && super.shouldCreateRemoteActorRef(system, address)

  override protected def warnIfNotRemoteActorRef(path: ActorPath): Unit =
    warnOnUnsafe(s"Remote deploy of [$path] outside this cluster is not allowed, falling back to local.")
}

/**
 * INTERNAL API
 *
 * Deployer of cluster aware routers.
 */
@InternalApi
private[akka] class ClusterDeployer(_settings: ActorSystem.Settings, _pm: DynamicAccess)
    extends RemoteDeployer(_settings, _pm) {

  override def parseConfig(path: String, config: Config): Option[Deploy] = {
    // config is the user supplied section, no defaults
    // amend it to use max-total-nr-of-instances as nr-of-instances if cluster.enabled and
    // user has not specified nr-of-instances
    val config2 =
      if (config.hasPath("cluster.enabled") && config.getBoolean("cluster.enabled") && !config.hasPath(
            "nr-of-instances")) {
        val maxTotalNrOfInstances = config.withFallback(default).getInt("cluster.max-total-nr-of-instances")
        ConfigFactory.parseString("nr-of-instances=" + maxTotalNrOfInstances).withFallback(config)
      } else config

    super.parseConfig(path, config2) match {
      case d @ Some(deploy) =>
        if (deploy.config.getBoolean("cluster.enabled")) {
          if (deploy.scope != NoScopeGiven)
            throw new ConfigurationException(
              "Cluster deployment can't be combined with scope [%s]".format(deploy.scope))
          if (deploy.routerConfig.isInstanceOf[RemoteRouterConfig])
            throw new ConfigurationException(
              "Cluster deployment can't be combined with [%s]".format(deploy.routerConfig))

          deploy.routerConfig match {
            case r: Pool =>
              Some(
                deploy.copy(
                  routerConfig = ClusterRouterPool(r, ClusterRouterPoolSettings.fromConfig(deploy.config)),
                  scope = ClusterScope))
            case r: Group =>
              Some(
                deploy.copy(
                  routerConfig = ClusterRouterGroup(r, ClusterRouterGroupSettings.fromConfig(deploy.config)),
                  scope = ClusterScope))
            case other =>
              throw new IllegalArgumentException(
                s"Cluster aware router can only wrap Pool or Group, got [${other.getClass.getName}]")
          }
        } else d
      case None => None
    }
  }

}

@nowarn("msg=@SerialVersionUID has no effect")
@SerialVersionUID(1L)
abstract class ClusterScope extends Scope

/**
 * Cluster aware scope of a [[akka.actor.Deploy]]
 */
case object ClusterScope extends ClusterScope {

  /**
   * Java API: get the singleton instance
   */
  def getInstance = this

  def withFallback(other: Scope): Scope = this
}
