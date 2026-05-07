/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote

import com.typesafe.config._

import akka.ConfigurationException
import akka.actor._
import akka.japi.Util.immutableSeq
import akka.remote.routing.RemoteRouterConfig
import akka.routing._
import akka.routing.Pool

@SerialVersionUID(1L)
final case class RemoteScope(node: Address) extends Scope {
  def withFallback(other: Scope): Scope = this
}

/**
 * INTERNAL API
 */
private[akka] class RemoteDeployer(_settings: ActorSystem.Settings, _pm: DynamicAccess)
    extends Deployer(_settings, _pm) {
  override def parseConfig(path: String, config: Config): Option[Deploy] = {

    super.parseConfig(path, config) match {
      case d @ Some(deploy) =>
        deploy.config.getString("remote") match {
          case AddressFromURIString(r) => Some(deploy.copy(scope = RemoteScope(r)))
          case str if !str.isEmpty     => throw new ConfigurationException(s"unparseable remote node name [${str}]")
          case _ =>
            val nodes = immutableSeq(deploy.config.getStringList("target.nodes")).map(AddressFromURIString(_))
            if (nodes.isEmpty || deploy.routerConfig == NoRouter) d
            else
              deploy.routerConfig match {
                case r: Pool => Some(deploy.copy(routerConfig = RemoteRouterConfig(r, nodes)))
                case _       => d
              }
        }
      case None => None
    }
  }
}
