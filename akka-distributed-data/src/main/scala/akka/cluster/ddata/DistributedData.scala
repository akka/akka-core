/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.cluster.{ Cluster, UniqueAddress }
import akka.event.Logging

object DistributedData extends ExtensionId[DistributedData] with ExtensionIdProvider {
  override def get(system: ActorSystem): DistributedData = super.get(system)
  override def get(system: ClassicActorSystemProvider): DistributedData = super.get(system)

  override def lookup = DistributedData

  override def createExtension(system: ExtendedActorSystem): DistributedData =
    new DistributedData(system)
}

/**
 * Akka extension for convenient configuration and use of the
 * [[Replicator]]. Configuration settings are defined in the
 * `akka.cluster.ddata` section, see `reference.conf`.
 */
class DistributedData(system: ExtendedActorSystem) extends Extension {

  private val settings = ReplicatorSettings(system)

  implicit val selfUniqueAddress: SelfUniqueAddress = SelfUniqueAddress(Cluster(system).selfUniqueAddress)

  /**
   * `ActorRef` of the [[Replicator]] .
   */
  val replicator: ActorRef =
    if (isTerminated) {
      val log = Logging(system, classOf[DistributedData])
      if (Cluster(system).isTerminated)
        log.warning("Replicator points to dead letters, because Cluster is terminated.")
      else
        log.warning(
          "Replicator points to dead letters. Make sure the cluster node has the proper role. " +
          "Node has roles [{}], Distributed Data is configured for roles [{}].",
          Cluster(system).selfRoles.mkString(","),
          settings.roles.mkString(","): Any)
      system.deadLetters
    } else {
      system.systemActorOf(Replicator.props(settings), ReplicatorSettings.name(system, None))
    }

  /**
   * Returns true if this member is not tagged with the role configured for the
   * replicas.
   */
  def isTerminated: Boolean =
    Cluster(system).isTerminated || !settings.roles.subsetOf(Cluster(system).selfRoles)

}

/**
 * Cluster non-specific (typed vs classic) wrapper for [[akka.cluster.UniqueAddress]].
 */
@SerialVersionUID(1L)
final case class SelfUniqueAddress(uniqueAddress: UniqueAddress)
