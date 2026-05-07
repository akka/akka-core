/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.sbr

import scala.annotation.nowarn
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Cluster
import akka.cluster.DowningProvider
import akka.coordination.lease.scaladsl.LeaseProvider

/**
 * See reference documentation: https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html
 *
 * Enabled with configuration:
 * {{{
 * akka.cluster.downing-provider-class = "akka.cluster.sbr.SplitBrainResolverProvider"
 * }}}
 */
@nowarn("msg=Use Akka Distributed Cluster")
final class SplitBrainResolverProvider(system: ActorSystem) extends DowningProvider {

  private val settings = new SplitBrainResolverSettings(system.settings.config)

  override def downRemovalMargin: FiniteDuration = {
    // if down-removal-margin is defined we let it trump stable-after to allow
    // for two different values for SBR downing and cluster tool stop/start after downing
    val drm = Cluster(system).settings.DownRemovalMargin
    if (drm != Duration.Zero) drm
    else settings.DowningStableAfter
  }

  override def downingActorProps: Option[Props] = {
    import SplitBrainResolverSettings._

    val cluster = Cluster(system)
    val selfDc = cluster.selfDataCenter
    val strategy =
      settings.DowningStrategy match {
        case KeepMajorityName =>
          new KeepMajority(selfDc, settings.keepMajorityRole, cluster.selfUniqueAddress)
        case StaticQuorumName =>
          val s = settings.staticQuorumSettings
          new StaticQuorum(selfDc, s.size, s.role, cluster.selfUniqueAddress)
        case KeepOldestName =>
          val s = settings.keepOldestSettings
          new KeepOldest(selfDc, s.downIfAlone, s.role, cluster.selfUniqueAddress)
        case DownAllName =>
          new DownAllNodes(selfDc, cluster.selfUniqueAddress)
        case LeaseMajorityName =>
          val s = settings.leaseMajoritySettings
          val leaseOwnerName = cluster.selfUniqueAddress.address.hostPort
          val leaseName = s.safeLeaseName(system.name)
          val lease = LeaseProvider(system).getLease(leaseName, s.leaseImplementation, leaseOwnerName)
          new LeaseMajority(
            selfDc,
            s.role,
            lease,
            s.acquireLeaseDelayForMinority,
            s.releaseAfter,
            cluster.selfUniqueAddress)
      }

    Some(SplitBrainResolver.props(settings.DowningStableAfter, strategy))
  }

}
