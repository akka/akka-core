/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ClusterLogClass {

  val ClusterCore: Class[Cluster] = classOf[Cluster]
  val ClusterHeartbeat: Class[ClusterHeartbeat] = classOf[ClusterHeartbeat]
  val ClusterGossip: Class[ClusterGossip] = classOf[ClusterGossip]

}

/**
 * INTERNAL API: Logger class for (verbose) heartbeat logging.
 */
@InternalApi private[akka] class ClusterHeartbeat

/**
 * INTERNAL API: Logger class for (verbose) gossip logging.
 */
@InternalApi private[akka] class ClusterGossip
