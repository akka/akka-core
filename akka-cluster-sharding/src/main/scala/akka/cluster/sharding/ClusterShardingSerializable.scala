/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://akka.io>
 */

package akka.cluster.sharding

/**
 * Marker trait for remote messages and persistent events/snapshots with special serializer.
 */
trait ClusterShardingSerializable extends Serializable
