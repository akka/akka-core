/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.metrics.sample

import akka.serialization.jackson.CborSerializable

//#messages
final case class StatsJob(text: String) extends CborSerializable
final case class StatsResult(meanWordLength: Double) extends CborSerializable
final case class JobFailed(reason: String) extends CborSerializable
//#messages
