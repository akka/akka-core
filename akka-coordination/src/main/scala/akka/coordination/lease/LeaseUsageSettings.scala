/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.coordination.lease

import scala.concurrent.duration.FiniteDuration

import akka.util.JavaDurationConverters._
import akka.util.PrettyDuration._

final class LeaseUsageSettings private[akka] (val leaseImplementation: String, val leaseRetryInterval: FiniteDuration) {
  def getLeaseRetryInterval(): java.time.Duration = leaseRetryInterval.asJava

  override def toString = s"LeaseUsageSettings($leaseImplementation, ${leaseRetryInterval.pretty})"
}
