/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io.dns.internal

import scala.annotation.nowarn

import akka.annotation.InternalApi
import akka.io._

/**
 * INTERNAL API
 */
@InternalApi
@nowarn("msg=deprecated")
private[akka] class AsyncDnsProvider extends DnsProvider {
  override def cache: Dns = new SimpleDnsCache()
  override def actorClass = classOf[AsyncDnsResolver]
  override def managerClass = classOf[AsyncDnsManager]
}
