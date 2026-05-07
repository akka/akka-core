/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import scala.annotation.nowarn

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@nowarn("msg=deprecated")
@InternalApi
class InetAddressDnsProvider extends DnsProvider {
  override def cache: Dns = new SimpleDnsCache()
  override def actorClass = classOf[InetAddressDnsResolver]
  override def managerClass = classOf[SimpleDnsManager]
}
