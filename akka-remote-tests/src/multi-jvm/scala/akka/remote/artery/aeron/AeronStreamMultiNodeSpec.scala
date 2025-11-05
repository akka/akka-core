/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.aeron

import java.io.File
import java.util.UUID

import io.aeron.CommonContext
import io.aeron.driver.MediaDriver

import akka.remote.artery.UdpPortActor
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{ MultiNodeConfig, MultiNodeSpec }

abstract class AeronStreamMultiNodeSpec(config: MultiNodeConfig) extends MultiNodeSpec(config) {

  def startDriver(): MediaDriver = {
    val driverContext = new MediaDriver.Context
    // create a random name but include the actor system name for easier debugging
    val uniquePart = UUID.randomUUID().toString
    val randomName = s"${CommonContext.getAeronDirectoryName}${File.separator}${system.name}-$uniquePart"
    driverContext.aeronDirectoryName(randomName)
    val d = MediaDriver.launchEmbedded(driverContext)
    log.info("Started embedded media driver in directory [{}]", d.aeronDirectoryName)
    d
  }

  def channel(roleName: RoleName) = {
    val n = node(roleName)
    val port = MultiNodeSpec.udpPort match {
      case None =>
        system.actorSelection(n / "user" / "updPort") ! UdpPortActor.GetUdpPort
        expectMsgType[Int]
      case Some(p) => p
    }
    s"aeron:udp?endpoint=${n.address.host.get}:$port"
  }
}
