/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.io

import akka.actor.Props
import akka.io.UdpConnected.Connect

/**
 * INTERNAL API
 */
private[io] class UdpConnectedManager(udpConn: UdpConnectedExt)
    extends SelectionHandler.SelectorBasedManager(udpConn.settings, udpConn.settings.NrOfSelectors) {

  def receive = workerForCommandHandler {
    case c: Connect =>
      val commander = sender() // cache because we create a function that will run asynchly
      registry => Props(classOf[UdpConnection], udpConn, registry, commander, c)
  }

}
