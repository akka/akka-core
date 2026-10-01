/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.io

import scala.concurrent.duration._

import akka.actor.DeadLetter
import akka.io.Tcp._
import akka.testkit.{ AkkaSpec, TestProbe }
import akka.testkit.WithLogCapturing
import akka.util.ByteString

/**
 * Verifies that a TCP Write racing with a peer-initiated connection close does NOT
 * produce dead letters — the scenario observed when HAProxy runs SSL health checks in
 * TCP mode: HAProxy opens a TLS connection, completes the handshake, then immediately
 * closes the socket.  Any in-flight Write from the server side should be silently
 * dropped (e.g. via DeadLetterSuppression) rather than showing up as a dead letter.
 */
class TcpDeadLetterOnClosedConnectionSpec extends AkkaSpec("""
    akka.loglevel = DEBUG
    akka.loggers = ["akka.testkit.SilenceAllTestEventListener"]
    akka.io.tcp.trace-logging = on
    akka.log-dead-letters = on
    """) with TcpIntegrationSpecSupport with WithLogCapturing {

  "Tcp" should {

    "not produce a dead letter when Write is sent to a connection actor after the peer closed it" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()

      // Subscribe to dead letters before triggering the race
      val deadLetterProbe = TestProbe()
      system.eventStream.subscribe(deadLetterProbe.ref, classOf[DeadLetter])
      watch(serverConnection)

      // Simulate HAProxy closing the connection abruptly after the health-check handshake
      clientHandler.send(clientConnection, Abort)
      clientHandler.expectMsg(Aborted)

      // Server side sees the abrupt close
      serverHandler.expectMsgType[ErrorClosed]

      // Immediately send a Write without waiting for the actor to fully terminate —
      // this mirrors the Akka HTTP stream pipeline sending a response while the
      // connection teardown is in progress. The actor must absorb this with
      // CommandFailed rather than letting it become a dead letter.
      serverHandler.send(serverConnection, Write(ByteString("health-check-response")))

      deadLetterProbe.expectNoMessage(500.millis)

      // The connection actor must eventually terminate
      expectTerminated(serverConnection, 5.seconds)
    }
  }
}
