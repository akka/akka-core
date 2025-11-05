/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.actor.Address
import akka.remote.UniqueAddress
import akka.remote.artery.OutboundHandshake.HandshakeReq
import akka.remote.artery.OutboundHandshake.HandshakeRsp
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe
import akka.util.OptionVal

object InboundHandshakeSpec {
  case object Control1 extends ControlMessage
  case object Control2 extends ControlMessage
  case object Control3 extends ControlMessage
}

class InboundHandshakeSpec extends AkkaSpec("""
    akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {

  val addressA = UniqueAddress(Address("akka", "sysA", "hostA", 1001), 1)
  val addressB = UniqueAddress(Address("akka", "sysB", "hostB", 1002), 2)

  private def setupStream(inboundContext: InboundContext): (TestPublisher.Probe[AnyRef], TestSubscriber.Probe[Any]) = {
    val recipient = OptionVal.None // not used
    TestSource[AnyRef]()
      .map(msg =>
        InboundEnvelope(recipient, msg, OptionVal.None, addressA.uid, inboundContext.association(addressA.uid)))
      .via(new InboundHandshake(inboundContext, inControlStream = true))
      .map { case env: InboundEnvelope => env.message }
      .toMat(TestSink[Any]())(Keep.both)
      .run()
  }

  private def futureUniqueRemoteAddress(association: OutboundContext): Future[UniqueAddress] = {
    val p = Promise[UniqueAddress]()
    association.associationState.addUniqueRemoteAddressListener(a => p.success(a))
    p.future
  }

  "InboundHandshake stage" must {

    "send HandshakeRsp as reply to HandshakeReq" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressB, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(HandshakeReq(addressA, addressB.address))
      upstream.sendNext("msg1")
      replyProbe.expectMsg(HandshakeRsp(addressB))
      downstream.expectNext("msg1")
      downstream.cancel()
    }

    "complete remoteUniqueAddress when receiving HandshakeReq" in {
      val inboundContext = new TestInboundContext(addressB)
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(HandshakeReq(addressA, addressB.address))
      upstream.sendNext("msg1")
      downstream.expectNext("msg1")
      val uniqueRemoteAddress =
        Await.result(futureUniqueRemoteAddress(inboundContext.association(addressA.address)), remainingOrDefault)
      uniqueRemoteAddress should ===(addressA)
      downstream.cancel()
    }

    "drop message from unknown (receiving system restarted)" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressB, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      // no HandshakeReq
      upstream.sendNext("msg17")
      downstream.expectNoMessage(200.millis) // messages from unknown are dropped

      // and accept messages after handshake
      upstream.sendNext(HandshakeReq(addressA, addressB.address))
      upstream.sendNext("msg18")
      replyProbe.expectMsg(HandshakeRsp(addressB))
      downstream.expectNext("msg18")
      upstream.sendNext("msg19")
      downstream.expectNext("msg19")

      downstream.cancel()
    }

  }

}
