/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.Address
import akka.remote.UniqueAddress
import akka.remote.artery.SystemMessageDelivery._
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.testkit.TestProbe
import akka.util.OptionVal

class SystemMessageAckerSpec extends AkkaSpec("""
    akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {

  val addressA = UniqueAddress(Address("akka", "sysA", "hostA", 1001), 1)
  val addressB = UniqueAddress(Address("akka", "sysB", "hostB", 1002), 2)
  val addressC = UniqueAddress(Address("akka", "sysC", "hostB", 1003), 3)

  private def setupStream(inboundContext: InboundContext): (TestPublisher.Probe[AnyRef], TestSubscriber.Probe[Any]) = {
    val recipient = OptionVal.None // not used
    TestSource[AnyRef]()
      .map {
        case sysMsg @ SystemMessageEnvelope(_, _, ackReplyTo) =>
          InboundEnvelope(recipient, sysMsg, OptionVal.None, ackReplyTo.uid, inboundContext.association(ackReplyTo.uid))
        case _ => throw new RuntimeException()
      }
      .via(new SystemMessageAcker(inboundContext))
      .map { case env: InboundEnvelope => env.message }
      .toMat(TestSink[Any]())(Keep.both)
      .run()
  }

  "SystemMessageAcker stage" must {

    "send Ack for expected message" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressA, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(SystemMessageEnvelope("b1", 1, addressB))
      replyProbe.expectMsg(Ack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("b2", 2, addressB))
      replyProbe.expectMsg(Ack(2, addressA))
      downstream.cancel()
    }

    "send Ack for duplicate message" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressA, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(SystemMessageEnvelope("b1", 1, addressB))
      replyProbe.expectMsg(Ack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("b2", 2, addressB))
      replyProbe.expectMsg(Ack(2, addressA))
      upstream.sendNext(SystemMessageEnvelope("b1", 1, addressB))
      replyProbe.expectMsg(Ack(2, addressA))
      downstream.cancel()
    }

    "send Nack for unexpected message" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressA, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(SystemMessageEnvelope("b1", 1, addressB))
      replyProbe.expectMsg(Ack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("b3", 3, addressB))
      replyProbe.expectMsg(Nack(1, addressA))
      downstream.cancel()
    }

    "send Nack for unexpected first message" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressA, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(SystemMessageEnvelope("b2", 2, addressB))
      replyProbe.expectMsg(Nack(0, addressA))
      downstream.cancel()
    }

    "keep track of sequence numbers per sending system" in {
      val replyProbe = TestProbe()
      val inboundContext = new TestInboundContext(addressA, controlProbe = Some(replyProbe.ref))
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(SystemMessageEnvelope("b1", 1, addressB))
      replyProbe.expectMsg(Ack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("b2", 2, addressB))
      replyProbe.expectMsg(Ack(2, addressA))

      upstream.sendNext(SystemMessageEnvelope("c1", 1, addressC))
      replyProbe.expectMsg(Ack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("c3", 3, addressC))
      replyProbe.expectMsg(Nack(1, addressA))
      upstream.sendNext(SystemMessageEnvelope("c2", 2, addressC))
      replyProbe.expectMsg(Ack(2, addressA))
      upstream.sendNext(SystemMessageEnvelope("c3", 3, addressC))
      replyProbe.expectMsg(Ack(3, addressA))
      upstream.sendNext(SystemMessageEnvelope("c4", 4, addressC))
      replyProbe.expectMsg(Ack(4, addressA))

      upstream.sendNext(SystemMessageEnvelope("b4", 4, addressB))
      replyProbe.expectMsg(Nack(2, addressA))
      upstream.sendNext(SystemMessageEnvelope("b3", 3, addressB))
      replyProbe.expectMsg(Ack(3, addressA))

      downstream.cancel()
    }

  }

}
