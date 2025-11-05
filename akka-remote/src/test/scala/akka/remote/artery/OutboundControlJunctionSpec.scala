/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.Address
import akka.remote.UniqueAddress
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.util.OptionVal

object OutboundControlJunctionSpec {
  case object Control1 extends ControlMessage
  case object Control2 extends ControlMessage
  case object Control3 extends ControlMessage
}

class OutboundControlJunctionSpec extends AkkaSpec("""
    akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {
  import OutboundControlJunctionSpec._

  val addressA = UniqueAddress(Address("akka", "sysA", "hostA", 1001), 1)
  val addressB = UniqueAddress(Address("akka", "sysB", "hostB", 1002), 2)

  private val outboundEnvelopePool = ReusableOutboundEnvelope.createObjectPool(capacity = 16)

  "Control messages" must {

    "be injected via side channel" in {
      val inboundContext = new TestInboundContext(localAddress = addressA)
      val outboundContext = inboundContext.association(addressB.address)

      val ((upstream, controlIngress), downstream) = TestSource[String]()
        .map(msg => outboundEnvelopePool.acquire().init(OptionVal.None, msg, OptionVal.None))
        .viaMat(new OutboundControlJunction(outboundContext, outboundEnvelopePool))(Keep.both)
        .map(env => env.message)
        .toMat(TestSink[Any]())(Keep.both)
        .run()

      controlIngress.sendControlMessage(Control1)
      downstream.request(1)
      downstream.expectNext(Control1)
      upstream.sendNext("msg1")
      downstream.request(1)
      downstream.expectNext("msg1")
      upstream.sendNext("msg2")
      downstream.request(1)
      downstream.expectNext("msg2")
      controlIngress.sendControlMessage(Control2)
      upstream.sendNext("msg3")
      downstream.request(10)
      downstream.expectNextUnorderedN(List("msg3", Control2))
      controlIngress.sendControlMessage(Control3)
      downstream.expectNext(Control3)
      downstream.cancel()
    }

  }

}
