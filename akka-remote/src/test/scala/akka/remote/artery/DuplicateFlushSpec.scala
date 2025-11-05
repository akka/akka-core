/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.Address
import akka.actor.ExtendedActorSystem
import akka.remote.UniqueAddress
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import akka.util.OptionVal

class DuplicateFlushSpec extends AkkaSpec("""
      akka.stream.materializer.debug.fuzzing-mode = on
  """) with ImplicitSender {

  private val pool = new EnvelopeBufferPool(1034 * 1024, 128)
  private val serialization = SerializationExtension(system)

  private val addressA = UniqueAddress(Address("akka", "sysA", "hostA", 1001), 1)
  private val addressB = UniqueAddress(Address("akka", "sysB", "hostB", 1002), 2)

  private def setupStream(inboundContext: InboundContext): (TestPublisher.Probe[AnyRef], TestSubscriber.Probe[Any]) = {
    TestSource[AnyRef]()
      .map { msg =>
        val association = inboundContext.association(addressA.uid)
        val ser = serialization.serializerFor(msg.getClass)
        val serializerId = ser.identifier
        val manifest = ser match {
          case s: SerializerWithStringManifest => s.manifest(msg)
          case _                               => ""
        }

        val env = new ReusableInboundEnvelope
        env
          .init(
            recipient = OptionVal.None,
            sender = OptionVal.None,
            originUid = addressA.uid,
            serializerId,
            manifest,
            flags = 0,
            envelopeBuffer = null,
            association,
            lane = 0)
          .withMessage(msg)
        env
      }
      .via(new DuplicateFlush(numberOfLanes = 3, system.asInstanceOf[ExtendedActorSystem], pool))
      .map(env => env.message -> env.lane)
      .toMat(TestSink[Any]())(Keep.both)
      .run()
  }

  "DuplicateFlush stage" must {

    "duplicate Flush messages" in {
      val inboundContext = new TestInboundContext(addressB, controlProbe = None)
      val (upstream, downstream) = setupStream(inboundContext)

      downstream.request(10)
      upstream.sendNext(Flush)
      upstream.sendNext("msg1")
      downstream.expectNext((Flush, 0))
      downstream.expectNext((Flush, 1))
      downstream.expectNext((Flush, 2))
      downstream.expectNext(("msg1", 0))
      upstream.sendNext(Flush)
      downstream.expectNext((Flush, 0))
      downstream.expectNext((Flush, 1))
      downstream.expectNext((Flush, 2))
      downstream.cancel()
    }

  }

}
