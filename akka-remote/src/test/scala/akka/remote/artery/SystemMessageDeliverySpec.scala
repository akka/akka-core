/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.Await
import scala.concurrent.duration._

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import akka.NotUsed
import akka.actor.ActorIdentity
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Identify
import akka.actor.RootActorPath
import akka.remote.RARP
import akka.remote.UniqueAddress
import akka.remote.artery.SystemMessageDelivery._
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.EventFilter
import akka.testkit.ImplicitSender
import akka.testkit.TestActors
import akka.testkit.TestEvent
import akka.testkit.TestProbe
import akka.util.OptionVal

object SystemMessageDeliverySpec {

  case class TestSysMsg(s: String) extends SystemMessageDelivery.AckedDeliveryMessage

  val safe = ConfigFactory.parseString("""
       akka.loglevel = INFO
       akka.remote.artery.advanced.stop-idle-outbound-after = 1000 ms
       akka.remote.artery.advanced.inject-handshake-interval = 500 ms
       akka.remote.watch-failure-detector.heartbeat-interval = 2 s
       akka.remote.artery.log-received-messages = on
       akka.remote.artery.log-sent-messages = on
       akka.stream.materializer.debug.fuzzing-mode = on
    """).withFallback(ArterySpecSupport.defaultConfig)

  val config =
    ConfigFactory.parseString("akka.remote.use-unsafe-remote-features-outside-cluster = on").withFallback(safe)
}

abstract class AbstractSystemMessageDeliverySpec(c: Config) extends ArteryMultiNodeSpec(c) with ImplicitSender {

  import SystemMessageDeliverySpec._

  val addressA = UniqueAddress(address(system), system.asInstanceOf[ExtendedActorSystem].uid)
  val systemB = newRemoteSystem(name = Some("systemB"))
  val addressB = UniqueAddress(address(systemB), systemB.asInstanceOf[ExtendedActorSystem].uid)
  val rootB = RootActorPath(addressB.address)

  private val outboundEnvelopePool = ReusableOutboundEnvelope.createObjectPool(capacity = 16)

  system.eventStream.publish(TestEvent.Mute(EventFilter.warning(pattern = ".*negative acknowledgement.*")))
  systemB.eventStream.publish(TestEvent.Mute(EventFilter.warning(pattern = ".*negative acknowledgement.*")))

  protected def send(
      sendCount: Int,
      resendInterval: FiniteDuration,
      outboundContext: OutboundContext): Source[OutboundEnvelope, NotUsed] = {
    val deadLetters = TestProbe().ref
    Source(1 to sendCount)
      .map(n => outboundEnvelopePool.acquire().init(OptionVal.None, TestSysMsg("msg-" + n), OptionVal.None))
      .via(new SystemMessageDelivery(outboundContext, deadLetters, resendInterval, maxBufferSize = 1000))
  }

  protected def inbound(inboundContext: InboundContext): Flow[OutboundEnvelope, InboundEnvelope, NotUsed] = {
    val recipient = OptionVal.None // not used
    Flow[OutboundEnvelope]
      .map(outboundEnvelope =>
        outboundEnvelope.message match {
          case sysEnv: SystemMessageEnvelope =>
            InboundEnvelope(recipient, sysEnv, OptionVal.None, addressA.uid, inboundContext.association(addressA.uid))
          case _ => throw new RuntimeException()
        })
      .async
      .via(new SystemMessageAcker(inboundContext))
  }

  protected def drop(dropSeqNumbers: Vector[Long]): Flow[OutboundEnvelope, OutboundEnvelope, NotUsed] = {
    Flow[OutboundEnvelope].statefulMapConcat(() => {
      var dropping = dropSeqNumbers

      { outboundEnvelope =>
        outboundEnvelope.message match {
          case SystemMessageEnvelope(_, seqNo, _) =>
            val i = dropping.indexOf(seqNo)
            if (i >= 0) {
              dropping = dropping.updated(i, -1L)
              Nil
            } else
              List(outboundEnvelope)
          case _ => Nil
        }
      }
    })
  }

  protected def randomDrop[T](dropRate: Double): Flow[T, T, NotUsed] = Flow[T].mapConcat { elem =>
    if (ThreadLocalRandom.current().nextDouble() < dropRate) Nil
    else List(elem)
  }
}

class SystemMessageDeliverySpec extends AbstractSystemMessageDeliverySpec(SystemMessageDeliverySpec.config) {
  import SystemMessageDeliverySpec._

  "System messages" must {

    "be delivered with real actors" in {
      val systemBRef = systemB.actorOf(TestActors.echoActorProps, "echo")

      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      watch(remoteRef)
      systemB.stop(systemBRef)
      expectTerminated(remoteRef)
    }

    "be delivered when concurrent idle stopping" in {
      // it's configured with short stop-idle-outbound-after to stress exercise stopping of idle outbound streams
      // at the same time as system messages are sent

      val systemBRef = systemB.actorOf(TestActors.echoActorProps, "echo2")

      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo2") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      val idleTimeout =
        RARP(system).provider.transport.asInstanceOf[ArteryTransport].settings.Advanced.StopIdleOutboundAfter
      val rnd = ThreadLocalRandom.current()

      (1 to 5).foreach { _ =>
        (1 to 1).foreach { _ =>
          watch(remoteRef)
          unwatch(remoteRef)
        }
        Thread.sleep((idleTimeout - 10.millis).toMillis + rnd.nextInt(20))
      }

      watch(remoteRef)
      remoteRef ! "ping2"
      expectMsg("ping2")
      systemB.stop(systemBRef)
      expectTerminated(remoteRef, 5.seconds)
    }

    "be flushed on shutdown" in {
      val systemC = ActorSystem("systemC", system.settings.config)
      try {
        systemC.actorOf(TestActors.echoActorProps, "echo")

        val addressC = RARP(systemC).provider.getDefaultAddress
        val rootC = RootActorPath(addressC)

        val remoteRef = {
          system.actorSelection(rootC / "user" / "echo") ! Identify(None)
          expectMsgType[ActorIdentity].ref.get
        }

        watch(remoteRef)
        remoteRef ! "hello"
        expectMsg("hello")
        Await.ready(systemC.terminate(), 10.seconds)
        system.log.debug("systemC terminated")
        // DeathWatchNotification is sent from systemC, failure detection takes longer than 3 seconds
        expectTerminated(remoteRef, 10.seconds)
      } finally {
        shutdown(systemC)
      }
    }

    "be resent when some in the middle are lost" in {
      val replyProbe = TestProbe()
      val controlSubject = new TestControlMessageSubject
      val inboundContextB = new ManualReplyInboundContext(replyProbe.ref, addressB, controlSubject)
      val inboundContextA = new TestInboundContext(addressB, controlSubject)
      val outboundContextA = inboundContextA.association(addressB.address)

      val sink = send(sendCount = 5, resendInterval = 1.second, outboundContextA)
        .via(drop(dropSeqNumbers = Vector(3L, 4L)))
        .via(inbound(inboundContextB))
        .map(_.message.asInstanceOf[TestSysMsg])
        .runWith(TestSink[TestSysMsg]())

      sink.request(100)
      sink.expectNext(TestSysMsg("msg-1"))
      sink.expectNext(TestSysMsg("msg-2"))
      replyProbe.expectMsg(Ack(1L, addressB))
      replyProbe.expectMsg(Ack(2L, addressB))
      // 3 and 4 was dropped
      replyProbe.expectMsg(Nack(2L, addressB))
      sink.expectNoMessage(100.millis) // 3 was dropped
      inboundContextB.deliverLastReply()
      // resending 3, 4, 5
      sink.expectNext(TestSysMsg("msg-3"))
      replyProbe.expectMsg(Ack(3L, addressB))
      sink.expectNext(TestSysMsg("msg-4"))
      replyProbe.expectMsg(Ack(4L, addressB))
      sink.expectNext(TestSysMsg("msg-5"))
      replyProbe.expectMsg(Ack(5L, addressB))
      replyProbe.expectNoMessage(100.millis)
      inboundContextB.deliverLastReply()
      sink.expectComplete()
    }

    "be resent when first is lost" in {
      val replyProbe = TestProbe()
      val controlSubject = new TestControlMessageSubject
      val inboundContextB = new ManualReplyInboundContext(replyProbe.ref, addressB, controlSubject)
      val inboundContextA = new TestInboundContext(addressB, controlSubject)
      val outboundContextA = inboundContextA.association(addressB.address)

      val sink = send(sendCount = 3, resendInterval = 1.second, outboundContextA)
        .via(drop(dropSeqNumbers = Vector(1L)))
        .via(inbound(inboundContextB))
        .map(_.message.asInstanceOf[TestSysMsg])
        .runWith(TestSink[TestSysMsg]())

      sink.request(100)
      replyProbe.expectMsg(Nack(0L, addressB)) // from receiving 2
      replyProbe.expectMsg(Nack(0L, addressB)) // from receiving 3
      sink.expectNoMessage(100.millis) // 1 was dropped
      inboundContextB.deliverLastReply() // it's ok to not delivery all nacks
      // resending 1, 2, 3
      sink.expectNext(TestSysMsg("msg-1"))
      replyProbe.expectMsg(Ack(1L, addressB))
      sink.expectNext(TestSysMsg("msg-2"))
      replyProbe.expectMsg(Ack(2L, addressB))
      sink.expectNext(TestSysMsg("msg-3"))
      replyProbe.expectMsg(Ack(3L, addressB))
      inboundContextB.deliverLastReply()
      sink.expectComplete()
    }

    "be resent when last is lost" in {
      val replyProbe = TestProbe()
      val controlSubject = new TestControlMessageSubject
      val inboundContextB = new ManualReplyInboundContext(replyProbe.ref, addressB, controlSubject)
      val inboundContextA = new TestInboundContext(addressB, controlSubject)
      val outboundContextA = inboundContextA.association(addressB.address)

      val sink = send(sendCount = 3, resendInterval = 2.seconds, outboundContextA)
        .via(drop(dropSeqNumbers = Vector(3L)))
        .via(inbound(inboundContextB))
        .map(_.message.asInstanceOf[TestSysMsg])
        .runWith(TestSink[TestSysMsg]())

      sink.request(100)
      sink.expectNext(TestSysMsg("msg-1"))
      replyProbe.expectMsg(Ack(1L, addressB))
      inboundContextB.deliverLastReply()
      sink.expectNext(TestSysMsg("msg-2"))
      replyProbe.expectMsg(Ack(2L, addressB))
      inboundContextB.deliverLastReply()
      sink.expectNoMessage(200.millis) // 3 was dropped
      // resending 3 due to timeout
      sink.expectNext(TestSysMsg("msg-3"))
      replyProbe.expectMsg(4.seconds, Ack(3L, addressB))
      // continue resending
      replyProbe.expectMsg(4.seconds, Ack(3L, addressB))
      inboundContextB.deliverLastReply()
      replyProbe.expectNoMessage(2200.millis)
      sink.expectComplete()
    }

    "deliver all during stress and random dropping" in {
      val N = 500
      val dropRate = 0.05
      val controlSubject = new TestControlMessageSubject
      val inboundContextB = new TestInboundContext(addressB, controlSubject, replyDropRate = dropRate)
      val inboundContextA = new TestInboundContext(addressB, controlSubject)
      val outboundContextA = inboundContextA.association(addressB.address)

      val output =
        send(N, 100.millis, outboundContextA)
          .via(randomDrop(dropRate))
          .via(inbound(inboundContextB))
          .map(_.message.asInstanceOf[TestSysMsg])
          .runWith(Sink.seq)

      Await.result(output, 30.seconds) should ===((1 to N).map(n => TestSysMsg("msg-" + n)).toVector)
    }

    "deliver all during throttling and random dropping" in {
      val N = 100
      val dropRate = 0.05
      val controlSubject = new TestControlMessageSubject
      val inboundContextB = new TestInboundContext(addressB, controlSubject, replyDropRate = dropRate)
      val inboundContextA = new TestInboundContext(addressB, controlSubject)
      val outboundContextA = inboundContextA.association(addressB.address)

      val output =
        send(N, 300.millis, outboundContextA)
          .throttle(200, 1.second, 10, ThrottleMode.shaping)
          .via(randomDrop(dropRate))
          .via(inbound(inboundContextB))
          .map(_.message.asInstanceOf[TestSysMsg])
          .runWith(Sink.seq)

      Await.result(output, 30.seconds) should ===((1 to N).map(n => TestSysMsg("msg-" + n)).toVector)
    }
  }
}

class SystemMessageDeliverySafeSpec extends AbstractSystemMessageDeliverySpec(SystemMessageDeliverySpec.safe) {
  "System messages without cluster" must {

    "not be delivered when concurrent idle stopping" in {
      val systemBRef = systemB.actorOf(TestActors.echoActorProps, "echo")

      val remoteRef = {
        system.actorSelection(rootB / "user" / "echo") ! Identify(None)
        expectMsgType[ActorIdentity].ref.get
      }

      val idleTimeout =
        RARP(system).provider.transport.asInstanceOf[ArteryTransport].settings.Advanced.StopIdleOutboundAfter
      val rnd = ThreadLocalRandom.current()

      (1 to 5).foreach { _ =>
        (1 to 1).foreach { _ =>
          watch(remoteRef)
          unwatch(remoteRef)
        }
        Thread.sleep((idleTimeout - 10.millis).toMillis + rnd.nextInt(20))
      }

      watch(remoteRef)
      remoteRef ! "ping"
      expectMsg("ping")
      systemB.stop(systemBRef)
      expectNoMessage(2.seconds)
    }
  }
}
