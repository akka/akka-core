/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.nio.{ ByteBuffer, CharBuffer }
import java.nio.ByteOrder
import java.nio.charset.Charset

import scala.annotation.nowarn
import scala.concurrent.duration._

import akka.actor.{ ActorRef, ActorSystem, ExtendedActorSystem, InternalActorRef }
import akka.event._
import akka.testkit.{ AkkaSpec, EventFilter, TestProbe }
import akka.testkit.TestEvent.Mute
import akka.util.OptionVal

class RemoteInstrumentsSerializationSpec extends AkkaSpec("akka.loglevel = DEBUG") {
  import RemoteInstrumentsSerializationSpec._

  def remoteInstruments(instruments: RemoteInstrument*): RemoteInstruments = {
    val vec = Vector(instruments: _*)
    new RemoteInstruments(system.asInstanceOf[ExtendedActorSystem], system.log, vec)
  }

  def ensureDebugLog[T](messages: String*)(f: => T): T = {
    if (messages.isEmpty)
      f
    else
      EventFilter.debug(message = messages.head, occurrences = 1).intercept {
        ensureDebugLog(messages.tail: _*)(f)
      }
  }

  "RemoteInstruments" should {
    "not write anything in the buffer if not deserializing" in {
      val buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)
      serialize(remoteInstruments(), buffer)
      buffer.position() should be(0)
    }

    "serialize and deserialize a single remote instrument" in {
      val p = TestProbe()
      val ri = remoteInstruments(testInstrument(1, "!"))
      serializeDeserialize(ri, ri, p.ref, "foo")
      p.expectMsgAllOf("foo-1-!")
      p.expectNoMessage(100.millis)
    }

    "serialize and deserialize multiple remote instruments in the correct order" in {
      val p = TestProbe()
      val ri = remoteInstruments(testInstrument(1, "!"), testInstrument(31, "???"), testInstrument(10, ".."))
      serializeDeserialize(ri, ri, p.ref, "bar")
      p.expectMsgAllOf("bar-1-!", "bar-10-..", "bar-31-???")
      p.expectNoMessage(100.millis)
    }

    "skip existing remote instruments not in the message" in {
      ensureDebugLog("Skipping local RemoteInstrument 10 that has no matching data in the message") {
        val p = TestProbe()
        val instruments = Seq(testInstrument(7, "!"), testInstrument(10, ".."), testInstrument(21, "???"))
        val riS = remoteInstruments(instruments(0), instruments(2))
        val riD = remoteInstruments(instruments: _*)
        serializeDeserialize(riS, riD, p.ref, "baz")
        p.expectMsgAllOf("baz-7-!", "baz-21-???")
        p.expectNoMessage(100.millis)
      }
    }

    "skip remote instruments in the message that are not existing" in {
      ensureDebugLog("Skipping serialized data in message for RemoteInstrument 11 that has no local match") {
        val p = TestProbe()
        val instruments = Seq(testInstrument(6, "!"), testInstrument(11, ".."), testInstrument(19, "???"))
        val riS = remoteInstruments(instruments: _*)
        val riD = remoteInstruments(instruments(0), instruments(2))
        serializeDeserialize(riS, riD, p.ref, "buz")
        p.expectMsgAllOf("buz-6-!", "buz-19-???")
        p.expectNoMessage(100.millis)
      }
    }

    "skip all remote instruments in the message if none are existing" in {
      ensureDebugLog("Skipping serialized data in message for RemoteInstrument(s) [1, 10, 31] that has no local match") {
        val p = TestProbe()
        val instruments = Seq(testInstrument(1, "!"), testInstrument(10, ".."), testInstrument(31, "???"))
        val riS = remoteInstruments(instruments: _*)
        val riD = remoteInstruments()
        serializeDeserialize(riS, riD, p.ref, "boz")
        p.expectNoMessage(100.millis)
      }
    }

    "skip serializing remote instrument that fails" in {
      ensureDebugLog(
        "Skipping serialization of RemoteInstrument 7 since it failed with boom",
        "Skipping local RemoteInstrument 7 that has no matching data in the message") {
        val p = TestProbe()
        val instruments =
          Seq(testInstrument(7, "!", sentThrowable = boom), testInstrument(10, ".."), testInstrument(21, "???"))
        val ri = remoteInstruments(instruments: _*)
        serializeDeserialize(ri, ri, p.ref, "woot")
        p.expectMsgAllOf("woot-10-..", "woot-21-???")
        p.expectNoMessage(100.millis)
      }
    }

    "skip deserializing remote instrument that fails" in {
      ensureDebugLog(
        "Skipping deserialization of RemoteInstrument 7 since it failed with boom",
        "Skipping deserialization of RemoteInstrument 21 since it failed with boom") {
        val p = TestProbe()
        val instruments = Seq(
          testInstrument(7, "!", receiveThrowable = boom),
          testInstrument(10, ".."),
          testInstrument(21, "???", receiveThrowable = boom))
        val ri = remoteInstruments(instruments: _*)
        serializeDeserialize(ri, ri, p.ref, "waat")
        p.expectMsgAllOf("waat-10-..")
        p.expectNoMessage(100.millis)
      }
    }
  }
}

object RemoteInstrumentsSerializationSpec {

  class Filter(@nowarn("msg=never used") settings: ActorSystem.Settings, stream: EventStream) extends LoggingFilter {
    stream.publish(Mute(EventFilter.debug()))

    override def isErrorEnabled(logClass: Class[_], logSource: String): Boolean = true

    override def isWarningEnabled(logClass: Class[_], logSource: String): Boolean = true

    override def isInfoEnabled(logClass: Class[_], logSource: String): Boolean = true

    override def isDebugEnabled(logClass: Class[_], logSource: String): Boolean = logSource == "DebugSource"
  }

  def testInstrument(
      id: Int,
      metadata: String,
      sentThrowable: Throwable = null,
      receiveThrowable: Throwable = null): RemoteInstrument = {
    new RemoteInstrument {
      private val charset = Charset.forName("UTF-8")
      private val encoder = charset.newEncoder()
      private val decoder = charset.newDecoder()

      override def identifier: Byte = id.toByte

      override def remoteWriteMetadata(
          recipient: ActorRef,
          message: Object,
          sender: ActorRef,
          buffer: ByteBuffer): Unit = {
        buffer.putInt(metadata.length)
        if (sentThrowable ne null) throw sentThrowable
        encoder.encode(CharBuffer.wrap(metadata), buffer, true)
        encoder.flush(buffer)
        encoder.reset()
      }

      override def remoteReadMetadata(
          recipient: ActorRef,
          message: Object,
          sender: ActorRef,
          buffer: ByteBuffer): Unit = {
        val size = buffer.getInt
        if (receiveThrowable ne null) throw receiveThrowable
        val charBuffer = CharBuffer.allocate(size)
        decoder.decode(buffer, charBuffer, false)
        decoder.reset()
        charBuffer.flip()
        val string = charBuffer.toString
        recipient ! s"$message-$identifier-$string"
      }

      override def remoteMessageSent(
          recipient: ActorRef,
          message: Object,
          sender: ActorRef,
          size: Int,
          time: Long): Unit = ()

      override def remoteMessageReceived(
          recipient: ActorRef,
          message: Object,
          sender: ActorRef,
          size: Int,
          time: Long): Unit = ()
    }
  }

  def serialize(ri: RemoteInstruments, buffer: ByteBuffer): Unit = {
    val mockOutbound = new ReusableOutboundEnvelope()
    ri.serialize(OptionVal(mockOutbound), buffer)
  }

  def deserialize(ri: RemoteInstruments, buffer: ByteBuffer, recipient: ActorRef, message: AnyRef): Unit = {
    val r = recipient.asInstanceOf[InternalActorRef]
    val envelopeBuffer = new EnvelopeBuffer(buffer)
    val mockInbound =
      new ReusableInboundEnvelope().withEnvelopeBuffer(envelopeBuffer).withRecipient(r).withMessage(message)
    ri.deserializeRaw(mockInbound)
  }

  def serializeDeserialize(
      riS: RemoteInstruments,
      riD: RemoteInstruments,
      recipient: ActorRef,
      message: AnyRef): Unit = {
    val buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN)
    serialize(riS, buffer)
    buffer.flip()
    deserialize(riD, buffer, recipient, message)
  }

  val boom = new IllegalArgumentException("boom")
}
