/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed.delivery

import org.scalatest.TestSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.typed.delivery.DurableProducerQueue.MessageSent
import akka.actor.typed.delivery.DurableProducerQueue.State
import akka.actor.typed.delivery.internal.ChunkedMessage
import akka.util.ByteString

class DurableProducerQueueSpec extends AnyWordSpec with TestSuite with Matchers {

  "DurableProducerQueue.State" must {
    "addMessageSent" in {
      val state1 = State.empty.addMessageSent(MessageSent(1, "a", false, "", 0L))
      state1.unconfirmed.size should ===(1)
      state1.unconfirmed.head.message should ===("a")
      state1.currentSeqNr should ===(2L)

      val state2 = state1.addMessageSent(MessageSent(2, "b", false, "", 0L))
      state2.unconfirmed.size should ===(2)
      state2.unconfirmed.last.message should ===("b")
      state2.currentSeqNr should ===(3L)
    }

    "confirm" in {
      val state1 = State.empty
        .addMessageSent(MessageSent(1, "a", false, "", 0L))
        .addMessageSent(MessageSent(2, "b", false, "", 0L))
      val state2 = state1.confirmed(1L, "", 0L)
      state2.unconfirmed.size should ===(1)
      state2.unconfirmed.head.message should ===("b")
      state2.currentSeqNr should ===(3L)
    }

    "filter partially stored chunked messages" in {
      val state1 = State
        .empty[String]
        .addMessageSent(
          MessageSent.fromChunked(1, ChunkedMessage(ByteString.fromString("a"), true, true, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(2, ChunkedMessage(ByteString.fromString("b"), true, false, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(3, ChunkedMessage(ByteString.fromString("c"), false, false, 20, ""), false, "", 0L))
      // last chunk was never stored

      val state2 = state1.cleanupPartialChunkedMessages()
      state2.unconfirmed.size should ===(1)
      state2.unconfirmed.head.message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("a"))
      state2.currentSeqNr should ===(2L)

      val state3 = state1
        .addMessageSent(
          MessageSent.fromChunked(2, ChunkedMessage(ByteString.fromString("d"), true, false, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(3, ChunkedMessage(ByteString.fromString("e"), false, true, 20, ""), false, "", 0L))

      val state4 = state3.cleanupPartialChunkedMessages()
      state4.unconfirmed.size should ===(3)
      state4.unconfirmed.head.message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("a"))
      state4.unconfirmed(1).message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("d"))
      state4.unconfirmed(2).message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("e"))
      state4.currentSeqNr should ===(4L)

      val state5 = state3
        .addMessageSent(
          MessageSent.fromChunked(4, ChunkedMessage(ByteString.fromString("f"), true, false, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(5, ChunkedMessage(ByteString.fromString("g"), false, false, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(4, ChunkedMessage(ByteString.fromString("h"), true, true, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(5, ChunkedMessage(ByteString.fromString("i"), true, false, 20, ""), false, "", 0L))
        .addMessageSent(
          MessageSent.fromChunked(6, ChunkedMessage(ByteString.fromString("j"), false, false, 20, ""), false, "", 0L))

      val state6 = state5.cleanupPartialChunkedMessages()
      state6.unconfirmed.size should ===(4)
      state6.unconfirmed.head.message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("a"))
      state6.unconfirmed(1).message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("d"))
      state6.unconfirmed(2).message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("e"))
      state6.unconfirmed(3).message.asInstanceOf[ChunkedMessage].serialized should be(ByteString.fromString("h"))
      state6.currentSeqNr should ===(5L)
    }

  }

}
