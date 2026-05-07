/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import java.io.OutputStream

import scala.annotation.nowarn
import scala.util.Success

import org.scalatest.concurrent.ScalaFutures

import akka.Done
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, IOOperationIncompleteException }
import akka.stream.scaladsl.{ Source, StreamConverters }
import akka.stream.testkit._
import akka.stream.testkit.Utils._
import akka.testkit.TestProbe
import akka.util.ByteString

@nowarn
class OutputStreamSinkSpec extends StreamSpec(UnboundedMailboxConfig) with ScalaFutures {

  val settings = ActorMaterializerSettings(system).withDispatcher("akka.actor.default-dispatcher")
  implicit val materializer: ActorMaterializer = ActorMaterializer(settings)

  "OutputStreamSink" must {
    "write bytes to void OutputStream" in {
      val p = TestProbe()
      val data = List(ByteString("a"), ByteString("c"), ByteString("c"))

      val completion = Source(data).runWith(StreamConverters.fromOutputStream(() =>
        new OutputStream {
          override def write(i: Int): Unit = ()
          override def write(bytes: Array[Byte]): Unit = p.ref ! ByteString(bytes).utf8String
        }))

      p.expectMsg(data(0).utf8String)
      p.expectMsg(data(1).utf8String)
      p.expectMsg(data(2).utf8String)
      completion.futureValue.count shouldEqual 3
      completion.futureValue.status shouldEqual Success(Done)
    }

    "auto flush when enabled" in {
      val p = TestProbe()
      val data = List(ByteString("a"), ByteString("c"))
      Source(data).runWith(
        StreamConverters.fromOutputStream(
          () =>
            new OutputStream {
              override def write(i: Int): Unit = ()
              override def write(bytes: Array[Byte]): Unit = p.ref ! ByteString(bytes).utf8String
              override def flush(): Unit = p.ref ! "flush"
            },
          autoFlush = true))
      p.expectMsg(data(0).utf8String)
      p.expectMsg("flush")
      p.expectMsg(data(1).utf8String)
      p.expectMsg("flush")
    }

    "close underlying stream when error received" in {
      val p = TestProbe()
      Source
        .failed(TE("Boom!"))
        .runWith(StreamConverters.fromOutputStream(() =>
          new OutputStream {
            override def write(i: Int): Unit = ()
            override def close() = p.ref ! "closed"
          }))

      p.expectMsg("closed")
    }

    "complete materialized value with the error for upstream" in {
      val completion = Source
        .failed(TE("Boom!"))
        .runWith(StreamConverters.fromOutputStream(() =>
          new OutputStream {
            override def write(i: Int): Unit = ()
            override def close() = ()
          }))

      completion.failed.futureValue shouldBe an[IOOperationIncompleteException]
    }

    "complete materialized value with the error if creation fails" in {
      val completion = Source
        .single(ByteString(1))
        .runWith(StreamConverters.fromOutputStream(() => {
          throw TE("Boom!")
          new OutputStream {
            override def write(i: Int): Unit = ()
            override def close() = ()
          }
        }))

      completion.failed.futureValue shouldBe an[IOOperationIncompleteException]
    }

    "complete materialized value with the error if write fails" in {
      val completion = Source
        .single(ByteString(1))
        .runWith(StreamConverters.fromOutputStream(() => {
          new OutputStream {
            override def write(i: Int): Unit = {
              throw TE("Boom!")
            }
            override def close() = ()
          }
        }))

      completion.failed.futureValue shouldBe an[IOOperationIncompleteException]
    }

    "complete materialized value with the error if close fails" in {
      val completion = Source
        .single(ByteString(1))
        .runWith(StreamConverters.fromOutputStream(() =>
          new OutputStream {
            override def write(i: Int): Unit = ()
            override def close(): Unit = {
              throw TE("Boom!")
            }
          }))

      completion.failed.futureValue shouldBe an[IOOperationIncompleteException]
    }

    "close underlying stream when completion received" in {
      val p = TestProbe()
      Source.empty.runWith(StreamConverters.fromOutputStream(() =>
        new OutputStream {
          override def write(i: Int): Unit = ()
          override def write(bytes: Array[Byte]): Unit = p.ref ! ByteString(bytes).utf8String
          override def close() = p.ref ! "closed"
        }))

      p.expectMsg("closed")
    }

  }

}
