/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.io

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.Attributes
import akka.stream.ThrottleMode
import akka.stream.impl.io.ByteStringParser
import akka.stream.impl.io.ByteStringParser.ByteReader
import akka.stream.impl.io.ByteStringParser.ParseResult
import akka.stream.impl.io.ByteStringParser.ParseStep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import akka.stream.testkit.StreamSpec
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.util.ByteString

class ByteStringParserSpec extends StreamSpec {

  "ByteStringParser" must {

    "respect backpressure" in {
      class Chunker extends ByteStringParser[ByteString] {
        import ByteStringParser._

        override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ParsingLogic {

          lazy val step: ParseStep[ByteString] = new ParseStep[ByteString] {
            override def parse(reader: ByteReader): ParseResult[ByteString] = {
              val bytes = reader.take(2)
              ParseResult(Some(bytes), step)
            }
          }

          startWith(step)

        }

      }

      // The Chunker produces two frames for one incoming 4 byte chunk. Hence, the rate in the incoming
      // side of the Chunker should only be half than on its outgoing side.

      val result = Source
        .repeat(ByteString("abcd"))
        .take(500)
        .throttle(1000, 1.second, 10, ThrottleMode.Enforcing)
        .via(new Chunker)
        .throttle(1000, 1.second, 10, ThrottleMode.Shaping)
        .runWith(Sink.ignore)

      Await.result(result, 5.seconds)

    }

    "continue parsing with multistep parsing logic if step doesn't produce element" in {
      object MultistepParsing extends ByteStringParser[ByteString] {
        def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ParsingLogic {
          object ReadHeader extends ParseStep[ByteString] {
            def parse(reader: ByteReader): ParseResult[ByteString] = {
              require(reader.readShortBE() == 0xcafe, "Magic header bytes not found")
              ParseResult(None, ReadData)
            }
          }
          object ReadData extends ParseStep[ByteString] {
            def parse(reader: ByteReader): ParseResult[ByteString] =
              ParseResult(Some(reader.takeAll()), ReadData)
          }

          startWith(ReadHeader)
        }
      }

      def run(data: ByteString*): ByteString =
        Await.result(
          Source[ByteString](data.toVector).via(MultistepParsing).fold(ByteString.empty)(_ ++ _).runWith(Sink.head),
          5.seconds)

      run(ByteString(0xca), ByteString(0xfe), ByteString(0xef, 0x12)) shouldEqual ByteString(0xef, 0x12)
      run(ByteString(0xca), ByteString(0xfe, 0xef, 0x12)) shouldEqual ByteString(0xef, 0x12)
      run(ByteString(0xca, 0xfe), ByteString(0xef, 0x12)) shouldEqual ByteString(0xef, 0x12)
      run(ByteString(0xca, 0xfe, 0xef, 0x12)) shouldEqual ByteString(0xef, 0x12)
    }

    "don't spin when logic is flawed" in {
      object SpinningLogic extends ByteStringParser[Int] {
        def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new ParsingLogic {
          object State1 extends ParseStep[Int] {
            def parse(reader: ByteReader): ParseResult[Int] =
              ParseResult(None, State2)
          }
          object State2 extends ParseStep[Int] {
            def parse(reader: ByteReader): ParseResult[Int] =
              ParseResult(None, State1)
          }

          startWith(State1)

          override protected def recursionLimit: Int = 10 // fail even earlier than the default
        }
      }

      (the[IllegalStateException] thrownBy Await.result(
        Source.single(ByteString("abc")).via(SpinningLogic).runWith(Sink.ignore),
        3.seconds)).getMessage shouldBe "Parsing logic didn't produce result after 10 steps. " +
      "Aborting processing to avoid infinite cycles. In the unlikely case that the parsing logic needs more recursion, " +
      "override ParsingLogic.recursionLimit."
    }

    "complete eagerly" in {
      object DummyParser extends ByteStringParser[ByteString] {
        def createLogic(inheritedAttributes: Attributes) = new ParsingLogic {
          startWith(new ParseStep[ByteString] {
            def parse(reader: ByteReader) = ParseResult(Some(reader.takeAll()), this)
          })
        }
      }

      val in = TestPublisher.probe[ByteString]()
      val out = TestSubscriber.probe[ByteString]()
      Source.fromPublisher(in).via(DummyParser).runWith(Sink.fromSubscriber(out))

      out.request(1L)
      in.expectRequest()
      in.sendNext(ByteString("aha!"))
      out.expectNext()
      // no new pull
      in.sendComplete()
      out.expectComplete()
    }

    "fail eagerly on truncation" in {
      object DummyParser extends ByteStringParser[ByteString] {
        def createLogic(inheritedAttributes: Attributes) = new ParsingLogic {
          startWith(new ParseStep[ByteString] {
            // take more data than there is in first chunk
            def parse(reader: ByteReader) = ParseResult(Some(reader.take(5)), this, false)
          })
        }
      }

      val in = TestPublisher.probe[ByteString]()
      val out = TestSubscriber.probe[ByteString]()
      Source.fromPublisher(in).via(DummyParser).runWith(Sink.fromSubscriber(out))

      out.request(1L)
      in.expectRequest()
      in.sendNext(ByteString("aha!"))
      out.expectNoMessage(100.millis)
      // no new pull
      in.sendComplete()
      out.expectError() shouldBe an[IllegalStateException]
    }
  }

}
