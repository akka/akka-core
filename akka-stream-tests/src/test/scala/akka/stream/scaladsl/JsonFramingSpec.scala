/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.scaladsl

import scala.collection.immutable.Seq
import scala.concurrent.Await
import scala.concurrent.duration._

import akka.stream.impl.JsonObjectParser
import akka.stream.scaladsl.Framing.FramingException
import akka.stream.scaladsl.JsonFraming.PartialObjectException
import akka.stream.testkit.{ TestPublisher, TestSubscriber }
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec
import akka.util.ByteString

class JsonFramingSpec extends AkkaSpec {

  "collecting multiple json" should {
    "parse json array" in {
      // #using-json-framing
      val input =
        """
          |[
          | { "name" : "john" },
          | { "name" : "Ég get etið gler án þess að meiða mig" },
          | { "name" : "jack" },
          |]
          |""".stripMargin // also should complete once notices end of array

      val result =
        Source.single(ByteString(input)).via(JsonFraming.objectScanner(Int.MaxValue)).runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }
      // #using-json-framing

      result.futureValue shouldBe Seq(
        """{ "name" : "john" }""",
        """{ "name" : "Ég get etið gler án þess að meiða mig" }""",
        """{ "name" : "jack" }""")
    }

    // coverage of #31569
    "parse json array with object size smaller than maximumObjectLength" in {
      val input =
        """
          |[
          | { "name" : "john" },
          | { "name" : "Ég get etið gler án þess að meiða mig" },
          | { "name" : "jack" },
          |]
          |""".stripMargin // also should complete once notices end of array

      // Using small maximumObjectLength here. Smaller than the full input, but larger than each json object.
      val result =
        Source.single(ByteString(input)).via(JsonFraming.objectScanner(64)).runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }

      result.futureValue shouldBe Seq(
        """{ "name" : "john" }""",
        """{ "name" : "Ég get etið gler án þess að meiða mig" }""",
        """{ "name" : "jack" }""")
    }

    "parse multiple arrays" in {
      val input1 =
        """
          |[
          | { "name" : "john" }
          |]
          |""".stripMargin

      val input2 =
        """
          |[
          | { "name" : "jack" }
          |]
          |""".stripMargin

      val result = Source(List(ByteString(input1), ByteString(input2)))
        .via(JsonFraming.objectScanner(Int.MaxValue))
        .runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }

      result.futureValue shouldBe Seq("""{ "name" : "john" }""", """{ "name" : "jack" }""")
    }

    "emit single json element from string" in {
      val input =
        """| { "name": "john" }
           | { "name": "jack" }
        """.stripMargin

      val result = Source
        .single(ByteString(input))
        .via(JsonFraming.objectScanner(Int.MaxValue))
        .take(1)
        .runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }

      Await.result(result, 3.seconds) shouldBe Seq("""{ "name": "john" }""")
    }

    "parse line delimited" in {
      val input =
        """| { "name": "john" }
           | { "name": "jack" }
           | { "name": "katie" }
        """.stripMargin

      val result =
        Source.single(ByteString(input)).via(JsonFraming.objectScanner(Int.MaxValue)).runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }

      Await.result(result, 3.seconds) shouldBe Seq(
        """{ "name": "john" }""",
        """{ "name": "jack" }""",
        """{ "name": "katie" }""")
    }

    "parse comma delimited" in {
      val input =
        """  { "name": "john" }, { "name": "jack" }, { "name": "katie" }  """

      val result =
        Source.single(ByteString(input)).via(JsonFraming.objectScanner(Int.MaxValue)).runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry.utf8String)
        }

      result.futureValue shouldBe Seq("""{ "name": "john" }""", """{ "name": "jack" }""", """{ "name": "katie" }""")
    }

    "parse chunks successfully" in {
      val input: Seq[ByteString] = Seq(
        """
          |[
          |  { "name": "john"""".stripMargin,
        """
          |},
        """.stripMargin,
        """{ "na""",
        """me": "jack""",
        """"}]""").map(ByteString(_))

      val result = Source.apply(input).via(JsonFraming.objectScanner(Int.MaxValue)).runFold(Seq.empty[String]) {
        case (acc, entry) => acc ++ Seq(entry.utf8String)
      }

      result.futureValue shouldBe Seq(
        """{ "name": "john"
          |}""".stripMargin,
        """{ "name": "jack"}""")
    }

    "emit all elements after input completes" in {
      // coverage for #21150
      val input = TestPublisher.probe[ByteString]()
      val output = TestSubscriber.probe[String]()

      Source
        .fromPublisher(input)
        .via(JsonFraming.objectScanner(Int.MaxValue))
        .map(_.utf8String)
        .runWith(Sink.fromSubscriber(output))

      output.request(1)
      input.expectRequest()
      input.sendNext(ByteString("""[{"a":0}, {"b":1}, {"c":2}, {"d":3}, {"e":4}]"""))
      input.sendComplete()
      Thread.sleep(10) // another of those races, we don't know the order of next and complete
      output.expectNext("""{"a":0}""")
      output.request(1)
      output.expectNext("""{"b":1}""")
      output.request(1)
      output.expectNext("""{"c":2}""")
      output.request(1)
      output.expectNext("""{"d":3}""")
      output.request(1)
      output.expectNext("""{"e":4}""")
      output.request(1)
      output.expectComplete()
    }
  }

  "collecting json buffer" when {
    "nothing is supplied" should {
      "return nothing" in {
        val buffer = new JsonObjectParser()
        buffer.poll() should ===(None)
      }
    }

    "valid json is supplied".which {
      "has one object" should {
        "successfully parse empty object" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{}"""))
          buffer.poll().get.utf8String shouldBe """{}"""
        }

        "successfully parse single field having string value" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "name": "john"}"""))
          buffer.poll().get.utf8String shouldBe """{ "name": "john"}"""
        }

        "successfully parse single field having string value containing space" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "name": "john doe"}"""))
          buffer.poll().get.utf8String shouldBe """{ "name": "john doe"}"""
        }

        "successfully parse single field having string value containing single quote" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "name": "john o'doe"}"""))
          buffer.poll().get.utf8String shouldBe """{ "name": "john o'doe"}"""
        }

        "successfully parse single field having string value containing curly brace" in {
          val buffer = new JsonObjectParser()

          buffer.offer(ByteString("""{ "name": "john{"""))
          buffer.offer(ByteString("}"))
          buffer.offer(ByteString("\""))
          buffer.offer(ByteString("}"))

          buffer.poll().get.utf8String shouldBe """{ "name": "john{}"}"""
        }

        "successfully parse single field having string value containing curly brace and escape character" in {
          val buffer = new JsonObjectParser()

          buffer.offer(ByteString("""{ "name": "john"""))
          buffer.offer(ByteString("\\\""))
          buffer.offer(ByteString("{"))
          buffer.offer(ByteString("}"))
          buffer.offer(ByteString("\\\""))
          buffer.offer(ByteString(" "))
          buffer.offer(ByteString("hey"))
          buffer.offer(ByteString("\""))

          buffer.offer(ByteString("}"))
          buffer.poll().get.utf8String shouldBe """{ "name": "john\"{}\" hey"}"""
        }

        "successfully parse single field having integer value" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "age": 101}"""))
          buffer.poll().get.utf8String shouldBe """{ "age": 101}"""
        }

        "successfully parse single field having decimal value" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "age": 10.1}"""))
          buffer.poll().get.utf8String shouldBe """{ "age": 10.1}"""
        }

        "successfully parse single field having nested object" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{  "name": "john",
              |   "age": 101,
              |   "address": {
              |     "street": "Straight Street",
              |     "postcode": 1234
              |   }
              |}
              | """.stripMargin))
          buffer.poll().get.utf8String shouldBe """{  "name": "john",
                                                      |   "age": 101,
                                                      |   "address": {
                                                      |     "street": "Straight Street",
                                                      |     "postcode": 1234
                                                      |   }
                                                      |}""".stripMargin
        }

        "successfully parse single field having multiple level of nested object" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{  "name": "john",
              |   "age": 101,
              |   "address": {
              |     "street": {
              |       "name": "Straight",
              |       "type": "Avenue"
              |     },
              |     "postcode": 1234
              |   }
              |}
              | """.stripMargin))
          buffer.poll().get.utf8String shouldBe """{  "name": "john",
                                                      |   "age": 101,
                                                      |   "address": {
                                                      |     "street": {
                                                      |       "name": "Straight",
                                                      |       "type": "Avenue"
                                                      |     },
                                                      |     "postcode": 1234
                                                      |   }
                                                      |}""".stripMargin
        }

        "successfully parse an escaped backslash followed by a double quote" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{
              | "key": "\\"
              | }
              | """.stripMargin))

          buffer.poll().get.utf8String shouldBe """{
                                          | "key": "\\"
                                          | }""".stripMargin
        }

        "successfully parse a string that contains an escaped quote" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{
              | "key": "\""
              | }
              | """.stripMargin))

          buffer.poll().get.utf8String shouldBe """{
                                                  | "key": "\""
                                                  | }""".stripMargin
        }

        "successfully parse a string that contains escape sequence" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{
              | "key": "\\\""
              | }
              | """.stripMargin))

          buffer.poll().get.utf8String shouldBe """{
                                                  | "key": "\\\""
                                                  | }""".stripMargin
        }
      }

      "has nested array" should {
        "successfully parse" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{  "name": "john",
              |   "things": [
              |     1,
              |     "hey",
              |     3,
              |     "there"
              |   ]
              |}
              | """.stripMargin))
          buffer.poll().get.utf8String shouldBe """{  "name": "john",
                                                      |   "things": [
                                                      |     1,
                                                      |     "hey",
                                                      |     3,
                                                      |     "there"
                                                      |   ]
                                                      |}""".stripMargin
        }
      }

      "has complex object graph" should {
        "successfully parse" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |{
              |  "name": "john",
              |  "addresses": [
              |    {
              |      "street": "3 Hopson Street",
              |      "postcode": "ABC-123",
              |      "tags": ["work", "office"],
              |      "contactTime": [
              |        {"time": "0900-1800", "timezone", "UTC"}
              |      ]
              |    },
              |    {
              |      "street": "12 Adielie Road",
              |      "postcode": "ZZY-888",
              |      "tags": ["home"],
              |      "contactTime": [
              |        {"time": "0800-0830", "timezone", "UTC"},
              |        {"time": "1800-2000", "timezone", "UTC"}
              |      ]
              |    }
              |  ]
              |}
              | """.stripMargin))

          buffer.poll().get.utf8String shouldBe """{
                                                      |  "name": "john",
                                                      |  "addresses": [
                                                      |    {
                                                      |      "street": "3 Hopson Street",
                                                      |      "postcode": "ABC-123",
                                                      |      "tags": ["work", "office"],
                                                      |      "contactTime": [
                                                      |        {"time": "0900-1800", "timezone", "UTC"}
                                                      |      ]
                                                      |    },
                                                      |    {
                                                      |      "street": "12 Adielie Road",
                                                      |      "postcode": "ZZY-888",
                                                      |      "tags": ["home"],
                                                      |      "contactTime": [
                                                      |        {"time": "0800-0830", "timezone", "UTC"},
                                                      |        {"time": "1800-2000", "timezone", "UTC"}
                                                      |      ]
                                                      |    }
                                                      |  ]
                                                      |}""".stripMargin
        }
      }

      "has multiple fields" should {
        "parse successfully" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "name": "john", "age": 101}"""))
          buffer.poll().get.utf8String shouldBe """{ "name": "john", "age": 101}"""
        }

        "parse successfully despite valid whitespaces around json" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""
              |
              |
              |{"name":   "john"
              |, "age": 101}""".stripMargin))
          buffer.poll().get.utf8String shouldBe
          """{"name":   "john"
              |, "age": 101}""".stripMargin
        }
      }

      "has multiple objects" should {
        "pops the right object as buffer is filled" in {
          val input =
            """
              |  {
              |    "name": "john",
              |    "age": 32
              |  },
              |  {
              |    "name": "katie",
              |    "age": 25
              |  }
            """.stripMargin

          val buffer = new JsonObjectParser()
          buffer.offer(ByteString(input))

          buffer.poll().get.utf8String shouldBe
          """{
              |    "name": "john",
              |    "age": 32
              |  }""".stripMargin
          buffer.poll().get.utf8String shouldBe
          """{
              |    "name": "katie",
              |    "age": 25
              |  }""".stripMargin
          buffer.poll() should ===(None)

          buffer.offer(ByteString("""{"name":"jenkins","age": """))
          buffer.poll() should ===(None)

          buffer.offer(ByteString("65 }"))
          buffer.poll().get.utf8String shouldBe """{"name":"jenkins","age": 65 }"""
        }
      }

      "returns none until valid json is encountered" in {
        val buffer = new JsonObjectParser()

        """{ "name": "john"""".foreach { c =>
          buffer.offer(ByteString(c))
          buffer.poll() should ===(None)
        }

        buffer.offer(ByteString("}"))
        buffer.poll().get.utf8String shouldBe """{ "name": "john"}"""
      }

      "invalid json is supplied" should {
        "fail if it's broken from the start" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""THIS IS NOT VALID { "name": "john"}"""))
          a[FramingException] shouldBe thrownBy { buffer.poll() }
        }

        "fail if it's broken at the end" in {
          val buffer = new JsonObjectParser()
          buffer.offer(ByteString("""{ "name": "john"} THIS IS NOT VALID"""))
          buffer.poll() // first emitting the valid element
          a[FramingException] shouldBe thrownBy { buffer.poll() }
        }
      }
    }

    "fail on too large initial object" in {
      val input =
        """
          | { "name": "john" }, { "name": "jack" }
        """.stripMargin

      val result = Source
        .single(ByteString(input))
        .via(JsonFraming.objectScanner(5))
        .map(_.utf8String)
        .runFold(Seq.empty[String]) {
          case (acc, entry) => acc ++ Seq(entry)
        }

      a[FramingException] shouldBe thrownBy {
        Await.result(result, 3.seconds)
      }
    }

    "fail when 2nd object is too large" in {
      val input = List(
        """{ "name": "john" }""",
        """{ "name": "jack" }""",
        """{ "name": "john2" }, { "name": "jack2" }, { "name": "john3" }, { "name": "jack3" }""",
        """{ "name": "very very long name somehow. how did this happen?" }""").map(s => ByteString(s))

      val probe = Source(input).via(JsonFraming.objectScanner(48)).runWith(TestSink())

      probe.ensureSubscription()
      probe
        .requestNext(ByteString("""{ "name": "john" }"""))
        .requestNext(ByteString("""{ "name": "jack" }"""))
        .requestNext(ByteString("""{ "name": "john2" }"""))
        .requestNext(ByteString("""{ "name": "jack2" }"""))
        .requestNext(ByteString("""{ "name": "john3" }"""))
        .requestNext(ByteString("""{ "name": "jack3" }"""))
        .request(1)
        .expectError()
        .getMessage should include("exceeded")
    }

    "fail when completing inside an object" in {
      val input = ByteString("{")
      val probe = Source.single(input).via(JsonFraming.objectScanner(48)).runWith(TestSink())

      probe.ensureSubscription()
      probe.request(1).expectError() shouldBe a[PartialObjectException]
    }

    "fail when pushing and inside an object" in {
      val input = """  { "name": "john" }, {  """
      Source
        .single(ByteString(input))
        .via(JsonFraming.objectScanner(Int.MaxValue))
        .runWith(Sink.last)
        .failed
        .futureValue shouldBe a[PartialObjectException]
    }
  }
}
