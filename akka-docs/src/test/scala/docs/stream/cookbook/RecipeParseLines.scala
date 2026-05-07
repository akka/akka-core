/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeParseLines extends RecipeSpec {

  "Recipe for parsing line from bytes" must {

    "work" in {
      val rawData = Source(
        List(
          ByteString("Hello World"),
          ByteString("\r"),
          ByteString("!\r"),
          ByteString("\nHello Akka!\r\nHello Streams!"),
          ByteString("\r\n\r\n")))

      //#parse-lines
      import akka.stream.scaladsl.Framing
      val linesStream = rawData
        .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = 100, allowTruncation = true))
        .map(_.utf8String)
      //#parse-lines

      Await.result(linesStream.limit(10).runWith(Sink.seq), 3.seconds) should be(
        List("Hello World\r!", "Hello Akka!", "Hello Streams!", ""))
    }

  }

}
