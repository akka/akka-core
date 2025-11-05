/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.cookbook

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration._

class RecipeDecompress extends RecipeSpec {
  "Recipe for decompressing a Gzip stream" must {
    "work" in {
      //#decompress-gzip
      import akka.stream.scaladsl.Compression
      //#decompress-gzip

      val compressed =
        Source.single(ByteString.fromString("Hello World")).via(Compression.gzip)

      //#decompress-gzip
      val uncompressed = compressed.via(Compression.gunzip()).map(_.utf8String)
      //#decompress-gzip

      Await.result(uncompressed.runWith(Sink.head), 3.seconds) should be("Hello World")
    }
  }
}
