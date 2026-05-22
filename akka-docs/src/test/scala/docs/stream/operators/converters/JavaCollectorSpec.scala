/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators.converters

import java.util.stream.Collectors

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters
import akka.testkit.AkkaSpec
import org.scalatest.concurrent.Futures

import scala.jdk.CollectionConverters._
import scala.concurrent.Future

class JavaCollectorSpec extends AkkaSpec with Futures {

  "demonstrate javaCollector" in {
    // #javaCollector
    val source: Source[String, NotUsed] = Source(List("one", "two", "three"))

    val sink: Sink[String, Future[java.util.List[String]]] =
      StreamConverters.javaCollector(() => Collectors.toList[String]())

    val result: Future[java.util.List[String]] = source.runWith(sink)
    // #javaCollector
    whenReady(result) { r =>
      r.asScala should contain theSameElementsInOrderAs List("one", "two", "three")
    }
  }
}
