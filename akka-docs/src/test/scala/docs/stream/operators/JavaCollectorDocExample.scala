/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.stream.operators

import java.util.stream.Collectors

import akka.NotUsed
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.StreamConverters

object JavaCollectorDocExample {

  // #javaCollector
  val source: Source[String, NotUsed] = Source(List("one", "two", "three"))

  val sink: Sink[String, _] =
    StreamConverters.javaCollector(() => Collectors.toList[String]())
  // #javaCollector
}
