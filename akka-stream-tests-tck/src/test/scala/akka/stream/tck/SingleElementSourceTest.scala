/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Publisher

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

class SingleElementSourceTest extends AkkaPublisherVerification[Int] {

  def createPublisher(elements: Long): Publisher[Int] =
    Source.single(1).runWith(Sink.asPublisher(false))

  override def maxElementsFromPublisher(): Long = 1
}
