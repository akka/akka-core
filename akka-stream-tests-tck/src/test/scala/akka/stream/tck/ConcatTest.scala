/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Publisher

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

class ConcatTest extends AkkaPublisherVerification[Int] {

  def createPublisher(elements: Long): Publisher[Int] = {
    Source(iterable(elements / 2)).concat(Source(iterable((elements + 1) / 2))).runWith(Sink.asPublisher(false))
  }

}
