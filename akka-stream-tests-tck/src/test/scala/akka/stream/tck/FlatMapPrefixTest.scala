/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Publisher

import akka.stream.scaladsl.{ Flow, Keep, Sink, Source }

class FlatMapPrefixTest extends AkkaPublisherVerification[Int] {
  override def createPublisher(elements: Long): Publisher[Int] = {
    val publisher = Source(iterable(elements))
      .map(_.toInt)
      .flatMapPrefixMat(1) { seq =>
        Flow[Int].prepend(Source(seq))
      }(Keep.left)
      .runWith(Sink.asPublisher(false))
    publisher
  }
}
