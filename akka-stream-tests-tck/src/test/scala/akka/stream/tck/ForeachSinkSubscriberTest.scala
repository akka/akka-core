/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Subscriber

import akka.stream.scaladsl._

class ForeachSinkSubscriberTest extends AkkaSubscriberBlackboxVerification[Int] {

  override def createSubscriber(): Subscriber[Int] =
    Flow[Int]
      .to(Sink.foreach { _ =>
      })
      .runWith(Source.asSubscriber)

  override def createElement(element: Int): Int = element
}
