/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import scala.concurrent.{ Await, Promise }
import scala.concurrent.duration._

import org.reactivestreams.{ Publisher, Subscriber }

import akka.stream.scaladsl.{ Sink, Source }

class FlatMapConcatDoubleSubscriberTest extends AkkaSubscriberBlackboxVerification[Int] {

  def createSubscriber(): Subscriber[Int] = {
    val subscriber = Promise[Subscriber[Int]]()
    Source
      .single(Source.fromPublisher(new Publisher[Int] {
        def subscribe(s: Subscriber[_ >: Int]): Unit =
          subscriber.success(s.asInstanceOf[Subscriber[Int]])
      }))
      .flatMapConcat(identity)
      .runWith(Sink.ignore)

    Await.result(subscriber.future, 1.second)
  }

  def createElement(element: Int): Int = element
}
