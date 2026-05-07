/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import scala.concurrent.Promise

import org.reactivestreams._

import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

class FuturePublisherTest extends AkkaPublisherVerification[Int] {

  def createPublisher(elements: Long): Publisher[Int] = {
    val p = Promise[Int]()
    val pub = Source.future(p.future).runWith(Sink.asPublisher(false))
    p.success(0)
    pub
  }

  override def maxElementsFromPublisher(): Long = 1
}
