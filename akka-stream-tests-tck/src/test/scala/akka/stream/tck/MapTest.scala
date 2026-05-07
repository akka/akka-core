/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Processor

import akka.stream.scaladsl.Flow

class MapTest extends AkkaIdentityProcessorVerification[Int] {

  override def createIdentityProcessor(maxBufferSize: Int): Processor[Int, Int] = {
    Flow[Int].map(elem => elem).named("identity").toProcessor.run()
  }

  override def createElement(element: Int): Int = element

}
