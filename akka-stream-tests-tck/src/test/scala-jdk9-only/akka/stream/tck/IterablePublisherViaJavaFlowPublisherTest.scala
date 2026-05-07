/*
 * Copyright (C) 2017-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import java.util.concurrent.{ Flow => JavaFlow }

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, JavaFlowSupport, Sink, Source }
import org.reactivestreams._

class IterablePublisherViaJavaFlowPublisherTest extends AkkaPublisherVerification[Int] {

  override def createPublisher(elements: Long): Publisher[Int] = {
    val sourceViaJavaFlowPublisher: JavaFlow.Publisher[Int] = Source(iterable(elements))
      .runWith(JavaFlowSupport.Sink.asPublisher(fanout = false))

    val javaFlowPublisherIntoAkkaSource: Source[Int, NotUsed] =
      JavaFlowSupport.Source.fromPublisher(sourceViaJavaFlowPublisher)

    javaFlowPublisherIntoAkkaSource
      .runWith(Sink.asPublisher(false)) // back as RS Publisher
  }

}
