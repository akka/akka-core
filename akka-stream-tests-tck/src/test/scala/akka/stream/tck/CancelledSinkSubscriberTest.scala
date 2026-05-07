/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import org.reactivestreams.Subscriber
import org.testng.SkipException

import akka.stream.scaladsl._

class CancelledSinkSubscriberTest extends AkkaSubscriberBlackboxVerification[Int] {

  override def createSubscriber(): Subscriber[Int] =
    Flow[Int].to(Sink.cancelled).runWith(Source.asSubscriber)

  override def createElement(element: Int): Int = element

  override def required_spec201_blackbox_mustSignalDemandViaSubscriptionRequest() = {
    throw new SkipException("Cancelled sink doesn't signal demand")
  }
  override def required_spec209_blackbox_mustBePreparedToReceiveAnOnCompleteSignalWithPrecedingRequestCall() = {
    throw new SkipException("Cancelled sink doesn't signal demand")
  }
  override def required_spec210_blackbox_mustBePreparedToReceiveAnOnErrorSignalWithPrecedingRequestCall() = {
    throw new SkipException("Cancelled sink doesn't signal demand")
  }
}
