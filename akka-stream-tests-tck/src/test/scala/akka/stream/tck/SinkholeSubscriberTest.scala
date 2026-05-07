/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.tck

import java.lang.{ Integer => JInt }

import scala.concurrent.Promise

import org.reactivestreams.{ Subscriber, Subscription }
import org.reactivestreams.tck.{ SubscriberWhiteboxVerification, TestEnvironment }
import org.reactivestreams.tck.SubscriberWhiteboxVerification.{ SubscriberPuppet, WhiteboxSubscriberProbe }
import org.scalatestplus.testng.TestNGSuiteLike

import akka.Done
import akka.stream.impl.SinkholeSubscriber

class SinkholeSubscriberTest extends SubscriberWhiteboxVerification[JInt](new TestEnvironment()) with TestNGSuiteLike {
  override def createSubscriber(probe: WhiteboxSubscriberProbe[JInt]): Subscriber[JInt] = {
    new Subscriber[JInt] {
      val hole = new SinkholeSubscriber[JInt](Promise[Done]())

      override def onError(t: Throwable): Unit = {
        hole.onError(t)
        probe.registerOnError(t)
      }

      override def onSubscribe(s: Subscription): Unit = {
        probe.registerOnSubscribe(new SubscriberPuppet() {
          override def triggerRequest(elements: Long): Unit = s.request(elements)
          override def signalCancel(): Unit = s.cancel()
        })
        hole.onSubscribe(s)
      }

      override def onComplete(): Unit = {
        hole.onComplete()
        probe.registerOnComplete()
      }

      override def onNext(t: JInt): Unit = {
        hole.onNext(t)
        probe.registerOnNext(t)
      }
    }
  }

  override def createElement(element: Int): JInt = element
}
