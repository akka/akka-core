/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.event

import akka.event.Logging._
import akka.testkit._

class MarkerLoggingSpec extends AkkaSpec with ImplicitSender {
  "A MarkerLoggerAdapter" should {
    val markerLogging = new MarkerLoggingAdapter(
      system.eventStream,
      getClass.getName,
      this.getClass,
      new DefaultLoggingFilter(() => Logging.InfoLevel))

    "add markers to logging" in {
      system.eventStream.subscribe(self, classOf[Info])
      system.eventStream.publish(TestEvent.Mute(EventFilter.info()))
      markerLogging.info(LogMarker.Security, "This is a security problem")

      val info = expectMsgType[Info3]
      info.message should be("This is a security problem")
      info.marker.name should be("SECURITY")
    }

    "allow cause exceptions in error messages" in {
      class MyException(message: String) extends Exception(message)
      system.eventStream.subscribe(self, classOf[Error])
      system.eventStream.publish(TestEvent.Mute(EventFilter[MyException]("this is a security crash")))

      markerLogging.error(LogMarker.Security, new MyException("Security Exception"), "this is a security crash")

      expectMsgType[Error].cause.getMessage should be("Security Exception")
    }
  }
}
