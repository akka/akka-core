/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.remote.artery

import org.scalatest.matchers.should.Matchers

import akka.testkit.AkkaSpec
import akka.util.JavaVersion

class RemotingFlightRecorderSpec extends AkkaSpec with Matchers {

  "The RemotingFlightRecorder" must {

    "use the no-op recorder by default when running on JDK 8" in {
      val extension = RemotingFlightRecorder(system)
      if (JavaVersion.majorVersion < 11)
        extension should ===(NoOpRemotingFlightRecorder)
    }
  }

}
