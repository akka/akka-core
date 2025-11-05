/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream

import akka.dispatch.Dispatchers
import akka.stream.testkit.StreamSpec

class StreamDispatcherSpec extends StreamSpec {

  "The default blocking io dispatcher for streams" must {

    "be the same as the default blocking io dispatcher for actors" in {
      val streamIoDispatcher = system.dispatchers.lookup(ActorAttributes.IODispatcher.dispatcher)
      val actorIoDispatcher = system.dispatchers.lookup(Dispatchers.DefaultBlockingDispatcherId)

      streamIoDispatcher shouldBe theSameInstanceAs(actorIoDispatcher)
    }

  }

  "The deprecated default stream io dispatcher" must {
    "be the same as the default blocking io dispatcher for actors" in {
      // in case it is still used
      val streamIoDispatcher = system.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")
      val actorIoDispatcher = system.dispatchers.lookup(Dispatchers.DefaultBlockingDispatcherId)

      streamIoDispatcher shouldBe theSameInstanceAs(actorIoDispatcher)
    }

  }
}
