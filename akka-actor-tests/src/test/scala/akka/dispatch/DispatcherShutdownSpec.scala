/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.dispatch

import java.lang.management.ManagementFactory

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem
import akka.testkit.TestKit

class DispatcherShutdownSpec extends AnyWordSpec with Matchers {

  "akka dispatcher" should {

    "eventually shutdown when used after system terminate" in {

      val threads = ManagementFactory.getThreadMXBean()
      def threadCount =
        threads
          .dumpAllThreads(false, false)
          .toList
          .map(_.getThreadName)
          .filter(name =>
            name.startsWith("DispatcherShutdownSpec-akka.actor.default") || name.startsWith(
              "DispatcherShutdownSpec-akka.actor.internal")) // nothing is run on default without any user actors started
          .size

      val system = ActorSystem("DispatcherShutdownSpec")
      threadCount should be > 0

      Await.ready(system.terminate(), 1.second)
      Await.ready(Future(akka.Done)(system.dispatcher), 1.second)

      TestKit.awaitCond(threadCount == 0, 3.second)
    }

  }

}
