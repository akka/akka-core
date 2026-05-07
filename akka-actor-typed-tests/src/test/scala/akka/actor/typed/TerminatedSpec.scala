/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.typed

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.TestInbox

class TerminatedSpec extends AnyWordSpec with Matchers with LogCapturing {

  "Child Failed" must {
    "should be pattern matchable" in {

      val probe = TestInbox[String]()
      val ex = new RuntimeException("oh dear")
      val childFailed = new ChildFailed(probe.ref, ex)

      (childFailed match {
        case Terminated(r) => r
        case unexpected    => throw new RuntimeException(s"Unexpected: $unexpected")
      }) shouldEqual probe.ref

      (childFailed match {
        case ChildFailed(ref, e) => (ref, e)
        case unexpected          => throw new RuntimeException(s"Unexpected: $unexpected")
      }) shouldEqual ((probe.ref, ex))

    }

  }

}
