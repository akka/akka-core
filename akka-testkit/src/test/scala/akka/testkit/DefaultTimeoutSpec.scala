/*
 * Copyright (C) 2013-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import akka.actor.ActorSystem

class DefaultTimeoutSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with TestKitBase with DefaultTimeout {

  implicit lazy val system: ActorSystem = ActorSystem("AkkaCustomSpec")

  override def afterAll() = system.terminate()

  "A spec with DefaultTimeout" should {
    "use timeout from settings" in {
      timeout should ===(testKitSettings.DefaultTimeout)
    }
  }
}
