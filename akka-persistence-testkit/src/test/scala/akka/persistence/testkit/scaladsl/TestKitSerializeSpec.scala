/*
 * Copyright (C) 2018-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.scaladsl

import akka.actor.Props
import akka.persistence.testkit._

class TestKitSerializeSpec extends CommonTestKitTests {

  override lazy val system = initSystemWithEnabledPlugin("TestKitSerializeSpec", true, true)

  override def specificTests(): Unit = "fail next nonserializable persisted" in {
    val pid = randomPid()
    val a = system.actorOf(Props(classOf[A], pid, None))
    a ! new C

    watch(a)
    expectTerminated(a)
  }
}
