/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.javadsl

import java.io.NotSerializableException

import akka.actor.Props
import akka.persistence.SaveSnapshotFailure
import akka.persistence.testkit._

class SnapshotSerializeSpec extends CommonSnapshotTests {

  override lazy val system = initSystemWithEnabledPlugin("SnapshotSerializeSpec", true, true)

  override def specificTests(): Unit =
    "fail if tries to save nonserializable snapshot" in {
      val pid = randomPid()
      val a = system.actorOf(Props(classOf[A], pid, Some(testActor)))
      a ! NewSnapshot(new C)

      expectMsg((List.empty, 0L))
      expectMsgPF() { case SaveSnapshotFailure(_, _: NotSerializableException) => }
    }

}
