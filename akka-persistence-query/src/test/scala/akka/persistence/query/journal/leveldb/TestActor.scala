/*
 * Copyright (C) 2015-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.journal.leveldb

import akka.actor.Props
import akka.persistence.PersistentActor

object TestActor {
  def props(persistenceId: String): Props =
    Props(new TestActor(persistenceId))

  case class DeleteCmd(toSeqNr: Long = Long.MaxValue)
}

class TestActor(override val persistenceId: String) extends PersistentActor {

  import TestActor.DeleteCmd

  val receiveRecover: Receive = {
    case _: String =>
  }

  val receiveCommand: Receive = {
    case DeleteCmd(toSeqNr) =>
      deleteMessages(toSeqNr)
      sender() ! s"$toSeqNr-deleted"

    case cmd: String =>
      persist(cmd) { evt =>
        sender() ! s"$evt-done"
      }
  }

}
