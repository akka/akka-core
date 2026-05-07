/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import java.io.{ File, IOException }

import akka.actor.{ ActorInitializationException, ActorRef, Props }
import akka.testkit.{ AkkaSpec, EventFilter, ImplicitSender }

object SnapshotDirectoryFailureSpec {
  val inUseSnapshotPath = "target/inUseSnapshotPath"

  class TestPersistentActor(name: String, probe: ActorRef) extends PersistentActor with TurnOffRecoverOnStart {

    override def persistenceId: String = name

    override def receiveRecover: Receive = {
      case SnapshotOffer(md, s) => probe ! ((md, s))
    }

    override def receiveCommand = {
      case s: String               => saveSnapshot(s)
      case SaveSnapshotSuccess(md) => probe ! md.sequenceNr
      case other                   => probe ! other
    }
  }
}

class SnapshotDirectoryFailureSpec
    extends AkkaSpec(
      PersistenceSpec.config(
        "inmem",
        "SnapshotDirectoryFailureSpec",
        extraConfig = Some(s"""
  akka.persistence.snapshot-store.local.dir = "${SnapshotDirectoryFailureSpec.inUseSnapshotPath}"
  """)))
    with ImplicitSender {

  import SnapshotDirectoryFailureSpec._

  val file = new File(inUseSnapshotPath)

  override protected def atStartup(): Unit = {
    if (!file.createNewFile()) throw new IOException(s"Failed to create test file [${file.getCanonicalFile}]")
  }

  override protected def afterTermination(): Unit = {
    if (!file.delete()) throw new IOException(s"Failed to delete test file [${file.getCanonicalFile}]")
  }

  "A local snapshot store configured with an failing directory name " must {
    "throw an exception at startup" in {
      EventFilter[ActorInitializationException](occurrences = 1).intercept {
        val p = system.actorOf(Props(classOf[TestPersistentActor], "SnapshotDirectoryFailureSpec-1", testActor))
        p ! "blahonga"
      }
    }
  }
}
