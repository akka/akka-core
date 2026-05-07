/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit

import java.util.UUID

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.{ ActorRef, ActorSystem }
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.persistence._
import akka.testkit.TestKitBase

trait CommonUtils extends AnyWordSpecLike with TestKitBase with LogCapturing {

  protected def randomPid() = UUID.randomUUID().toString

  import scala.jdk.CollectionConverters._

  def initSystemWithEnabledPlugin(name: String, serializeMessages: Boolean, serializeSnapshots: Boolean) =
    ActorSystem(
      s"persistence-testkit-$name",
      PersistenceTestKitSnapshotPlugin.config
        .withFallback(PersistenceTestKitPlugin.config)
        .withFallback(
          ConfigFactory.parseMap(
            Map(
              // testing serialization of the events when persisting in the storage
              // using default java serializers for convenience
              "akka.actor.allow-java-serialization" -> true,
              "akka.persistence.testkit.events.serialize" -> serializeMessages,
              "akka.persistence.testkit.snapshots.serialize" -> serializeSnapshots).asJava))
        .withFallback(ConfigFactory.parseString("akka.loggers = [\"akka.testkit.TestEventListener\"]"))
        .withFallback(ConfigFactory.defaultApplication()))

}

case class NewSnapshot(state: Any)
case object DeleteAllMessages
case class DeleteSomeSnapshot(seqNum: Long)
case class DeleteSomeSnapshotByCriteria(crit: SnapshotSelectionCriteria)
case object AskMessageSeqNum
case object AskSnapshotSeqNum
case class DeleteSomeMessages(upToSeqNum: Long)

class C

case class B(i: Int)

class A(pid: String, notifyOnStateChange: Option[ActorRef]) extends PersistentActor {

  import scala.collection.immutable

  var recovered = immutable.List.empty[Any]
  var snapshotState = 0

  override def receiveRecover = {
    case SnapshotOffer(_, snapshot: Int) =>
      snapshotState = snapshot
    case RecoveryCompleted =>
      notifyOnStateChange.foreach(_ ! Tuple2(recovered, snapshotState))
    case s => recovered :+= s
  }

  override def receiveCommand = {
    case AskMessageSeqNum =>
      notifyOnStateChange.foreach(_ ! lastSequenceNr)
    case AskSnapshotSeqNum =>
      notifyOnStateChange.foreach(_ ! snapshotSequenceNr)
    case d @ DeleteMessagesFailure(_, _) =>
      notifyOnStateChange.foreach(_ ! d)
    case d @ DeleteMessagesSuccess(_) =>
      notifyOnStateChange.foreach(_ ! d)
    case s: SnapshotProtocol.Response =>
      notifyOnStateChange.foreach(_ ! s)
    case DeleteAllMessages =>
      deleteMessages(lastSequenceNr)
    case DeleteSomeSnapshot(sn) =>
      deleteSnapshot(sn)
    case DeleteSomeSnapshotByCriteria(crit) =>
      deleteSnapshots(crit)
    case DeleteSomeMessages(sn) =>
      deleteMessages(sn)
    case NewSnapshot(state: Int) =>
      snapshotState = state: Int
      saveSnapshot(state)
    case NewSnapshot(other) =>
      saveSnapshot(other)
    case s =>
      persist(s) { _ =>
        sender() ! s
      }
  }

  override def persistenceId = pid
}

sealed trait TestCommand
case class Cmd(data: String) extends TestCommand
case object Passivate extends TestCommand
case class Evt(data: String)
case class EmptyState()
case class NonEmptyState(data: String)
case object Recovered
case object Stopped
