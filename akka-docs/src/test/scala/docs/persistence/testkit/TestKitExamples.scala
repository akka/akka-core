/*
 * Copyright (C) 2019-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.persistence.testkit

import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.serialization.jackson.CborSerializable
import com.typesafe.config.ConfigFactory
import docs.persistence.testkit.PersistenceTestKitSampleSpec.{ Cmd, Evt, _ }
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

import scala.annotation.nowarn

object PersistenceTestKitSampleSpec {
  final case class Cmd(data: String) extends CborSerializable
  final case class Evt(data: String) extends CborSerializable
  object State {
    val empty: State = new State
  }
  final class State extends CborSerializable {
    def updated(event: Evt): State = this
  }
}

//#test
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.scaladsl.PersistenceTestKit

class PersistenceTestKitSampleSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config.withFallback(ConfigFactory.defaultApplication()))
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  val persistenceTestKit = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    persistenceTestKit.clearAll()
  }

  "Persistent actor" should {

    "persist all events" in {

      val persistenceId = PersistenceId.ofUniqueId("your-persistence-id")
      val persistentActor = spawn(
        EventSourcedBehavior[Cmd, Evt, State](
          persistenceId,
          emptyState = State.empty,
          commandHandler = (_, cmd) => Effect.persist(Evt(cmd.data)),
          eventHandler = (state, evt) => state.updated(evt)))
      val cmd = Cmd("data")

      persistentActor ! cmd

      val expectedPersistedEvent = Evt(cmd.data)
      persistenceTestKit.expectNextPersisted(persistenceId.id, expectedPersistedEvent)
    }

  }
}
//#test

//#set-event-storage-policy
import akka.persistence.testkit._

class SampleEventStoragePolicy extends EventStorage.JournalPolicies.PolicyType {

  //you can use internal state, it does not need to be thread safe
  var count = 1

  override def tryProcess(persistenceId: String, processingUnit: JournalOperation): ProcessingResult =
    if (count < 10) {
      count += 1
      //check the type of operation and react with success or with reject or with failure.
      //if you return ProcessingSuccess the operation will be performed, otherwise not.
      processingUnit match {
        case ReadEvents(batch) if batch.nonEmpty => ProcessingSuccess
        case WriteEvents(batch) if batch.size > 1 =>
          ProcessingSuccess
        case ReadSeqNum      => StorageFailure()
        case DeleteEvents(_) => Reject()
        case _               => StorageFailure()
      }
    } else {
      ProcessingSuccess
    }

}
//#set-event-storage-policy

@nowarn("msg=never used") // sample snippets
//#set-snapshot-storage-policy
class SampleSnapshotStoragePolicy extends SnapshotStorage.SnapshotPolicies.PolicyType {

  //you can use internal state, it does not need to be thread safe
  var count = 1

  override def tryProcess(persistenceId: String, processingUnit: SnapshotOperation): ProcessingResult =
    if (count < 10) {
      count += 1
      //check the type of operation and react with success or with reject or with failure.
      //if you return ProcessingSuccess the operation will be performed, otherwise not.
      processingUnit match {
        case ReadSnapshot(_, payload) if payload.nonEmpty =>
          ProcessingSuccess
        case WriteSnapshot(meta, payload) if meta.sequenceNr > 10 =>
          ProcessingSuccess
        case DeleteSnapshotsByCriteria(_) => StorageFailure()
        case DeleteSnapshotByMeta(meta) if meta.sequenceNr < 10 =>
          ProcessingSuccess
        case _ => StorageFailure()
      }
    } else {
      ProcessingSuccess
    }
}
//#set-snapshot-storage-policy

//#policy-test
class PersistenceTestKitSampleSpecWithPolicy
    extends ScalaTestWithActorTestKit(PersistenceTestKitPlugin.config.withFallback(ConfigFactory.defaultApplication()))
    with AnyWordSpecLike
    with BeforeAndAfterEach {

  val persistenceTestKit = PersistenceTestKit(system)

  override def beforeEach(): Unit = {
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
  }

  "Testkit policy" should {

    "fail all operations with custom exception" in {
      val policy = new EventStorage.JournalPolicies.PolicyType {

        class CustomFailure extends RuntimeException

        override def tryProcess(persistenceId: String, processingUnit: JournalOperation): ProcessingResult =
          processingUnit match {
            case WriteEvents(_) => StorageFailure(new CustomFailure)
            case _              => ProcessingSuccess
          }
      }
      persistenceTestKit.withPolicy(policy)

      val persistenceId = PersistenceId.ofUniqueId("your-persistence-id")
      val persistentActor = spawn(
        EventSourcedBehavior[Cmd, Evt, State](
          persistenceId,
          emptyState = State.empty,
          commandHandler = (_, cmd) => Effect.persist(Evt(cmd.data)),
          eventHandler = (state, evt) => state.updated(evt)))

      persistentActor ! Cmd("data")
      persistenceTestKit.expectNothingPersisted(persistenceId.id)

    }
  }
}
//#policy-test
