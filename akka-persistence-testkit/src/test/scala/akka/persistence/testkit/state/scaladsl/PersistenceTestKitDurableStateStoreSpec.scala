/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.state.scaladsl

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.DurableStateChange
import akka.persistence.query.NoOffset
import akka.persistence.query.Sequence
import akka.persistence.query.UpdatedDurableState
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.stream.scaladsl.Sink
import akka.stream.testkit.scaladsl.TestSink

object PersistenceTestKitDurableStateStoreSpec {
  val config =
    PersistenceTestKitDurableStateStorePlugin.config.withFallback(ConfigFactory.parseString("""
    akka.loglevel = DEBUG
      """))
  case class Record(id: Int, name: String)
}

class PersistenceTestKitDurableStateStoreSpec
    extends ScalaTestWithActorTestKit(PersistenceTestKitDurableStateStoreSpec.config)
    with LogCapturing
    with AnyWordSpecLike {

  import PersistenceTestKitDurableStateStoreSpec._

  implicit val classic: akka.actor.ActorSystem = system.classicSystem

  "Persistent test kit state store" must {
    "find individual objects" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val tag = "tag-1"
      val persistenceId = "record-1"
      stateStore.upsertObject(persistenceId, 1L, record, tag).futureValue
      val updated = stateStore.getObject(persistenceId).futureValue
      updated.value should be(Some(record))
      updated.revision should be(1L)
    }

    "find tagged state changes ordered by upsert" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val recordChange = Record(1, "my-name-1")
      val tag = "tag-1"
      stateStore.upsertObject("record-1", 1L, record, tag)
      val testSink = stateStore
        .changes(tag, NoOffset)
        .collect { case u: UpdatedDurableState[Record] => u }
        .runWith(TestSink[UpdatedDurableState[Record]]())

      val firstStateChange = testSink.request(1).expectNext()
      firstStateChange.value should be(record)
      firstStateChange.revision should be(1L)

      stateStore.upsertObject("record-1", 2L, recordChange, tag)
      val secondStateChange = testSink.request(1).expectNext()
      secondStateChange.value should be(recordChange)
      secondStateChange.revision should be(2L)
      secondStateChange.offset
        .asInstanceOf[Sequence]
        .value should be >= (firstStateChange.offset.asInstanceOf[Sequence].value)
    }

    "find tagged state changes for deleted object" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val tag = "tag-1"
      val persistenceId = "record-1"
      val testSink = stateStore.changes(tag, NoOffset).runWith(TestSink[DurableStateChange[Record]]())

      stateStore.upsertObject(persistenceId, 1L, record, tag)
      val updatedDurableState = testSink.request(1).expectNext().asInstanceOf[UpdatedDurableState[Record]]
      updatedDurableState.persistenceId should be(persistenceId)
      updatedDurableState.value should be(record)
      updatedDurableState.revision should be(1L)

      stateStore.deleteObject(persistenceId, 2L)
      val deletedDurableState = testSink.request(1).expectNext().asInstanceOf[DeletedDurableState[Record]]
      deletedDurableState.persistenceId should be(persistenceId)
      deletedDurableState.revision should be(2L)
      deletedDurableState.timestamp should be >= updatedDurableState.timestamp
    }

    "find tagged current state changes ordered by upsert" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val tag = "tag-1"

      stateStore.upsertObject("record-1", 1L, record, tag)

      val testSinkCurrentChanges =
        stateStore
          .currentChanges(tag, NoOffset)
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())

      stateStore.upsertObject("record-1", 2L, record.copy(name = "my-name-1-2"), tag)
      stateStore.upsertObject("record-1", 3L, record.copy(name = "my-name-1-3"), tag)

      val currentStateChange = testSinkCurrentChanges.request(1).expectNext()

      currentStateChange.value should be(record)
      currentStateChange.revision should be(1L)
      testSinkCurrentChanges.request(1).expectComplete()

      val testSinkIllegalOffset =
        stateStore
          .currentChanges(tag, Sequence(100L))
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())
      testSinkIllegalOffset.request(1).expectNoMessage()
    }

    "return current changes when there are no further changes" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val tag = "tag-1"

      stateStore.upsertObject("record-1", 1L, record, tag)

      val testSinkCurrentChanges =
        stateStore
          .currentChanges(tag, NoOffset)
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())

      val currentStateChange = testSinkCurrentChanges.request(1).expectNext()

      currentStateChange.value should be(record)
      currentStateChange.revision should be(1L)
      testSinkCurrentChanges.request(1).expectComplete()
    }

    "find state changes by slice ordered by upsert" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val recordChange = Record(1, "my-name-1")
      stateStore.upsertObject("Test|record-1", 1L, record, "")
      val maxSlice = stateStore.sliceRanges(1).head.max
      val testSink = stateStore
        .changesBySlices("Test", 0, maxSlice, NoOffset)
        .collect { case u: UpdatedDurableState[Record] => u }
        .runWith(TestSink[UpdatedDurableState[Record]]())

      val firstStateChange = testSink.request(1).expectNext()
      firstStateChange.value should be(record)
      firstStateChange.revision should be(1L)

      stateStore.upsertObject("Test|record-1", 2L, recordChange, "")
      val secondStateChange = testSink.request(1).expectNext()
      secondStateChange.value should be(recordChange)
      secondStateChange.revision should be(2L)
      secondStateChange.offset
        .asInstanceOf[Sequence]
        .value should be >= (firstStateChange.offset.asInstanceOf[Sequence].value)
    }

    "find current state changes by slice ordered by upsert" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")

      stateStore.upsertObject("Test|record-1", 1L, record, "")

      val maxSlice = stateStore.sliceRanges(1).head.max
      val testSinkCurrentChanges =
        stateStore
          .currentChangesBySlices("Test", 0, maxSlice, NoOffset)
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())

      stateStore.upsertObject("record-1", 2L, record.copy(name = "my-name-1-2"), "")
      stateStore.upsertObject("record-1", 3L, record.copy(name = "my-name-1-3"), "")

      val currentStateChange = testSinkCurrentChanges.request(1).expectNext()

      currentStateChange.value should be(record)
      currentStateChange.revision should be(1L)
      testSinkCurrentChanges.request(1).expectComplete()

      val testSinkIllegalOffset =
        stateStore
          .currentChangesBySlices("Test", 0, maxSlice, Sequence(100L))
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())
      testSinkIllegalOffset.request(1).expectNoMessage()
    }

    "return current changes by slice when there are no further changes" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])
      val record = Record(1, "name-1")
      val tag = "tag-1"

      stateStore.upsertObject("Test|record-1", 1L, record, tag)

      val maxSlice = stateStore.sliceRanges(1).head.max
      val testSinkCurrentChanges =
        stateStore
          .currentChangesBySlices("Test", 0, maxSlice, NoOffset)
          .collect { case u: UpdatedDurableState[Record] => u }
          .runWith(TestSink[UpdatedDurableState[Record]]())

      val currentStateChange = testSinkCurrentChanges.request(1).expectNext()

      currentStateChange.value should be(record)
      currentStateChange.revision should be(1L)
      testSinkCurrentChanges.request(1).expectComplete()
    }

    "return all current persistence ids" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])

      val record = Record(1, "name-1")
      val tag = "tag-1"
      val ids = (1 to 20).map(i => s"id-$i")
      for (id <- ids) {
        stateStore.upsertObject(id, 1, record, tag)
      }

      val resultIds = stateStore.currentPersistenceIds(None, Long.MaxValue).runWith(Sink.seq).futureValue
      resultIds should have size 20
      resultIds should contain allElementsOf ids
    }

    "return paged current persistence ids" in {
      val stateStore = new PersistenceTestKitDurableStateStore[Record](classic.asInstanceOf[ExtendedActorSystem])

      val record = Record(1, "name-1")
      val tag = "tag-1"
      val ids = (1 to 20).map(i => s"id-$i")
      for (id <- ids) {
        stateStore.upsertObject(id, 1, record, tag)
      }

      val page1 = stateStore.currentPersistenceIds(None, 9).runWith(Sink.seq).futureValue
      page1 should have size 9
      val page2 = stateStore.currentPersistenceIds(page1.lastOption, 9).runWith(Sink.seq).futureValue
      page2 should have size 9
      val page3 = stateStore.currentPersistenceIds(page2.lastOption, 9).runWith(Sink.seq).futureValue
      page3 should have size 2

      (page1 ++ page2 ++ page3) should contain allElementsOf ids
    }
  }
}
