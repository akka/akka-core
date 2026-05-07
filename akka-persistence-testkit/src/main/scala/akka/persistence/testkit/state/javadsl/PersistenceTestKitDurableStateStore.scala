/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.testkit.state.javadsl

import java.util.Optional
import java.util.concurrent.{ CompletableFuture, CompletionStage }

import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.{ Done, NotUsed }
import akka.japi.Pair
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.{ DurableStateStorePagedPersistenceIdsQuery, DurableStateStoreQuery }
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.CurrentEventsBySliceQuery
import akka.persistence.query.typed.javadsl.DurableStateStoreBySliceQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.state.javadsl.DurableStateUpdateWithChangeEventStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.persistence.testkit.state.scaladsl.{ PersistenceTestKitDurableStateStore => SStore }
import akka.stream.javadsl.Source

object PersistenceTestKitDurableStateStore {
  val Identifier = akka.persistence.testkit.state.scaladsl.PersistenceTestKitDurableStateStore.Identifier
}

class PersistenceTestKitDurableStateStore[A](stateStore: SStore[A])
    extends DurableStateUpdateWithChangeEventStore[A]
    with DurableStateStoreQuery[A]
    with DurableStateStoreBySliceQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A]
    with CurrentEventsBySliceQuery
    with EventsBySliceQuery {

  def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    stateStore.getObject(persistenceId).map(_.toJava)(stateStore.system.dispatcher).asJava

  def upsertObject(persistenceId: String, seqNr: Long, value: A, tag: String): CompletionStage[Done] =
    stateStore.upsertObject(persistenceId, seqNr, value, tag).asJava

  def upsertObject(persistenceId: String, seqNr: Long, value: A, tag: String, changeEvent: Any): CompletionStage[Done] =
    stateStore.upsertObject(persistenceId, seqNr, value, tag, changeEvent).asJava

  def deleteObject(persistenceId: String): CompletionStage[Done] = CompletableFuture.completedFuture(Done)

  def deleteObject(persistenceId: String, revision: Long): CompletionStage[Done] =
    stateStore.deleteObject(persistenceId, revision).asJava

  def deleteObject(persistenceId: String, revision: Long, changeEvent: Any): CompletionStage[Done] =
    stateStore.deleteObject(persistenceId, revision, changeEvent).asJava

  def changes(tag: String, offset: Offset): Source[DurableStateChange[A], akka.NotUsed] = {
    stateStore.changes(tag, offset).asJava
  }
  def currentChanges(tag: String, offset: Offset): Source[DurableStateChange[A], akka.NotUsed] = {
    stateStore.currentChanges(tag, offset).asJava
  }

  override def currentChangesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    stateStore.currentChangesBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def changesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed] =
    stateStore.changesBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceForPersistenceId(persistenceId: String): Int =
    stateStore.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]] = {
    import scala.jdk.CollectionConverters._
    stateStore
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] =
    stateStore.currentPersistenceIds(afterId.toScala, limit).asJava

  /**
   * For change events.
   */
  override def currentEventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    stateStore.currentEventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  /**
   * For change events.
   */
  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    stateStore.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava
}
