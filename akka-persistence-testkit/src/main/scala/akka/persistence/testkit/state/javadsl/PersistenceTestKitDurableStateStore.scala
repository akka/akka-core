/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://akka.io>
 */

package akka.persistence.testkit.state.javadsl

import java.util.Optional
import java.util.concurrent.{ CompletableFuture, CompletionStage }

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

import akka.{ Done, NotUsed }
import akka.japi.Pair
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.{ DurableStateStorePagedPersistenceIdsQuery, DurableStateStoreQuery }
import akka.persistence.query.typed.javadsl.DurableStateStoreBySliceQuery
import akka.persistence.state.javadsl.DurableStateUpdateStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.persistence.testkit.state.scaladsl.{ PersistenceTestKitDurableStateStore => SStore }
import akka.stream.javadsl.Source

object PersistenceTestKitDurableStateStore {
  val Identifier = akka.persistence.testkit.state.scaladsl.PersistenceTestKitDurableStateStore.Identifier
}

class PersistenceTestKitDurableStateStore[A](stateStore: SStore[A])
    extends DurableStateUpdateStore[A]
    with DurableStateStoreQuery[A]
    with DurableStateStoreBySliceQuery[A]
    with DurableStateStorePagedPersistenceIdsQuery[A] {

  def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
    stateStore.getObject(persistenceId).map(_.toJava)(stateStore.system.dispatcher).toJava

  def upsertObject(persistenceId: String, seqNr: Long, value: A, tag: String): CompletionStage[Done] =
    stateStore.upsertObject(persistenceId, seqNr, value, tag).toJava

  def deleteObject(persistenceId: String): CompletionStage[Done] = CompletableFuture.completedFuture(Done)

  def deleteObject(persistenceId: String, revision: Long): CompletionStage[Done] =
    stateStore.deleteObject(persistenceId, revision).toJava

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
    import akka.util.ccompat.JavaConverters._
    stateStore
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def currentPersistenceIds(afterId: Optional[String], limit: Long): Source[String, NotUsed] =
    stateStore.currentPersistenceIds(afterId.asScala, limit).asJava

}
