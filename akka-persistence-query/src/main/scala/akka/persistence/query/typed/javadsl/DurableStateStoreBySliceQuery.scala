/*
 * Copyright (C) 2021-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.query.typed.javadsl

import akka.NotUsed
import akka.japi.Pair
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.state.javadsl.DurableStateStore
import akka.stream.javadsl.Source

/**
 * Query API for reading durable state objects.
 *
 * For Scala API see [[DurableStateStoreBySliceQuery]].
 */
trait DurableStateStoreBySliceQuery[A] extends DurableStateStore[A] {

  /**
   * Get a source of the most recent changes made to objects with the given slice range since the passed in offset.
   *
   * A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
   * persistence ids over the slices.
   *
   * Note that this only returns the most recent change to each object, if an object has been updated multiple times
   * since the offset, only the most recent of those changes will be part of the stream.
   *
   * This will return changes that occurred up to when the `Source` returned by this call is materialized. Changes to
   * objects made since materialization are not guaranteed to be included in the results.
   *
   * The [[DurableStateChange]] elements can be [[akka.persistence.query.UpdatedDurableState]] or
   * [[akka.persistence.query.DeletedDurableState]].
   */
  def currentChangesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed]

  /**
   * Get a source of the most recent changes made to objects of the given slice range since the passed in offset.
   *
   * A slice is deterministically defined based on the persistence id. The purpose is to evenly distribute all
   * persistence ids over the slices.
   *
   * The returned source will never terminate, it effectively watches for changes to the objects and emits changes as
   * they happen.
   *
   * Not all changes that occur are guaranteed to be emitted, this call only guarantees that eventually, the most recent
   * change for each object since the offset will be emitted. In particular, multiple updates to a given object in quick
   * succession are likely to be skipped, with only the last update resulting in a change from this source.
   *
   * The [[DurableStateChange]] elements can be [[akka.persistence.query.UpdatedDurableState]] or
   * [[akka.persistence.query.DeletedDurableState]].
   */
  def changesBySlices(
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[DurableStateChange[A], NotUsed]

  def sliceForPersistenceId(persistenceId: String): Int

  def sliceRanges(numberOfRanges: Int): java.util.List[Pair[Integer, Integer]]

}
