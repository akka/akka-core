/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster

import scala.collection.immutable

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
private[cluster] object Reachability {
  val empty = new Reachability(Vector.empty, Map.empty)

  def apply(records: immutable.IndexedSeq[Record], versions: Map[UniqueAddress, Long]): Reachability =
    new Reachability(records, versions)

  def create(records: immutable.Seq[Record], versions: Map[UniqueAddress, Long]): Reachability = records match {
    case r: immutable.IndexedSeq[Record] => apply(r, versions)
    case _                               => apply(records.toVector, versions)
  }

  @SerialVersionUID(1L)
  final case class Record(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus, version: Long)

  sealed trait ReachabilityStatus
  @SerialVersionUID(1L) case object Reachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Unreachable extends ReachabilityStatus
  @SerialVersionUID(1L) case object Terminated extends ReachabilityStatus

}

/**
 * INTERNAL API
 *
 * Immutable data structure that holds the reachability status of subject nodes as seen
 * from observer nodes. Failure detector for the subject nodes exist on the
 * observer nodes. Changes (reachable, unreachable, terminated) are only performed
 * by observer nodes to its own records. Each change bumps the version number of the
 * record, and thereby it is always possible to determine which record is newest when
 * merging two instances.
 *
 * By default, each observer treats every other node as reachable. That allows to
 * introduce the invariant that if an observer sees all nodes as reachable, no
 * records should be kept at all. Therefore, in a running cluster with full
 * reachability, no records need to be kept at all.
 *
 * Aggregated status of a subject node is defined as (in this order):
 * - Terminated if any observer node considers it as Terminated
 * - Unreachable if any observer node considers it as Unreachable
 * - Reachable otherwise, i.e. no observer node considers it as Unreachable
 */
@SerialVersionUID(1L)
@InternalApi
private[cluster] class Reachability private (
    val records: immutable.IndexedSeq[Reachability.Record],
    val versions: Map[UniqueAddress, Long])
    extends Serializable {

  import Reachability._

  private class Cache {
    // `allUnreachable` contains all nodes that have been observed as Unreachable by at least one other node
    // `allTerminated` contains all nodes that have been observed as Terminated by at least one other node
    val (observerRowsMap, allUnreachable, allTerminated) = {
      if (records.isEmpty) {
        val observerRowsMap = Map.empty[UniqueAddress, Map[UniqueAddress, Reachability.Record]]
        val allTerminated = Set.empty[UniqueAddress]
        val allUnreachable = Set.empty[UniqueAddress]
        (observerRowsMap, allUnreachable, allTerminated)
      } else {
        val mapBuilder = scala.collection.mutable.Map.empty[UniqueAddress, Map[UniqueAddress, Reachability.Record]]
        var allTerminated = Set.empty[UniqueAddress]
        var allUnreachable = Set.empty[UniqueAddress]

        records.foreach { r =>
          val m = mapBuilder.get(r.observer) match {
            case None    => Map(r.subject -> r)
            case Some(m) => m.updated(r.subject, r)
          }
          mapBuilder += (r.observer -> m)

          if (r.status == Unreachable) allUnreachable += r.subject
          else if (r.status == Terminated) allTerminated += r.subject
        }

        val observerRowsMap: Map[UniqueAddress, Map[UniqueAddress, Reachability.Record]] = mapBuilder.toMap

        (observerRowsMap, allUnreachable.diff(allTerminated), allTerminated)
      }
    }

    val allUnreachableOrTerminated: Set[UniqueAddress] =
      if (allTerminated.isEmpty) allUnreachable
      else allUnreachable.union(allTerminated)

  }

  @transient private lazy val cache = new Cache

  private def observerRows(observer: UniqueAddress): Option[Map[UniqueAddress, Reachability.Record]] =
    cache.observerRowsMap.get(observer)

  def unreachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Unreachable)

  def reachable(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Reachable)

  def terminated(observer: UniqueAddress, subject: UniqueAddress): Reachability =
    change(observer, subject, Terminated)

  private def currentVersion(observer: UniqueAddress): Long = versions.get(observer) match {
    case None    => 0
    case Some(v) => v
  }

  private def nextVersion(observer: UniqueAddress): Long = currentVersion(observer) + 1

  private def change(observer: UniqueAddress, subject: UniqueAddress, status: ReachabilityStatus): Reachability = {
    val v = nextVersion(observer)
    val newVersions = versions.updated(observer, v)
    val newRecord = Record(observer, subject, status, v)
    observerRows(observer) match {
      // don't record Reachable observation if nothing has been noted so far
      case None if status == Reachable => this
      // otherwise, create new instance including this first observation
      case None =>
        new Reachability(records :+ newRecord, newVersions)

      // otherwise, update old observations
      case Some(oldObserverRows) =>
        oldObserverRows.get(subject) match {
          case None =>
            if (status == Reachable && oldObserverRows.forall { case (_, r) => r.status == Reachable }) {
              // FIXME: how should we have gotten into this state?
              // all Reachable, prune by removing the records of the observer, and bump the version
              new Reachability(records.filterNot(_.observer == observer), newVersions)
            } else
              new Reachability(records :+ newRecord, newVersions)
          case Some(oldRecord) =>
            if (oldRecord.status == Terminated || oldRecord.status == status)
              this
            else {
              if (status == Reachable && oldObserverRows.forall {
                    case (_, r) => r.status == Reachable || r.subject == subject
                  }) {
                // all Reachable, prune by removing the records of the observer, and bump the version
                new Reachability(records.filterNot(_.observer == observer), newVersions)
              } else {
                val newRecords = records.updated(records.indexOf(oldRecord), newRecord)
                new Reachability(newRecords, newVersions)
              }
            }
        }
    }
  }

  def merge(allowed: immutable.Set[UniqueAddress], other: Reachability): Reachability = {
    val recordBuilder = new immutable.VectorBuilder[Record]
    recordBuilder.sizeHint(math.max(this.records.size, other.records.size))
    var newVersions = versions
    allowed.foreach { observer =>
      val observerVersion1 = this.currentVersion(observer)
      val observerVersion2 = other.currentVersion(observer)

      (this.observerRows(observer), other.observerRows(observer)) match {
        case (None, None)               =>
        case (Some(rows1), Some(rows2)) =>
          // We throw away a complete set of records based on the version here. Couldn't we lose records here? No,
          // because the observer gossips always the complete set of records. (That's hard to see in the model, because
          // records also contain the version number for which they were introduced but actually the version number
          // corresponds to the whole set of records of one observer at one point in time.
          val rows = if (observerVersion1 > observerVersion2) rows1 else rows2
          recordBuilder ++= rows.collect { case (_, r) if allowed(r.subject) => r }
        case (Some(rows1), None) =>
          if (observerVersion1 > observerVersion2)
            recordBuilder ++= rows1.collect { case (_, r) if allowed(r.subject) => r }
        case (None, Some(rows2)) =>
          if (observerVersion2 > observerVersion1)
            recordBuilder ++= rows2.collect { case (_, r) if allowed(r.subject) => r }
      }

      if (observerVersion2 > observerVersion1)
        newVersions += (observer -> observerVersion2)
    }

    newVersions = newVersions.filterNot { case (k, _) => !allowed(k) }

    new Reachability(recordBuilder.result(), newVersions)
  }

  def remove(nodes: Iterable[UniqueAddress]): Reachability = {
    val nodesSet: Set[UniqueAddress] = nodes.toSet
    val newRecords = records.filterNot(r => nodesSet(r.observer) || nodesSet(r.subject))
    val newVersions = versions -- nodes
    Reachability(newRecords, newVersions)
  }

  def removeObservers(nodes: Set[UniqueAddress]): Reachability =
    if (nodes.isEmpty)
      this
    else {
      val newRecords = records.filterNot(r => nodes(r.observer))
      val newVersions = versions -- nodes
      Reachability(newRecords, newVersions)
    }

  def filterRecords(f: Record => Boolean): Reachability =
    Reachability(records.filter(f), versions)

  def status(observer: UniqueAddress, subject: UniqueAddress): ReachabilityStatus =
    observerRows(observer) match {
      case None => Reachable
      case Some(observerRows) =>
        observerRows.get(subject) match {
          case None         => Reachable
          case Some(record) => record.status
        }
    }

  def status(node: UniqueAddress): ReachabilityStatus =
    if (cache.allTerminated(node)) Terminated
    else if (cache.allUnreachable(node)) Unreachable
    else Reachable

  /**
   * @return true if the given node is seen as Reachable, i.e. there's no negative (Unreachable, Terminated) observation
   * record known for that the node.
   */
  def isReachable(node: UniqueAddress): Boolean = isAllReachable || !allUnreachableOrTerminated.contains(node)

  /**
   * @return true if the given observer node can reach the subject node.
   */
  def isReachable(observer: UniqueAddress, subject: UniqueAddress): Boolean =
    status(observer, subject) == Reachable

  /**
   * @return true if there's no negative (Unreachable, Terminated) observation record at all for
   * any node
   */
  def isAllReachable: Boolean = records.isEmpty

  /**
   * @return all nodes that are Unreachable (i.e. they have been reported as Unreachable by at least one other node).
   * This does not include nodes observed to be Terminated.
   */
  def allUnreachable: Set[UniqueAddress] = cache.allUnreachable

  /**
   * @return all nodes that are Unreachable or Terminated (i.e. they have been reported as Unreachable or Terminated
   * by at least one other node).
   */
  def allUnreachableOrTerminated: Set[UniqueAddress] = cache.allUnreachableOrTerminated

  /**
   * @return all nodes that have been observed as Unreachable by the given observer.
   * This doesn't include nodes observed as Terminated.
   */
  def allUnreachableFrom(observer: UniqueAddress): Set[UniqueAddress] =
    observerRows(observer) match {
      case None => Set.empty
      case Some(observerRows) =>
        observerRows.iterator
          .collect {
            case (subject, record) if record.status == Unreachable => subject
          }
          .to(immutable.Set)
    }

  def observersGroupedByUnreachable: Map[UniqueAddress, Set[UniqueAddress]] = {
    records.groupBy(_.subject).collect {
      case (subject, recordsForSubject) if recordsForSubject.exists(_.status == Unreachable) =>
        val observers: Set[UniqueAddress] =
          recordsForSubject.iterator.collect { case r if r.status == Unreachable => r.observer }.to(immutable.Set)
        subject -> observers
    }
  }

  def allObservers: Set[UniqueAddress] = records.iterator.map(_.observer).toSet

  def recordsFrom(observer: UniqueAddress): immutable.IndexedSeq[Record] = {
    observerRows(observer) match {
      case None       => Vector.empty
      case Some(rows) => rows.valuesIterator.toVector
    }
  }

  // only used for testing
  override def hashCode: Int = versions.hashCode

  // only used for testing
  override def equals(obj: Any): Boolean = obj match {
    case other: Reachability =>
      records.size == other.records.size && versions == other.versions &&
      cache.observerRowsMap == other.cache.observerRowsMap
    case _ => false
  }

  override def toString: String = {
    val rows = for {
      observer <- versions.keys.toSeq.sorted
      rowsOption = observerRows(observer)
      if rowsOption.isDefined // compilation err for subject <- rowsOption
      rows = rowsOption.get
      subject <- rows.keys.toSeq.sorted
    } yield {
      val record = rows(subject)
      val aggregated = status(subject)
      s"${observer.address} -> ${subject.address}: ${record.status} [$aggregated] (${record.version})"
    }

    rows.mkString(", ")
  }

}
