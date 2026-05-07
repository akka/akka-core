/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence
import scala.runtime.AbstractFunction3

/**
 * Snapshot metadata.
 *
 * @param persistenceId id of persistent actor from which the snapshot was taken.
 * @param sequenceNr sequence number at which the snapshot was taken.
 * @param timestamp time at which the snapshot was saved, defaults to 0 when unknown.
 * @param metadata a journal can optionally support persisting metadata separate to the domain state, used for Replicated Event Sourcing support
 */
@SerialVersionUID(1L)
final class SnapshotMetadata(
    val persistenceId: String,
    val sequenceNr: Long,
    val timestamp: Long,
    val metadata: Option[Any])
    extends Product3[String, Long, Long]
    with Serializable {

  def this(persistenceId: String, sequenceNr: Long, timestamp: Long) = {
    this(persistenceId, sequenceNr, timestamp, None)
  }

  private[akka] def this(persistenceId: String, sequenceNr: Long, meta: Option[Any]) = {
    this(persistenceId, sequenceNr, 0L, meta)
  }

  def withMetadata(metadata: Any): SnapshotMetadata =
    new SnapshotMetadata(persistenceId, sequenceNr, timestamp, Some(metadata))

  // for bincompat, used to be a case class
  def copy(
      persistenceId: String = this.persistenceId,
      sequenceNr: Long = this.sequenceNr,
      timestamp: Long = this.timestamp): SnapshotMetadata =
    SnapshotMetadata(persistenceId, sequenceNr, timestamp, metadata)

  override def toString =
    s"SnapshotMetadata($persistenceId, $sequenceNr, $timestamp, ${metadata.map(_.getClass.getName)})"

  // Product 3
  override def productPrefix = "SnapshotMetadata"
  override def _1: String = persistenceId
  override def _2: Long = sequenceNr
  override def _3: Long = timestamp
  override def canEqual(that: Any): Boolean = that.isInstanceOf[SnapshotMetadata]

  override def equals(other: Any): Boolean = other match {
    case that: SnapshotMetadata =>
      persistenceId == that.persistenceId &&
      sequenceNr == that.sequenceNr &&
      timestamp == that.timestamp
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq[Any](persistenceId, sequenceNr, timestamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object SnapshotMetadata extends AbstractFunction3[String, Long, Long, SnapshotMetadata] {
  implicit val ordering: Ordering[SnapshotMetadata] = Ordering.fromLessThan[SnapshotMetadata] { (a, b) =>
    if (a eq b) false
    else if (a.persistenceId != b.persistenceId) a.persistenceId.compareTo(b.persistenceId) < 0
    else if (a.sequenceNr != b.sequenceNr) a.sequenceNr < b.sequenceNr
    else if (a.timestamp != b.timestamp) a.timestamp < b.timestamp
    else false
  }

  def apply(persistenceId: String, sequenceNr: Long, timestamp: Long, meta: Option[Any]): SnapshotMetadata =
    new SnapshotMetadata(persistenceId, sequenceNr, timestamp, meta)

  def apply(persistenceId: String, sequenceNr: Long, timestamp: Long): SnapshotMetadata =
    new SnapshotMetadata(persistenceId, sequenceNr, timestamp, None)

  def apply(persistenceId: String, sequenceNr: Long): SnapshotMetadata =
    new SnapshotMetadata(persistenceId, sequenceNr, 0, None)

  def unapply(sm: SnapshotMetadata): Option[(String, Long, Long)] =
    Some((sm.persistenceId, sm.sequenceNr, sm.timestamp))

  def apply$default$3(): Long = 0L

  def `<init>$default$3`: Long = 0L
}

/**
 * Sent to a [[PersistentActor]] after successful saving of a snapshot.
 *
 * @param metadata snapshot metadata.
 */
@SerialVersionUID(1L)
final case class SaveSnapshotSuccess(metadata: SnapshotMetadata) extends SnapshotProtocol.Response

/**
 * Sent to a [[PersistentActor]] after successful deletion of a snapshot.
 *
 * @param metadata snapshot metadata.
 */
@SerialVersionUID(1L)
final case class DeleteSnapshotSuccess(metadata: SnapshotMetadata) extends SnapshotProtocol.Response

/**
 * Sent to a [[PersistentActor]] after successful deletion of specified range of snapshots.
 *
 * @param criteria snapshot selection criteria.
 */
@SerialVersionUID(1L)
final case class DeleteSnapshotsSuccess(criteria: SnapshotSelectionCriteria) extends SnapshotProtocol.Response

/**
 * Sent to a [[PersistentActor]] after failed saving of a snapshot.
 *
 * @param metadata snapshot metadata.
 * @param cause failure cause.
 */
@SerialVersionUID(1L)
final case class SaveSnapshotFailure(metadata: SnapshotMetadata, cause: Throwable) extends SnapshotProtocol.Response

/**
 * Sent to a [[PersistentActor]] after failed deletion of a snapshot.
 *
 * @param metadata snapshot metadata.
 * @param cause failure cause.
 */
@SerialVersionUID(1L)
final case class DeleteSnapshotFailure(metadata: SnapshotMetadata, cause: Throwable) extends SnapshotProtocol.Response

/**
 * Sent to a [[PersistentActor]] after failed deletion of a range of snapshots.
 *
 * @param criteria snapshot selection criteria.
 * @param cause failure cause.
 */
@SerialVersionUID(1L)
final case class DeleteSnapshotsFailure(criteria: SnapshotSelectionCriteria, cause: Throwable)
    extends SnapshotProtocol.Response

/**
 * Offers a [[PersistentActor]] a previously saved `snapshot` during recovery. This offer is received
 * before any further replayed messages.
 */
@SerialVersionUID(1L)
final case class SnapshotOffer(metadata: SnapshotMetadata, snapshot: Any)

/**
 * Selection criteria for loading and deleting snapshots.
 *
 * @param maxSequenceNr upper bound for a selected snapshot's sequence number. Default is no upper bound,
 *   i.e. `Long.MaxValue`
 * @param maxTimestamp upper bound for a selected snapshot's timestamp. Default is no upper bound,
 *   i.e. `Long.MaxValue`
 * @param minSequenceNr lower bound for a selected snapshot's sequence number. Default is no lower bound,
 *   i.e. `0L`
 * @param minTimestamp lower bound for a selected snapshot's timestamp. Default is no lower bound,
 *   i.e. `0L`
 *
 * @see [[Recovery]]
 */
@SerialVersionUID(1L)
final case class SnapshotSelectionCriteria(
    maxSequenceNr: Long = Long.MaxValue,
    maxTimestamp: Long = Long.MaxValue,
    minSequenceNr: Long = 0L,
    minTimestamp: Long = 0L) {

  /**
   * INTERNAL API.
   */
  private[persistence] def limit(toSequenceNr: Long): SnapshotSelectionCriteria =
    if (toSequenceNr < maxSequenceNr) copy(maxSequenceNr = toSequenceNr) else this

  /**
   * INTERNAL API.
   */
  private[persistence] def matches(metadata: SnapshotMetadata): Boolean =
    metadata.sequenceNr <= maxSequenceNr && metadata.timestamp <= maxTimestamp &&
    metadata.sequenceNr >= minSequenceNr && metadata.timestamp >= minTimestamp
}

object SnapshotSelectionCriteria {

  /**
   * The latest saved snapshot.
   */
  val Latest = SnapshotSelectionCriteria()

  /**
   * No saved snapshot matches.
   */
  val None = SnapshotSelectionCriteria(maxSequenceNr = 0L, maxTimestamp = 0L)

  /**
   * Java API.
   */
  def create(maxSequenceNr: Long, maxTimestamp: Long) =
    SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp)

  /**
   * Java API.
   */
  def create(maxSequenceNr: Long, maxTimestamp: Long, minSequenceNr: Long, minTimestamp: Long) =
    SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp)

  /**
   * Java API.
   */
  def latest() = Latest

  /**
   * Java API.
   */
  def none() = None

}

/**
 * Plugin API: a selected snapshot matching [[SnapshotSelectionCriteria]].
 *
 * @param metadata snapshot metadata.
 * @param snapshot snapshot.
 */
final case class SelectedSnapshot(metadata: SnapshotMetadata, snapshot: Any)

object SelectedSnapshot {

  /**
   * Java API, Plugin API.
   */
  def create(metadata: SnapshotMetadata, snapshot: Any): SelectedSnapshot =
    SelectedSnapshot(metadata, snapshot)
}

/**
 * INTERNAL API.
 *
 * Defines messages exchanged between persistent actors and a snapshot store.
 */
private[persistence] object SnapshotProtocol {

  /** Marker trait shared by internal snapshot messages. */
  sealed trait Message extends Protocol.Message

  /** Internal snapshot command. */
  sealed trait Request extends Message

  /** Internal snapshot acknowledgement. */
  sealed trait Response extends Message

  /**
   * Instructs a snapshot store to load a snapshot.
   *
   * @param persistenceId persistent actor id.
   * @param criteria criteria for selecting a snapshot from which recovery should start.
   * @param toSequenceNr upper sequence number bound (inclusive) for recovery.
   */
  final case class LoadSnapshot(persistenceId: String, criteria: SnapshotSelectionCriteria, toSequenceNr: Long)
      extends Request

  /**
   * Response message to a [[LoadSnapshot]] message.
   *
   * @param snapshot loaded snapshot, if any.
   */
  final case class LoadSnapshotResult(snapshot: Option[SelectedSnapshot], toSequenceNr: Long) extends Response

  /**
   * Reply message to a failed [[LoadSnapshot]] request.
   * @param cause failure cause.
   */
  final case class LoadSnapshotFailed(cause: Throwable) extends Response

  /**
   * Instructs snapshot store to save a snapshot.
   *
   * @param metadata snapshot metadata.
   * @param snapshot snapshot.
   */
  final case class SaveSnapshot(metadata: SnapshotMetadata, snapshot: Any) extends Request

  /**
   * Instructs snapshot store to delete a snapshot.
   *
   * @param metadata snapshot metadata.
   */
  final case class DeleteSnapshot(metadata: SnapshotMetadata) extends Request

  /**
   * Instructs snapshot store to delete all snapshots that match `criteria`.
   *
   * @param persistenceId persistent actor id.
   * @param criteria criteria for selecting snapshots to be deleted.
   */
  final case class DeleteSnapshots(persistenceId: String, criteria: SnapshotSelectionCriteria) extends Request
}
