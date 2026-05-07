/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.collection.immutable
import scala.reflect.ClassTag

import akka.actor.{ ActorRef, NoSerializationVerificationNeeded }
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.persistence.serialization.Message
import akka.util.HashCode

/**
 * INTERNAL API
 *
 * Marks messages which can be resequenced by the [[akka.persistence.journal.AsyncWriteJournal]].
 *
 * In essence it is either an [[NonPersistentRepr]] or [[AtomicWrite]].
 */
@InternalApi
private[persistence] sealed trait PersistentEnvelope {
  def payload: Any
  def sender: ActorRef
  def size: Int
}

/**
 * INTERNAL API
 * Message which can be resequenced by the Journal, but will not be persisted.
 */
@InternalApi
private[persistence] final case class NonPersistentRepr(payload: Any, sender: ActorRef) extends PersistentEnvelope {
  override def size: Int = 1
}

object AtomicWrite {
  def apply(event: PersistentRepr): AtomicWrite = apply(List(event))
}

final case class AtomicWrite(payload: immutable.Seq[PersistentRepr]) extends PersistentEnvelope with Message {
  require(payload.nonEmpty, "payload of AtomicWrite must not be empty!")
  private var _highestSequenceNr: Long = payload.head.sequenceNr

  // only check that all persistenceIds are equal when there's more than one in the Seq
  if (payload match {
        case l: List[PersistentRepr]   => l.tail.nonEmpty // avoids calling .size
        case v: Vector[PersistentRepr] => v.size > 1
        case _                         => true // some other collection type, let's just check
      }) payload.foreach { pr =>
    if (pr.persistenceId != payload.head.persistenceId)
      throw new IllegalArgumentException(
        "AtomicWrite must contain messages for the same persistenceId, " +
        s"yet different persistenceIds found: ${payload.map(_.persistenceId).toSet}")
    _highestSequenceNr = pr.sequenceNr
  }

  def persistenceId = payload.head.persistenceId
  def lowestSequenceNr =
    payload.head.sequenceNr // this assumes they're gapless; they should be (it is only our code creating AWs)
  def highestSequenceNr = _highestSequenceNr

  override def sender: ActorRef = ActorRef.noSender
  override def size: Int = payload.size
}

/**
 * Plugin API: representation of a persistent message in the journal plugin API.
 *
 * @see [[akka.persistence.journal.AsyncWriteJournal]]
 * @see [[akka.persistence.journal.AsyncRecovery]]
 */
@DoNotInherit trait PersistentRepr extends Message {

  /**
   * This persistent message's payload (the event).
   */
  def payload: Any

  /**
   * Returns the event adapter manifest for the persistent payload (event) if available
   * May be `""` if event adapter manifest is not used.
   * Note that this is not the same as the manifest of the serialized representation of the `payload`.
   */
  def manifest: String

  /**
   * Persistent id that journals a persistent message
   */
  def persistenceId: String

  /**
   * This persistent message's sequence number.
   */
  def sequenceNr: Long

  /**
   * The `timestamp` is the time the event was stored, in milliseconds since midnight, January 1, 1970 UTC
   * (same as `System.currentTimeMillis`).
   *
   * Value `0` is used if undefined.
   */
  def timestamp: Long

  def withTimestamp(newTimestamp: Long): PersistentRepr

  def metadata: Option[Any]

  def withMetadata(metadata: Any): PersistentRepr

  /**
   * Unique identifier of the writing persistent actor.
   * Used to detect anomalies with overlapping writes from multiple
   * persistent actors, which can result in inconsistent replays.
   */
  def writerUuid: String

  /**
   * Creates a new persistent message with the specified `payload` (event).
   */
  def withPayload(payload: Any): PersistentRepr

  /**
   * Creates a new persistent message with the specified event adapter `manifest`.
   */
  def withManifest(manifest: String): PersistentRepr

  /**
   * Not used, can always be `false`.
   *
   * Not used in new records stored with Akka v2.4, but
   * old records from v2.3 may have this as `true` if
   * it was a non-permanent delete.
   */
  def deleted: Boolean // FIXME deprecate, issue #27278

  /**
   * Not used, can be `null`
   */
  def sender: ActorRef // FIXME deprecate, issue #27278

  /**
   * Creates a new copy of this [[PersistentRepr]].
   */
  def update(
      sequenceNr: Long = sequenceNr,
      persistenceId: String = persistenceId,
      deleted: Boolean = deleted,
      sender: ActorRef = sender,
      writerUuid: String = writerUuid): PersistentRepr
}

object PersistentRepr {

  /** Plugin API: value of an undefined persistenceId or manifest. */
  val Undefined = ""

  /** Plugin API: value of an undefined / identity event adapter. */
  val UndefinedId = 0

  /**
   * Plugin API.
   */
  def apply(
      payload: Any,
      sequenceNr: Long = 0L,
      persistenceId: String = PersistentRepr.Undefined,
      manifest: String = PersistentRepr.Undefined,
      deleted: Boolean = false,
      sender: ActorRef = null,
      writerUuid: String = PersistentRepr.Undefined): PersistentRepr =
    PersistentImpl(payload, sequenceNr, persistenceId, manifest, deleted, sender, writerUuid, 0L, None)

  /**
   * Java API, Plugin API.
   */
  def create = apply _

  /**
   * extractor of payload and sequenceNr.
   */
  def unapply(persistent: PersistentRepr): Option[(Any, Long)] =
    Some((persistent.payload, persistent.sequenceNr))
}

/**
 * INTERNAL API.
 */
@InternalApi
private[persistence] final case class PersistentImpl(
    override val payload: Any,
    override val sequenceNr: Long,
    override val persistenceId: String,
    override val manifest: String,
    override val deleted: Boolean,
    override val sender: ActorRef,
    override val writerUuid: String,
    override val timestamp: Long,
    override val metadata: Option[Any])
    extends PersistentRepr
    with NoSerializationVerificationNeeded {

  def withPayload(payload: Any): PersistentRepr =
    copy(payload = payload)

  def withManifest(manifest: String): PersistentRepr =
    if (this.manifest == manifest) this
    else copy(manifest = manifest)

  override def withTimestamp(newTimestamp: Long): PersistentRepr =
    if (this.timestamp == newTimestamp) this
    else copy(timestamp = newTimestamp)

  override def withMetadata(metadata: Any): PersistentRepr = {
    copy(metadata = Some(metadata))
  }

  def update(sequenceNr: Long, persistenceId: String, deleted: Boolean, sender: ActorRef, writerUuid: String) =
    copy(
      sequenceNr = sequenceNr,
      persistenceId = persistenceId,
      deleted = deleted,
      sender = sender,
      writerUuid = writerUuid)

  override def hashCode(): Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, payload)
    result = HashCode.hash(result, sequenceNr)
    result = HashCode.hash(result, persistenceId)
    result = HashCode.hash(result, manifest)
    result = HashCode.hash(result, deleted)
    result = HashCode.hash(result, sender)
    result = HashCode.hash(result, writerUuid)
    // timestamp not included in equals for backwards compatibility
    // meta not included in equals for backwards compatibility
    result
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: PersistentImpl =>
      payload == other.payload && sequenceNr == other.sequenceNr && persistenceId == other.persistenceId &&
      manifest == other.manifest && deleted == other.deleted &&
      sender == other.sender && writerUuid == other.writerUuid // timestamp not included in equals for backwards compatibility
    case _ => false
  }

  override def toString: String = {
    s"PersistentRepr($persistenceId,$sequenceNr,$writerUuid,$timestamp,$metadata)"
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CompositeMetadata {
  def extract[M: ClassTag](metadata: Option[Any]): Option[M] = {
    val metadataType = implicitly[ClassTag[M]].runtimeClass
    metadata.flatMap {
      case CompositeMetadata(entries) =>
        entries.collectFirst {
          case m: M if metadataType == m.getClass => m
        }
      case other: M if metadataType == other.getClass => Some(other)
      case _                                          => None
    }
  }

  def extract[M: ClassTag](metadataEntries: Seq[Any]): Option[M] = {
    val metadataType = implicitly[ClassTag[M]].runtimeClass
    metadataEntries.collectFirst {
      case m: M if metadataType == m.getClass => m
    }
  }

  def construct(metadataEntries: Seq[Any]): Option[Any] = {
    if (metadataEntries.isEmpty)
      None
    else if (metadataEntries.size == 1)
      Some(metadataEntries.head)
    else
      Some(CompositeMetadata(metadataEntries))
  }

}

/**
 * INTERNAL API: Metadata entries keyed by metadata class.
 *
 * For backwards compatibility of for example EventEnvelope.eventMetadata the entries is a Seq
 * with the metadata that was added last (ReplicatedEventMetaData) at the head of the Seq.
 * Otherwise it could have been a `Map[Class, Any]`.
 */
@InternalApi
private[akka] final case class CompositeMetadata(entries: immutable.Seq[Any]) {
  if (entries.isEmpty)
    throw new IllegalArgumentException("CompositeMetadata must have at least one entry")
}
