/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import akka.actor.ActorRef
import akka.actor.InternalActorRef
import akka.actor.NoSerializationVerificationNeeded
import akka.util.OptionVal

/**
 * INTERNAL API
 */
private[remote] object InboundEnvelope {

  /**
   * Only used in tests
   */
  def apply(
      recipient: OptionVal[InternalActorRef],
      message: AnyRef,
      sender: OptionVal[ActorRef],
      originUid: Long,
      association: OptionVal[OutboundContext]): InboundEnvelope = {
    val env = new ReusableInboundEnvelope
    env.init(recipient, sender, originUid, -1, "", 0, null, association, lane = 0).withMessage(message)
  }

}

/**
 * INTERNAL API
 */
private[remote] trait InboundEnvelope extends NoSerializationVerificationNeeded {
  def recipient: OptionVal[InternalActorRef]
  def sender: OptionVal[ActorRef]
  def originUid: Long
  def association: OptionVal[OutboundContext]

  def serializer: Int
  def classManifest: String
  def message: AnyRef
  def envelopeBuffer: EnvelopeBuffer

  def flags: Byte
  def flag(byteFlag: ByteFlag): Boolean

  def withMessage(message: AnyRef): InboundEnvelope

  def releaseEnvelopeBuffer(): InboundEnvelope

  def withRecipient(ref: InternalActorRef): InboundEnvelope

  def lane: Int
  def copyForLane(lane: Int): InboundEnvelope
}

/**
 * INTERNAL API
 */
private[remote] object ReusableInboundEnvelope {
  def createObjectPool(capacity: Int) =
    new ObjectPool[ReusableInboundEnvelope](
      capacity,
      create = () => new ReusableInboundEnvelope,
      clear = inEnvelope => inEnvelope.asInstanceOf[ReusableInboundEnvelope].clear())
}

/**
 * INTERNAL API
 */
private[remote] final class ReusableInboundEnvelope extends InboundEnvelope {
  private var _recipient: OptionVal[InternalActorRef] = OptionVal.None
  private var _sender: OptionVal[ActorRef] = OptionVal.None
  private var _originUid: Long = 0L
  private var _association: OptionVal[OutboundContext] = OptionVal.None
  private var _serializer: Int = -1
  private var _classManifest: String = null
  private var _flags: Byte = 0
  private var _lane: Int = 0
  private var _message: AnyRef = null
  private var _envelopeBuffer: EnvelopeBuffer = null

  override def recipient: OptionVal[InternalActorRef] = _recipient
  override def sender: OptionVal[ActorRef] = _sender
  override def originUid: Long = _originUid
  override def association: OptionVal[OutboundContext] = _association
  override def serializer: Int = _serializer
  override def classManifest: String = _classManifest
  override def message: AnyRef = _message
  override def envelopeBuffer: EnvelopeBuffer = _envelopeBuffer

  override def flags: Byte = _flags
  override def flag(byteFlag: ByteFlag): Boolean = byteFlag.isEnabled(_flags)

  override def lane: Int = _lane

  override def withMessage(message: AnyRef): InboundEnvelope = {
    _message = message
    this
  }

  def releaseEnvelopeBuffer(): InboundEnvelope = {
    _envelopeBuffer = null
    this
  }

  def withRecipient(ref: InternalActorRef): InboundEnvelope = {
    _recipient = OptionVal(ref)
    this
  }

  def clear(): Unit = {
    _recipient = OptionVal.None
    _message = null
    _sender = OptionVal.None
    _originUid = 0L
    _association = OptionVal.None
    _lane = 0
  }

  def init(
      recipient: OptionVal[InternalActorRef],
      sender: OptionVal[ActorRef],
      originUid: Long,
      serializer: Int,
      classManifest: String,
      flags: Byte,
      envelopeBuffer: EnvelopeBuffer,
      association: OptionVal[OutboundContext],
      lane: Int): InboundEnvelope = {
    _recipient = recipient
    _sender = sender
    _originUid = originUid
    _serializer = serializer
    _classManifest = classManifest
    _flags = flags
    _envelopeBuffer = envelopeBuffer
    _association = association
    _lane = lane
    this
  }

  def withEnvelopeBuffer(envelopeBuffer: EnvelopeBuffer): InboundEnvelope = {
    _envelopeBuffer = envelopeBuffer
    this
  }

  override def copyForLane(lane: Int): InboundEnvelope = {
    val buf = if (envelopeBuffer eq null) null else envelopeBuffer.copy()
    val env = new ReusableInboundEnvelope
    env
      .init(recipient, sender, originUid, serializer, classManifest, flags, buf, association, lane)
      .withMessage(message)

  }

  override def toString: String =
    s"InboundEnvelope($recipient, $message, $sender, $originUid, $association)"
}
