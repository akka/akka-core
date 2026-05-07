/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.util.control.NonFatal

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.WrappedMessage
import akka.annotation.InternalApi
import akka.annotation.InternalStableApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.remote.RemoteActorRefProvider
import akka.util.OptionVal

/**
 * INTERNAL API
 *
 * Part of the monitoring SPI which allows attaching metadata to outbound remote messages,
 * and reading in metadata from incoming messages.
 *
 * Multiple instruments are automatically handled, however they MUST NOT overlap in their idenfitiers.
 *
 * Instances of `RemoteInstrument` are created from configuration. A new instance of RemoteInstrument
 * will be created for each encoder and decoder. It's only called from the operator, so if it doesn't
 * delegate to any shared instance it doesn't have to be thread-safe.
 */
abstract class RemoteInstrument {

  /**
   * Instrument identifier.
   *
   * MUST be >=1 and <32.
   *
   * Values between 1 and 7 are reserved for Akka internal use.
   */
  def identifier: Byte

  /**
   * Should the serialization be timed? Otherwise times are always 0.
   */
  def serializationTimingEnabled: Boolean = false

  /**
   * Called while serializing the message.
   * Parameters MAY be `null` (except `message` and `buffer`)!
   */
  def remoteWriteMetadata(recipient: ActorRef, message: Object, sender: ActorRef, buffer: ByteBuffer): Unit

  /**
   * Called right before putting the message onto the wire.
   * Parameters MAY be `null` (except `message` and `buffer`)!
   *
   * The `size` is the total serialized size in bytes of the complete message including akka specific headers and any
   * `RemoteInstrument` metadata.
   * If `serializationTimingEnabled` returns true, then `time` will be the total time it took to serialize all data
   * in the message in nanoseconds, otherwise it is 0.
   */
  def remoteMessageSent(recipient: ActorRef, message: Object, sender: ActorRef, size: Int, time: Long): Unit

  /**
   * Called while deserializing the message once a message (containing a metadata field designated for this instrument) is found.
   */
  def remoteReadMetadata(recipient: ActorRef, message: Object, sender: ActorRef, buffer: ByteBuffer): Unit

  /**
   * Called when the message has been deserialized.
   *
   * The `size` is the total serialized size in bytes of the complete message including akka specific headers and any
   * `RemoteInstrument` metadata.
   * If `serializationTimingEnabled` returns true, then `time` will be the total time it took to deserialize all data
   * in the message in nanoseconds, otherwise it is 0.
   */
  def remoteMessageReceived(recipient: ActorRef, message: Object, sender: ActorRef, size: Int, time: Long): Unit
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class LoggingRemoteInstrument(system: ActorSystem) extends RemoteInstrument {

  private val settings = system
    .asInstanceOf[ExtendedActorSystem]
    .provider
    .asInstanceOf[RemoteActorRefProvider]
    .transport
    .asInstanceOf[ArteryTransport]
    .settings
  private val logFrameSizeExceeding = settings.LogFrameSizeExceeding.get

  private val log = Logging(system, classOf[LoggingRemoteInstrument])

  private val maxPayloadBytes: ConcurrentHashMap[Class[_], Integer] = new ConcurrentHashMap

  override def identifier: Byte = 1 // Cinnamon is using 0

  override def remoteWriteMetadata(recipient: ActorRef, message: Object, sender: ActorRef, buffer: ByteBuffer): Unit =
    ()

  override def remoteMessageSent(
      recipient: ActorRef,
      message: Object,
      sender: ActorRef,
      size: Int,
      time: Long): Unit = {
    if (size >= logFrameSizeExceeding) {
      val clazz = message match {
        case x: WrappedMessage => x.message.getClass
        case _                 => message.getClass
      }

      // 10% threshold until next log
      def newMax = (size * 1.1).toInt

      @tailrec def check(): Unit = {
        val max = maxPayloadBytes.get(clazz)
        if (max eq null) {
          if (maxPayloadBytes.putIfAbsent(clazz, newMax) eq null)
            log.info("Payload size for [{}] is [{}] bytes. Sent to {}", clazz.getName, size, recipient)
          else check()
        } else if (size > max) {
          if (maxPayloadBytes.replace(clazz, max, newMax))
            log.info("New maximum payload size for [{}] is [{}] bytes. Sent to {}.", clazz.getName, size, recipient)
          else check()
        }
      }
      check()
    }
  }

  override def remoteReadMetadata(recipient: ActorRef, message: Object, sender: ActorRef, buffer: ByteBuffer): Unit =
    ()

  override def remoteMessageReceived(
      recipient: ActorRef,
      message: Object,
      sender: ActorRef,
      size: Int,
      time: Long): Unit = ()
}

/**
 * INTERNAL API
 *
 * The metadata section is stored as raw bytes (prefixed with an Int length field,
 * the same way as any other literal), however the internal structure of it is as follows:
 *
 * {{{
 * Metadata entry:
 *
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |   Key     |             Metadata entry length                   |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                   ... metadata entry ...                        |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * }}}
 *
 */
private[remote] final class RemoteInstruments(
    private val system: ExtendedActorSystem,
    private val log: LoggingAdapter,
    _instruments: Vector[RemoteInstrument]) {
  import RemoteInstruments._

  def this(system: ExtendedActorSystem, log: LoggingAdapter) = this(system, log, RemoteInstruments.create(system, log))
  def this(system: ExtendedActorSystem) = this(system, Logging.getLogger(system, classOf[RemoteInstruments]))

  // keep the remote instruments sorted by identifier to speed up deserialization
  private val instruments: Vector[RemoteInstrument] = _instruments.sortBy(_.identifier)
  // does any of the instruments want serialization timing?
  private val serializationTimingEnabled = instruments.exists(_.serializationTimingEnabled)

  def serialize(outboundEnvelope: OptionVal[OutboundEnvelope], buffer: ByteBuffer): Unit = {
    if (instruments.nonEmpty && outboundEnvelope.isDefined) {
      val startPos = buffer.position()
      val oe = outboundEnvelope.get
      try {
        buffer.putInt(0)
        val dataPos = buffer.position()
        var i = 0
        while (i < instruments.length) {
          val rewindPos = buffer.position()
          val instrument = instruments(i)
          try {
            serializeInstrument(instrument, oe, buffer)
          } catch {
            case NonFatal(t) =>
              log.debug(
                "Skipping serialization of RemoteInstrument {} since it failed with {}",
                instrument.identifier,
                t.getMessage)
              buffer.position(rewindPos)
          }
          i += 1
        }
        val endPos = buffer.position()
        if (endPos == dataPos) {
          // no instruments wrote anything so we need to rewind to start
          buffer.position(startPos)
        } else {
          // some instruments wrote data, so write the total length
          buffer.putInt(startPos, endPos - dataPos)
        }
      } catch {
        case NonFatal(t) =>
          log.debug("Skipping serialization of all RemoteInstruments due to unhandled failure {}", t)
          buffer.position(startPos)
      }
    }
  }

  private def serializeInstrument(
      instrument: RemoteInstrument,
      outboundEnvelope: OutboundEnvelope,
      buffer: ByteBuffer): Unit = {
    val startPos = buffer.position()
    buffer.putInt(0)
    val dataPos = buffer.position()
    instrument.remoteWriteMetadata(
      outboundEnvelope.recipient.orNull,
      outboundEnvelope.message,
      outboundEnvelope.sender.orNull,
      buffer)
    val endPos = buffer.position()
    if (endPos == dataPos) {
      // if the instrument didn't write anything, then rewind to the start
      buffer.position(startPos)
    } else {
      // the instrument wrote something so we need to write the identifier and length
      buffer.putInt(startPos, combineKeyLength(instrument.identifier, endPos - dataPos))
    }
  }

  def deserialize(inboundEnvelope: InboundEnvelope): Unit = {
    if (inboundEnvelope.flag(EnvelopeBuffer.MetadataPresentFlag)) {
      inboundEnvelope.envelopeBuffer.byteBuffer.position(EnvelopeBuffer.MetadataContainerAndLiteralSectionOffset)
      deserializeRaw(inboundEnvelope)
    }
  }

  def deserializeRaw(inboundEnvelope: InboundEnvelope): Unit = {
    val buffer = inboundEnvelope.envelopeBuffer.byteBuffer
    val length = buffer.getInt
    val endPos = buffer.position() + length
    try {
      if (instruments.nonEmpty) {
        var i = 0
        while (i < instruments.length && buffer.position() < endPos) {
          val instrument = instruments(i)
          val startPos = buffer.position()
          val keyAndLength = buffer.getInt
          val dataPos = buffer.position()
          val key = getKey(keyAndLength)
          val length = getLength(keyAndLength)
          var nextPos = dataPos + length
          val identifier = instrument.identifier
          if (key == identifier) {
            try {
              deserializeInstrument(instrument, inboundEnvelope, buffer)
            } catch {
              case NonFatal(t) =>
                log.debug(
                  "Skipping deserialization of RemoteInstrument {} since it failed with {}",
                  instrument.identifier,
                  t.getMessage)
            }
            i += 1
          } else if (key > identifier) {
            // since instruments are sorted on both sides skip this local one and retry the serialized one
            log.debug("Skipping local RemoteInstrument {} that has no matching data in the message", identifier)
            nextPos = startPos
            i += 1
          } else {
            // since instruments are sorted on both sides skip the serialized one and retry the local one
            log.debug("Skipping serialized data in message for RemoteInstrument {} that has no local match", key)
          }
          buffer.position(nextPos)
        }
      } else {
        if (log.isDebugEnabled)
          log.debug(
            "Skipping serialized data in message for RemoteInstrument(s) {} that has no local match",
            remoteInstrumentIdIteratorRaw(buffer, endPos).mkString("[", ", ", "]"))
      }
    } catch {
      case NonFatal(t) =>
        log.debug("Skipping further deserialization of remaining RemoteInstruments due to unhandled failure {}", t)
    } finally {
      buffer.position(endPos)
    }
  }

  private def deserializeInstrument(
      instrument: RemoteInstrument,
      inboundEnvelope: InboundEnvelope,
      buffer: ByteBuffer): Unit = {
    instrument.remoteReadMetadata(
      inboundEnvelope.recipient.orNull,
      inboundEnvelope.message,
      inboundEnvelope.sender.orNull,
      buffer)
  }

  def messageSent(outboundEnvelope: OutboundEnvelope, size: Int, time: Long): Unit = {
    @tailrec def messageSent(pos: Int): Unit = {
      if (pos < instruments.length) {
        val instrument = instruments(pos)
        try {
          messageSentInstrument(instrument, outboundEnvelope, size, time)
        } catch {
          case NonFatal(t) =>
            log.debug("Message sent in RemoteInstrument {} failed with {}", instrument.identifier, t.getMessage)
        }
        messageSent(pos + 1)
      }
    }
    messageSent(0)
  }

  private def messageSentInstrument(
      instrument: RemoteInstrument,
      outboundEnvelope: OutboundEnvelope,
      size: Int,
      time: Long): Unit = {
    instrument.remoteMessageSent(
      outboundEnvelope.recipient.orNull,
      outboundEnvelope.message,
      outboundEnvelope.sender.orNull,
      size,
      time)
  }

  def messageReceived(inboundEnvelope: InboundEnvelope, size: Int, time: Long): Unit = {
    @tailrec def messageRecieved(pos: Int): Unit = {
      if (pos < instruments.length) {
        val instrument = instruments(pos)
        try {
          messageReceivedInstrument(instrument, inboundEnvelope, size, time)
        } catch {
          case NonFatal(t) =>
            log.debug("Message received in RemoteInstrument {} failed with {}", instrument.identifier, t.getMessage)
        }
        messageRecieved(pos + 1)
      }
    }
    messageRecieved(0)
  }

  private def messageReceivedInstrument(
      instrument: RemoteInstrument,
      inboundEnvelope: InboundEnvelope,
      size: Int,
      time: Long): Unit = {
    instrument.remoteMessageReceived(
      inboundEnvelope.recipient.orNull,
      inboundEnvelope.message,
      inboundEnvelope.sender.orNull,
      size,
      time)
  }

  private def remoteInstrumentIdIteratorRaw(buffer: ByteBuffer, endPos: Int): Iterator[Int] = {
    new Iterator[Int] {
      override def hasNext: Boolean = buffer.position() < endPos
      override def next(): Int = {
        val keyAndLength = buffer.getInt
        buffer.position(buffer.position() + getLength(keyAndLength))
        getKey(keyAndLength)
      }
    }
  }

  def isEmpty: Boolean = instruments.isEmpty
  def nonEmpty: Boolean = instruments.nonEmpty
  def timeSerialization: Boolean = serializationTimingEnabled
}

/** INTERNAL API */
private[remote] object RemoteInstruments {

  def apply(system: ExtendedActorSystem): RemoteInstruments =
    new RemoteInstruments(system)

  // key/length of a metadata element are encoded within a single integer:
  // supports keys in the range of <0-31>
  private final val lengthMask: Int = ~(31 << 26)
  def combineKeyLength(k: Byte, l: Int): Int = (k.toInt << 26) | (l & lengthMask)
  def getKey(kl: Int): Byte = (kl >>> 26).toByte
  def getLength(kl: Int): Int = kl & lengthMask

  @InternalStableApi
  def create(system: ExtendedActorSystem, @nowarn("msg=never used") log: LoggingAdapter): Vector[RemoteInstrument] = {
    val c = system.settings.config
    val path = "akka.remote.artery.advanced.instruments"
    import scala.jdk.CollectionConverters._
    val configuredInstruments = c
      .getStringList(path)
      .asScala
      .iterator
      .map { fqcn =>
        system.dynamicAccess
          .createInstanceFor[RemoteInstrument](fqcn, Nil)
          .orElse(system.dynamicAccess
            .createInstanceFor[RemoteInstrument](fqcn, List(classOf[ExtendedActorSystem] -> system)))
          .get
      }
      .toVector

    system.provider match {
      case rarp: RemoteActorRefProvider =>
        rarp.transport match {
          case artery: ArteryTransport =>
            artery.settings.LogFrameSizeExceeding match {
              case Some(_) => configuredInstruments :+ new LoggingRemoteInstrument(system)
              case None    => configuredInstruments
            }
          case _ => configuredInstruments
        }
      case _ => configuredInstruments
    }
  }
}
