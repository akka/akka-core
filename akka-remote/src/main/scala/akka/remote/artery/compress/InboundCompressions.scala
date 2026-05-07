/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery.compress

import java.util.function.LongFunction

import scala.annotation.nowarn
import scala.annotation.tailrec

import org.agrona.collections.Long2ObjectHashMap

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.InternalActorRef
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.remote.artery._
import akka.util.OptionVal

/**
 * INTERNAL API
 * Decompress and cause compression advertisements.
 *
 * One per inbound message stream thus must demux by originUid to use the right tables.
 */
private[remote] trait InboundCompressions {
  def hitActorRef(originUid: Long, remote: Address, ref: ActorRef, n: Int): Unit
  def decompressActorRef(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[ActorRef]
  def confirmActorRefCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit

  /** Triggers compression advertisement via control message. */
  def runNextActorRefAdvertisement(): Unit

  def hitClassManifest(originUid: Long, remote: Address, manifest: String, n: Int): Unit
  def decompressClassManifest(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[String]
  def confirmClassManifestCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit

  /** Triggers compression advertisement via control message. */
  def runNextClassManifestAdvertisement(): Unit

  def currentOriginUids: Set[Long]

  /**
   * Remove compression and cancel advertisement scheduling for a specific origin
   */
  def close(originUid: Long): Unit

}

/**
 * INTERNAL API
 *
 * One per incoming Aeron stream, actual compression tables are kept per-originUid and created on demand.
 * All access is via the Decoder stage.
 */
private[remote] final class InboundCompressionsImpl(
    system: ActorSystem,
    inboundContext: InboundContext,
    settings: ArterySettings.Compression)
    extends InboundCompressions {

  private[this] val _actorRefsIns = new Long2ObjectHashMap[InboundActorRefCompression]()
  private[this] val _inboundActorRefsLog = Logging(system, classOf[InboundActorRefCompression])
  private val createInboundActorRefsForOrigin = new LongFunction[InboundActorRefCompression] {
    override def apply(originUid: Long): InboundActorRefCompression = {
      val actorRefHitters = new TopHeavyHitters[ActorRef](settings.ActorRefs.Max)
      new InboundActorRefCompression(_inboundActorRefsLog, settings, originUid, inboundContext, actorRefHitters)
    }
  }
  private def actorRefsIn(originUid: Long): InboundActorRefCompression =
    _actorRefsIns.computeIfAbsent(originUid, createInboundActorRefsForOrigin)

  private[this] val _classManifestsIns = new Long2ObjectHashMap[InboundManifestCompression]()

  private[this] val _inboundManifestLog = Logging(system, classOf[InboundManifestCompression])
  private val createInboundManifestsForOrigin = new LongFunction[InboundManifestCompression] {
    override def apply(originUid: Long): InboundManifestCompression = {
      val manifestHitters = new TopHeavyHitters[String](settings.Manifests.Max)
      new InboundManifestCompression(_inboundManifestLog, settings, originUid, inboundContext, manifestHitters)
    }
  }
  private def classManifestsIn(originUid: Long): InboundManifestCompression =
    _classManifestsIns.computeIfAbsent(originUid, createInboundManifestsForOrigin)

  // actor ref compression ---

  override def decompressActorRef(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[ActorRef] =
    actorRefsIn(originUid).decompress(tableVersion, idx)

  override def hitActorRef(originUid: Long, address: Address, ref: ActorRef, n: Int): Unit = {
    if (ArterySettings.Compression.Debug) println(s"[compress] hitActorRef($originUid, $address, $ref, $n)")
    actorRefsIn(originUid).increment(address, ref, n)
  }

  override def confirmActorRefCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = {
    _actorRefsIns.get(originUid) match {
      case null => // ignore
      case a    => a.confirmAdvertisement(tableVersion, gaveUp = false)
    }
  }

  /** Send compression table advertisement over control stream. Should be called from Decoder. */
  override def runNextActorRefAdvertisement(): Unit = {
    val vs = _actorRefsIns.values.iterator()
    var remove = Vector.empty[Long]
    while (vs.hasNext) {
      val inbound = vs.next()
      inboundContext.association(inbound.originUid) match {
        case OptionVal.Some(a) if !a.associationState.isQuarantined(inbound.originUid) =>
          RemotingFlightRecorder.compressionActorRefAdvertisement(inbound.originUid)
          inbound.runNextTableAdvertisement()
        case _ => remove :+= inbound.originUid
      }
    }
    if (remove.nonEmpty) remove.foreach(close)
  }

  // class manifest compression ---

  override def decompressClassManifest(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[String] =
    classManifestsIn(originUid).decompress(tableVersion, idx)

  override def hitClassManifest(originUid: Long, address: Address, manifest: String, n: Int): Unit = {
    if (ArterySettings.Compression.Debug) println(s"[compress] hitClassManifest($originUid, $address, $manifest, $n)")
    classManifestsIn(originUid).increment(address, manifest, n)
  }
  override def confirmClassManifestCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = {
    _classManifestsIns.get(originUid) match {
      case null => // ignore
      case a    => a.confirmAdvertisement(tableVersion, gaveUp = false)
    }
  }

  /** Send compression table advertisement over control stream. Should be called from Decoder. */
  override def runNextClassManifestAdvertisement(): Unit = {
    val vs = _classManifestsIns.values.iterator()
    var remove = Vector.empty[Long]
    while (vs.hasNext) {
      val inbound = vs.next()
      inboundContext.association(inbound.originUid) match {
        case OptionVal.Some(a) if !a.associationState.isQuarantined(inbound.originUid) =>
          RemotingFlightRecorder.compressionClassManifestAdvertisement(inbound.originUid)
          inbound.runNextTableAdvertisement()
        case _ => remove :+= inbound.originUid
      }
    }
    if (remove.nonEmpty) remove.foreach(close)
  }

  override def currentOriginUids: Set[Long] = {
    import scala.jdk.CollectionConverters._
    // can't use union because of java.lang.Long and Scala Long mismatch,
    // only used for testing so doesn't matter
    val result = Set.empty[java.lang.Long] ++ _actorRefsIns.keySet.asScala.iterator ++
      _classManifestsIns.keySet.asScala.iterator
    result.map(_.longValue)
  }

  override def close(originUid: Long): Unit = {
    _actorRefsIns.remove(originUid)
    _classManifestsIns.remove(originUid)
  }
}

/**
 * INTERNAL API
 * Dedicated per remote system inbound compression table.
 *
 * The outbound context is available by looking it up in the association.
 * It can be used to advertise a compression table.
 * If the association is not complete - we simply dont advertise the table, which is fine (handshake not yet complete).
 */
private[remote] final class InboundActorRefCompression(
    log: LoggingAdapter,
    settings: ArterySettings.Compression,
    originUid: Long,
    inboundContext: InboundContext,
    heavyHitters: TopHeavyHitters[ActorRef])
    extends InboundCompression[ActorRef](log, settings, originUid, inboundContext, heavyHitters) {

  override def increment(remoteAddress: Address, value: ActorRef, n: Long): Unit = {
    // don't count PromiseActorRefs as they are used only once and becomes a sort of memory leak
    if (!InternalActorRef.isTemporaryRef(value)) super.increment(remoteAddress, value, n)
  }

  override def decompress(tableVersion: Byte, idx: Int): OptionVal[ActorRef] =
    super.decompressInternal(tableVersion, idx, 0)

  override def advertiseCompressionTable(outboundContext: OutboundContext, table: CompressionTable[ActorRef]): Unit = {
    log.debug(
      s"Advertise {} compression [{}] to [{}#{}]",
      Logging.simpleName(getClass),
      table,
      outboundContext.remoteAddress,
      originUid)
    outboundContext.sendControl(
      CompressionProtocol.ActorRefCompressionAdvertisement(inboundContext.localAddress, table))
  }

  override protected def buildTableForAdvertisement(elements: Iterator[ActorRef]): Map[ActorRef, Int] = {
    val mb = Map.newBuilder[ActorRef, Int]
    var idx = 0
    elements.foreach {
      case ref: InternalActorRef =>
        if (!InternalActorRef.isTemporaryRef(ref)) {
          mb += ref -> idx
          idx += 1
        }
      case _ => // ignore others
    }
    mb.result()
  }
}

/**
 * INTERNAL API
 */
private[remote] final class InboundManifestCompression(
    log: LoggingAdapter,
    settings: ArterySettings.Compression,
    originUid: Long,
    inboundContext: InboundContext,
    heavyHitters: TopHeavyHitters[String])
    extends InboundCompression[String](log, settings, originUid, inboundContext, heavyHitters) {

  override def advertiseCompressionTable(outboundContext: OutboundContext, table: CompressionTable[String]): Unit = {
    log.debug(
      s"Advertise {} compression [{}] to [{}#{}]",
      Logging.simpleName(getClass),
      table,
      outboundContext.remoteAddress,
      originUid)
    outboundContext.sendControl(
      CompressionProtocol.ClassManifestCompressionAdvertisement(inboundContext.localAddress, table))
  }

  override def increment(remoteAddress: Address, value: String, n: Long): Unit =
    if (value != "") super.increment(remoteAddress, value, n)

  override def decompress(incomingTableVersion: Byte, idx: Int): OptionVal[String] =
    decompressInternal(incomingTableVersion, idx, 0)
}

/**
 * INTERNAL API
 */
private[remote] object InboundCompression {

  final val KeepOldTablesNumber = 3 // TODO could be configurable

  object Tables {
    def empty[T] =
      Tables(
        oldTables = List(DecompressionTable.disabled[T]),
        activeTable = DecompressionTable.empty[T],
        nextTable = DecompressionTable.empty[T].copy(version = 1),
        advertisementInProgress = None,
        keepOldTables = KeepOldTablesNumber)
  }

  /**
   * Encapsulates the various compression tables that Inbound Compression uses.
   *
   * @param oldTables is guaranteed to always have at-least one and at-most [[keepOldTables]] elements.
   *                  It starts with containing only a single "disabled" table (versioned as `DecompressionTable.DisabledVersion`),
   *                  and from there on continuously accumulates at most [[keepOldTables]] recently used tables.
   */
  final case class Tables[T](
      oldTables: List[DecompressionTable[T]],
      activeTable: DecompressionTable[T],
      nextTable: DecompressionTable[T],
      advertisementInProgress: Option[CompressionTable[T]],
      keepOldTables: Int) {

    def selectTable(version: Int): OptionVal[DecompressionTable[T]] = {
      if (activeTable.version == version) {
        if (ArterySettings.Compression.Debug)
          println(s"[compress] Found table [version: ${version}], was [ACTIVE]${activeTable}")
        OptionVal.Some(activeTable)
      } else {
        @tailrec def find(tables: List[DecompressionTable[T]]): OptionVal[DecompressionTable[T]] = {
          tables match {
            case Nil => OptionVal.None
            case t :: tail =>
              if (t.version == version) OptionVal.Some(t)
              else find(tail)
          }
        }
        val found = find(oldTables)

        if (ArterySettings.Compression.Debug) {
          found match {
            case OptionVal.Some(t) =>
              println(
                s"[compress] Found table [version: ${version}], was [OLD][${t}], old tables: [${oldTables.map(_.version)}]")
            case OptionVal.None =>
              println(s"[compress] Did not find table [version: ${version}], old tables: [${oldTables
                .map(_.version)}], activeTable: ${activeTable}, nextTable: ${nextTable}")
          }
        }
        found
      }
    }

    def startUsingNextTable(): Tables[T] = {
      def incrementTableVersion(version: Byte): Byte =
        if (version == 127) 0
        else (version + 1).toByte

      Tables(
        oldTables = (activeTable :: oldTables).take(keepOldTables),
        activeTable = nextTable,
        nextTable = DecompressionTable.empty[T].copy(version = incrementTableVersion(nextTable.version)),
        advertisementInProgress = None,
        keepOldTables = keepOldTables)
    }
  }

}

/**
 * INTERNAL API
 * Handles counting and detecting of heavy-hitters and compressing them via a table lookup.
 *
 * Access to this class must be externally synchronised (e.g. by accessing it from only Actors or a GraphStage etc).
 */
private[remote] abstract class InboundCompression[T >: Null](
    val log: LoggingAdapter,
    val settings: ArterySettings.Compression,
    val originUid: Long,
    inboundContext: InboundContext,
    val heavyHitters: TopHeavyHitters[T]) {

  private[this] var tables: InboundCompression.Tables[T] = InboundCompression.Tables.empty

  // We should not continue sending advertisements to an association that might be dead (not quarantined yet)
  @volatile private[this] var alive = true
  private[this] var resendCount = 0
  private[this] val maxResendCount = 3

  private[this] val cms = new CountMinSketch(16, 1024, System.currentTimeMillis().toInt)

  log.debug("Initializing {} for originUid [{}]", Logging.simpleName(getClass), originUid)

  /* ==== COMPRESSION ==== */

  /** Override and specialize if needed, for default compression logic delegate to 3-param overload */
  def decompress(incomingTableVersion: Byte, idx: Int): OptionVal[T]

  /**
   * Decompress given identifier into its original representation.
   * Passed in tableIds must only ever be in not-decreasing order (as old tables are dropped),
   * tableIds must not have gaps. If an "old" tableId is received the value will fail to be decompressed.
   *
   * @throws UnknownCompressedIdException if given id is not known, this may indicate a bug – such situation should not happen.
   */
  @tailrec final def decompressInternal(incomingTableVersion: Byte, idx: Int, attemptCounter: Int): OptionVal[T] = {
    // effectively should never loop more than once, to avoid infinite recursion blow up eagerly
    if (attemptCounter > 2)
      throw new IllegalStateException(
        s"Unable to decompress $idx from table $incomingTableVersion. Internal tables: $tables")

    val current = tables
    val activeVersion = current.activeTable.version
    def incomingVersionIsAdvertisementInProgress(incomingTableVersion: Byte): Boolean =
      current.advertisementInProgress.isDefined &&
      incomingTableVersion == current.advertisementInProgress.get.version

    if (incomingTableVersion == DecompressionTable.DisabledVersion) {
      // no compression, bail out early
      OptionVal.None
    } else {
      current.selectTable(version = incomingTableVersion) match {
        case OptionVal.Some(selectedTable) =>
          val value: T = selectedTable.get(idx)
          if (value != null) OptionVal.Some[T](value)
          else throw new UnknownCompressedIdException(idx)

        case _ if incomingVersionIsAdvertisementInProgress(incomingTableVersion) =>
          log.debug(
            "Received first value from originUid [{}] compressed using the advertised compression table, " +
            "flipping to it (version: {})",
            originUid,
            current.nextTable.version)
          confirmAdvertisement(incomingTableVersion, gaveUp = false)
          decompressInternal(incomingTableVersion, idx, attemptCounter + 1) // recurse

        case _ =>
          // which means that incoming version was > nextTable.version, which likely that
          // it is using a table that was built for previous incarnation of this system
          log.warning(
            "Inbound message from originUid [{}] is using unknown compression table version. " +
            "It may have been sent with compression table built for previous incarnation of this system. " +
            "Versions activeTable: {}, nextTable: {}, incomingTable: {}",
            originUid,
            activeVersion,
            current.nextTable.version,
            incomingTableVersion)
          OptionVal.None
      }
    }
  }

  final def confirmAdvertisement(tableVersion: Byte, gaveUp: Boolean): Unit = {
    tables.advertisementInProgress match {
      case Some(inProgress) if tableVersion == inProgress.version =>
        tables = tables.startUsingNextTable()
        log.debug(
          "{} compression table version [{}] for originUid [{}]",
          if (gaveUp) "Gave up" else "Confirmed",
          tableVersion,
          originUid)
      case Some(inProgress) if tableVersion != inProgress.version =>
        log.debug(
          "{} compression table version [{}] for originUid [{}] but other version in progress [{}]",
          if (gaveUp) "Gave up" else "Confirmed",
          tableVersion,
          originUid,
          inProgress.version)
      case _ =>
      // already confirmed
    }

  }

  /**
   * Add `n` occurrence for the given key and call `heavyHittedDetected` if element has become a heavy hitter.
   * Empty keys are omitted.
   */
  def increment(@nowarn("msg=never used") remoteAddress: Address, value: T, n: Long): Unit = {
    val count = cms.addObjectAndEstimateCount(value, n)
    addAndCheckIfheavyHitterDetected(value, count)
    alive = true
  }

  /** Mutates heavy hitters */
  private def addAndCheckIfheavyHitterDetected(value: T, count: Long): Boolean = {
    heavyHitters.update(value, count)
  }

  /* ==== TABLE ADVERTISEMENT ==== */

  /**
   * Entry point to advertising a new compression table.
   *
   * [1] First we must *hand the new table over to the Incoming compression side on this system*,
   * so it will not be used by someone else before "we" know about it in the Decoder.
   * [2] Then the table must be *advertised to the remote system*, and MAY start using it immediately
   *
   * It must be advertised to the other side so it can start using it in its outgoing compression.
   * Triggers compression table advertisement. May be triggered by schedule or manually, i.e. for testing.
   */
  private[remote] def runNextTableAdvertisement(): Unit = {
    if (ArterySettings.Compression.Debug) println(s"[compress] runNextTableAdvertisement, tables = $tables")
    tables.advertisementInProgress match {
      case None =>
        inboundContext.association(originUid) match {
          case OptionVal.Some(association) =>
            if (alive && association.isOrdinaryMessageStreamActive()) {
              val table = prepareCompressionAdvertisement(tables.nextTable.version)
              // TODO expensive, check if building the other way wouldn't be faster?
              val nextState = tables.copy(nextTable = table.invert, advertisementInProgress = Some(table))
              tables = nextState
              alive = false // will be set to true on first incoming message
              resendCount = 0
              advertiseCompressionTable(association, table)
            } else if (association.isOrdinaryMessageStreamActive()) {
              log.debug(
                "{} for originUid [{}] not changed, no need to advertise same.",
                Logging.simpleName(tables.activeTable),
                originUid)
            }

          case _ =>
            // otherwise it's too early, association not ready yet.
            // so we don't build the table since we would not be able to send it anyway.
            log.debug("No Association for originUid [{}] yet, unable to advertise compression table.", originUid)
        }

      case Some(inProgress) =>
        resendCount += 1
        if (resendCount <= maxResendCount) {
          // The ActorRefCompressionAdvertisement message is resent because it can be lost

          inboundContext.association(originUid) match {
            case OptionVal.Some(association) =>
              log.debug(
                "Advertisement in progress for originUid [{}] version [{}], resending [{}:{}]",
                originUid,
                inProgress.version,
                resendCount,
                maxResendCount)
              advertiseCompressionTable(association, inProgress) // resend
            case _ =>
          }
        } else {
          // give up, it might be dead
          log.debug(
            "Advertisement in progress for originUid [{}] version [{}] but no confirmation after retries.",
            originUid,
            inProgress.version)
          confirmAdvertisement(inProgress.version, gaveUp = true)
        }
    }
  }

  /**
   * Must be implemented by extending classes in order to send a `ControlMessage`
   * of appropriate type to the remote system in order to advertise the compression table to it.
   */
  protected def advertiseCompressionTable(association: OutboundContext, table: CompressionTable[T]): Unit

  private def prepareCompressionAdvertisement(nextTableVersion: Byte): CompressionTable[T] = {
    val mappings: Map[T, Int] = buildTableForAdvertisement(heavyHitters.iterator)
    CompressionTable(originUid, nextTableVersion, mappings)
  }

  protected def buildTableForAdvertisement(elements: Iterator[T]): Map[T, Int] = {
    // TODO optimised somewhat, check if still to heavy; could be encoded into simple array
    val mb = Map.newBuilder[T, Int]
    mb ++= elements.zipWithIndex
    mb.result()
  }

  override def toString =
    s"""${Logging.simpleName(getClass)}(countMinSketch: $cms, heavyHitters: $heavyHitters)"""

}

/**
 * INTERNAL API
 */
private[akka] final class UnknownCompressedIdException(id: Long)
    extends RuntimeException(
      s"Attempted de-compress unknown id [$id]! " +
      s"This could happen if this node has started a new ActorSystem bound to the same address as previously, " +
      s"and previous messages from a remote system were still in flight (using an old compression table). " +
      s"The remote system is expected to drop the compression table and this system will advertise a new one.")

/**
 * INTERNAL API
 *
 * Literarily, no compression!
 */
private[remote] case object NoInboundCompressions extends InboundCompressions {
  override def hitActorRef(originUid: Long, remote: Address, ref: ActorRef, n: Int): Unit = ()
  override def decompressActorRef(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[ActorRef] =
    if (idx == -1) throw new IllegalArgumentException("Attempted decompression of illegal compression id: -1")
    else OptionVal.None
  override def confirmActorRefCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = ()
  override def runNextActorRefAdvertisement(): Unit = ()

  override def hitClassManifest(originUid: Long, remote: Address, manifest: String, n: Int): Unit = ()
  override def decompressClassManifest(originUid: Long, tableVersion: Byte, idx: Int): OptionVal[String] =
    if (idx == -1) throw new IllegalArgumentException("Attempted decompression of illegal compression id: -1")
    else OptionVal.None
  override def confirmClassManifestCompressionAdvertisement(originUid: Long, tableVersion: Byte): Unit = ()
  override def runNextClassManifestAdvertisement(): Unit = ()

  override def currentOriginUids: Set[Long] = Set.empty

  override def close(originUid: Long): Unit = ()
}
