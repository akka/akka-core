/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.net.ConnectException
import java.util.Queue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import org.agrona.concurrent.ManyToOneConcurrentArrayQueue

import akka.Done
import akka.NotUsed
import akka.actor.ActorRef
import akka.actor.ActorSelectionMessage
import akka.actor.Address
import akka.actor.Cancellable
import akka.actor.Dropped
import akka.dispatch.Dispatchers
import akka.dispatch.sysmsg.DeathWatchNotification
import akka.dispatch.sysmsg.SystemMessage
import akka.dispatch.sysmsg.Unwatch
import akka.event.Logging
import akka.remote.DaemonMsgCreate
import akka.remote.PriorityMessage
import akka.remote.RemoteActorRef
import akka.remote.RemoteLogMarker
import akka.remote.UniqueAddress
import akka.remote.artery.ArteryTransport.AeronTerminated
import akka.remote.artery.ArteryTransport.ShuttingDown
import akka.remote.artery.Association.UidCollisionException
import akka.remote.artery.Encoder.OutboundCompressionAccess
import akka.remote.artery.InboundControlJunction.ControlMessageSubject
import akka.remote.artery.OutboundControlJunction.OutboundControlIngress
import akka.remote.artery.OutboundHandshake.HandshakeTimeoutException
import akka.remote.artery.SystemMessageDelivery.ClearSystemMessageDelivery
import akka.remote.artery.aeron.AeronSink.GaveUpMessageException
import akka.remote.artery.compress.CompressionTable
import akka.stream.AbruptTerminationException
import akka.stream.KillSwitches
import akka.stream.Materializer
import akka.stream.SharedKillSwitch
import akka.stream.StreamTcpException
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.MergeHub
import akka.stream.scaladsl.Source
import akka.util.OptionVal
import akka.util.PrettyDuration._
import akka.util.WildcardIndex

/**
 * INTERNAL API
 */
private[remote] object Association {
  sealed trait QueueWrapper extends SendQueue.ProducerApi[OutboundEnvelope] {
    def queue: Queue[OutboundEnvelope]
  }

  final case class QueueWrapperImpl(queue: Queue[OutboundEnvelope]) extends QueueWrapper {
    override def offer(message: OutboundEnvelope): Boolean = queue.offer(message)

    override def isEnabled: Boolean = true
  }

  object DisabledQueueWrapper extends QueueWrapper {
    override def queue: java.util.Queue[OutboundEnvelope] =
      throw new UnsupportedOperationException("The Queue is disabled")

    override def offer(message: OutboundEnvelope): Boolean =
      throw new UnsupportedOperationException("The method offer() is illegal on a disabled queue")

    override def isEnabled: Boolean = false
  }

  object RemovedQueueWrapper extends QueueWrapper {
    override def queue: java.util.Queue[OutboundEnvelope] =
      throw new UnsupportedOperationException("The Queue is removed")

    override def offer(message: OutboundEnvelope): Boolean = false

    override def isEnabled: Boolean = false
  }

  final case class LazyQueueWrapper(queue: Queue[OutboundEnvelope], materialize: () => Unit) extends QueueWrapper {
    private val onlyOnce = new AtomicBoolean

    def runMaterialize(): Unit = {
      if (onlyOnce.compareAndSet(false, true))
        materialize()
    }

    override def offer(message: OutboundEnvelope): Boolean = {
      runMaterialize()
      queue.offer(message)
    }

    override def isEnabled: Boolean = true
  }

  final val ControlQueueIndex = 0
  final val LargeQueueIndex = 1
  final val OrdinaryQueueIndex = 2

  sealed trait StopSignal
  case object OutboundStreamStopIdleSignal extends RuntimeException("") with StopSignal with NoStackTrace
  case object OutboundStreamStopQuarantinedSignal extends RuntimeException("") with StopSignal with NoStackTrace

  final case class OutboundStreamMatValues(
      streamKillSwitch: OptionVal[SharedKillSwitch],
      completed: Future[Done],
      stopping: OptionVal[StopSignal])

  final class UidCollisionException(message: String) extends IllegalArgumentException(message) with NoStackTrace
}

/**
 * INTERNAL API
 *
 * Thread-safe, mutable holder for association state. Main entry point for remote destined message to a specific
 * remote address.
 */
private[remote] class Association(
    val transport: ArteryTransport,
    val materializer: Materializer,
    val controlMaterializer: Materializer,
    override val remoteAddress: Address,
    override val controlSubject: ControlMessageSubject,
    largeMessageDestinations: WildcardIndex[NotUsed],
    priorityMessageDestinations: WildcardIndex[NotUsed],
    outboundEnvelopePool: ObjectPool[ReusableOutboundEnvelope])
    extends AbstractAssociation
    with OutboundContext {
  import Association._

  require(remoteAddress.port.nonEmpty)

  private val log = Logging.withMarker(transport.system, classOf[Association])

  override def settings = transport.settings
  private def advancedSettings = transport.settings.Advanced
  private val deathWatchNotificationFlushEnabled = advancedSettings.DeathWatchNotificationFlushTimeout > Duration.Zero && transport.provider.settings.HasCluster

  private val restartCounter =
    new RestartCounter(advancedSettings.OutboundMaxRestarts, advancedSettings.OutboundRestartTimeout)

  // We start with the raw wrapped queue and then it is replaced with the materialized value of
  // the `SendQueue` after materialization. Using same underlying queue. This makes it possible to
  // start sending (enqueuing) to the Association immediate after construction.

  def createQueue(capacity: Int, queueIndex: Int): Queue[OutboundEnvelope] = {
    val linked = queueIndex == ControlQueueIndex || queueIndex == LargeQueueIndex
    if (linked)
      new LinkedBlockingQueue[OutboundEnvelope](capacity) // less memory than ManyToOneConcurrentArrayQueue
    else
      new ManyToOneConcurrentArrayQueue[OutboundEnvelope](capacity)
  }

  private val outboundLanes = advancedSettings.OutboundLanes
  private val controlQueueSize = advancedSettings.OutboundControlQueueSize
  private val queueSize = advancedSettings.OutboundMessageQueueSize
  private val largeQueueSize = advancedSettings.OutboundLargeMessageQueueSize

  private[this] val queues: Array[SendQueue.ProducerApi[OutboundEnvelope]] = new Array(2 + outboundLanes)
  queues(ControlQueueIndex) = QueueWrapperImpl(createQueue(controlQueueSize, ControlQueueIndex)) // control stream
  queues(LargeQueueIndex) =
    if (transport.largeMessageChannelEnabled) // large messages stream
      QueueWrapperImpl(createQueue(largeQueueSize, LargeQueueIndex))
    else
      DisabledQueueWrapper

  (0 until outboundLanes).foreach { i =>
    queues(OrdinaryQueueIndex + i) = QueueWrapperImpl(createQueue(queueSize, OrdinaryQueueIndex + i)) // ordinary messages stream
  }
  @volatile private[this] var queuesVisibility = false

  private def controlQueue: SendQueue.ProducerApi[OutboundEnvelope] = queues(ControlQueueIndex)

  @volatile private[this] var _outboundControlIngress: OptionVal[OutboundControlIngress] = OptionVal.None
  @volatile private[this] var materializing = new CountDownLatch(1)
  @volatile private[this] var outboundCompressionAccess: Vector[OutboundCompressionAccess] = Vector.empty

  // keyed by stream queue index
  private[this] val streamMatValues = new AtomicReference(Map.empty[Int, OutboundStreamMatValues])

  private[this] val idleTimer = new AtomicReference[Option[Cancellable]](None)
  private[this] val stopQuarantinedTimer = new AtomicReference[Option[Cancellable]](None)

  private[remote] def changeActorRefCompression(table: CompressionTable[ActorRef]): Future[Done] =
    updateOutboundCompression(c => c.changeActorRefCompression(table))

  private[remote] def changeClassManifestCompression(table: CompressionTable[String]): Future[Done] =
    updateOutboundCompression(c => c.changeClassManifestCompression(table))

  private def clearOutboundCompression(): Future[Done] =
    updateOutboundCompression(c => c.clearCompression())

  private def updateOutboundCompression(action: OutboundCompressionAccess => Future[Done]): Future[Done] = {
    implicit val ec = transport.system.dispatchers.internalDispatcher
    val c = outboundCompressionAccess
    if (c.isEmpty) Future.successful(Done)
    else if (c.size == 1) action(c.head)
    else Future.sequence(c.map(action(_))).map(_ => Done)
  }

  private def clearInboundCompression(originUid: Long): Unit =
    transport.inboundCompressionAccess match {
      case OptionVal.Some(access) => access.closeCompressionFor(originUid)
      case _                      => // do nothing
    }

  private def deadletters = transport.system.deadLetters

  def outboundControlIngress: OutboundControlIngress = {
    _outboundControlIngress match {
      case OptionVal.Some(o) => o
      case _ =>
        controlQueue match {
          case w: LazyQueueWrapper => w.runMaterialize()
          case _                   =>
        }
        // the outboundControlIngress may be accessed before the stream is materialized
        // using CountDownLatch to make sure that materialization is completed
        materializing.await(10, TimeUnit.SECONDS)
        _outboundControlIngress match {
          case OptionVal.Some(o) => o
          case _ =>
            if (transport.isShutdown || isRemovedAfterQuarantined()) throw ShuttingDown
            else throw new IllegalStateException(s"outboundControlIngress for [$remoteAddress] not initialized yet")
        }
    }
  }

  override def localAddress: UniqueAddress = transport.localAddress

  /**
   * Holds reference to shared state of Association - *access only via helper methods*
   */
  @volatile
  private[artery] var _sharedStateDoNotCallMeDirectly: AssociationState = AssociationState()

  /**
   * Helper method for access to underlying state via Unsafe
   *
   * @param oldState Previous state
   * @param newState Next state on transition
   * @return Whether the previous state matched correctly
   */
  @inline
  private[artery] def swapState(oldState: AssociationState, newState: AssociationState): Boolean =
    AbstractAssociation.sharedStateHandle.compareAndSet(this, oldState, newState)

  /**
   * @return Reference to current shared state
   */
  def associationState: AssociationState =
    AbstractAssociation.sharedStateHandle.getVolatile(this).asInstanceOf[AssociationState]

  def setControlIdleKillSwitch(killSwitch: OptionVal[SharedKillSwitch]): Unit = {
    val current = associationState
    swapState(current, current.withControlIdleKillSwitch(killSwitch))
  }

  def completeHandshake(peer: UniqueAddress): Future[Done] = {
    require(
      remoteAddress == peer.address,
      s"wrong remote address in completeHandshake, got ${peer.address}, expected $remoteAddress")
    val current = associationState

    current.uniqueRemoteAddress() match {
      case Some(`peer`) =>
        // handshake already completed
        Future.successful(Done)
      case _ =>
        // clear outbound compression, it's safe to do that several times if someone else
        // completes handshake at same time, but it's important to clear it before
        implicit val ec = transport.system.dispatchers.internalDispatcher
        clearOutboundCompression().map { _ =>
          current.completeUniqueRemoteAddress(peer)
          current.uniqueRemoteAddress() match {
            case Some(`peer`) =>
            // our value
            case _ =>
              val newState = current.newIncarnation(peer)
              if (swapState(current, newState)) {
                current.uniqueRemoteAddress() match {
                  case Some(old) =>
                    cancelStopQuarantinedTimer()
                    log.debug(
                      "Incarnation {} of association to [{}] with new UID [{}] (old UID [{}])",
                      newState.incarnation,
                      peer.address,
                      peer.uid,
                      old.uid)
                    clearInboundCompression(old.uid)
                  case None =>
                  // Failed, nothing to do
                }
                // if swap failed someone else completed before us, and that is fine
              }
          }
          Done
        }
    }
  }

  // OutboundContext
  override def sendControl(message: ControlMessage): Unit = {
    try {
      if (!transport.isShutdown && !isRemovedAfterQuarantined()) {
        if (associationState.isQuarantined()) {
          log.debug("Send control message [{}] to quarantined [{}]", Logging.messageClassName(message), remoteAddress)
          setupStopQuarantinedTimer()
        }
        outboundControlIngress.sendControlMessage(message)
      }
    } catch {
      case ShuttingDown => // silence it
    }
  }

  def send(message: Any, sender: OptionVal[ActorRef], recipient: OptionVal[RemoteActorRef]): Unit = {

    def createOutboundEnvelope(): OutboundEnvelope =
      outboundEnvelopePool.acquire().init(recipient, message.asInstanceOf[AnyRef], sender)

    // volatile read to see latest queue array
    @nowarn("msg=never used")
    val unused = queuesVisibility

    def dropped(queueIndex: Int, qSize: Int, env: OutboundEnvelope): Unit = {
      val removed = isRemovedAfterQuarantined()
      if (removed) recipient match {
        case OptionVal.Some(ref) => ref.cachedAssociation = null // don't use this Association instance any more
        case _                   =>
      }
      val reason =
        if (removed) "Due to removed unused quarantined association"
        else s"Due to overflow of send queue, size [$qSize]"
      transport.system.eventStream
        .publish(Dropped(message, reason, env.sender.getOrElse(ActorRef.noSender), recipient.getOrElse(deadletters)))

      RemotingFlightRecorder.transportSendQueueOverflow(queueIndex)
      deadletters ! env
    }

    def shouldSendUnwatch(): Boolean =
      !transport.provider.settings.HasCluster || !transport.system.isTerminating()

    def shouldSendDeathWatchNotification(d: DeathWatchNotification): Boolean =
      d.addressTerminated || !transport.provider.settings.HasCluster || !transport.system.isTerminating()

    def sendSystemMessage(outboundEnvelope: OutboundEnvelope): Unit = {
      outboundEnvelope.message match {
        case u: Unwatch if shouldSendUnwatch() =>
          log.debug(
            "Not sending Unwatch of {} to {} because it will be notified when this member " +
            "has been removed from Cluster.",
            u.watcher,
            u.watchee)
        case d: DeathWatchNotification if !shouldSendDeathWatchNotification(d) =>
          log.debug(
            "Not sending DeathWatchNotification of {} to {} because it will be notified when this member " +
            "has been removed from Cluster.",
            d.actor,
            outboundEnvelope.recipient.getOrElse("unknown"))
        case _ =>
          if (!controlQueue.offer(outboundEnvelope)) {
            quarantine(reason = s"Due to overflow of control queue, size [$controlQueueSize]")
            dropped(ControlQueueIndex, controlQueueSize, outboundEnvelope)
          }
      }
    }

    val state = associationState
    val quarantined = state.isQuarantined()
    val messageIsClearSystemMessageDelivery = message.isInstanceOf[ClearSystemMessageDelivery]

    // allow ActorSelectionMessage to pass through quarantine, to be able to establish interaction with new system
    if (message.isInstanceOf[ActorSelectionMessage] || !quarantined || messageIsClearSystemMessageDelivery) {
      if (quarantined && !messageIsClearSystemMessageDelivery) {
        log.debug(
          "Quarantine piercing attempt with message [{}] to [{}]",
          Logging.messageClassName(message),
          recipient.getOrElse(""))
        setupStopQuarantinedTimer()
      }
      try {
        val outboundEnvelope = createOutboundEnvelope()
        message match {
          case d: DeathWatchNotification if deathWatchNotificationFlushEnabled && shouldSendDeathWatchNotification(d) =>
            val flushingPromise = Promise[Done]()
            log.debug("Delaying death watch notification until flush has been sent. {}", d)
            transport.system.systemActorOf(
              FlushBeforeDeathWatchNotification
                .props(flushingPromise, settings.Advanced.DeathWatchNotificationFlushTimeout, this)
                .withDispatcher(Dispatchers.InternalDispatcherId),
              FlushBeforeDeathWatchNotification.nextName())
            flushingPromise.future.onComplete { _ =>
              log.debug("Sending death watch notification as flush is complete. {}", d)
              sendSystemMessage(outboundEnvelope)
            }(materializer.executionContext)
          case _: SystemMessage =>
            sendSystemMessage(outboundEnvelope)
          case ActorSelectionMessage(_: PriorityMessage, _, _) | _: ControlMessage | _: ClearSystemMessageDelivery =>
            // ActorSelectionMessage with PriorityMessage is used by cluster and remote failure detector heartbeating
            if (!controlQueue.offer(outboundEnvelope)) {
              dropped(ControlQueueIndex, controlQueueSize, outboundEnvelope)
            }
          case _: DaemonMsgCreate =>
            // DaemonMsgCreate is not a SystemMessage, but must be sent over the control stream because
            // remote deployment process depends on message ordering for DaemonMsgCreate and Watch messages.
            // First ordinary message may arrive earlier but then the resolve in the Decoder is retried
            // so that the first message can be delivered after the remote actor has been created.
            if (!controlQueue.offer(outboundEnvelope))
              dropped(ControlQueueIndex, controlQueueSize, outboundEnvelope)
          case _ =>
            val queueIndex = selectQueue(recipient)
            val queue = queues(queueIndex)
            val offerOk = queue.offer(outboundEnvelope)
            if (!offerOk)
              dropped(queueIndex, queueSize, outboundEnvelope)
        }
      } catch {
        case ShuttingDown => // silence it
      }
    } else if (log.isDebugEnabled)
      log.debug(
        "Dropping message [{}] from [{}] to [{}] due to quarantined system [{}]",
        Logging.messageClassName(message),
        sender.getOrElse(deadletters),
        recipient.getOrElse(recipient),
        remoteAddress)
  }

  private def selectQueue(recipient: OptionVal[RemoteActorRef]): Int = {
    recipient match {
      case OptionVal.Some(r) =>
        r.cachedSendQueueIndex match {
          case -1 =>
            // only happens when messages are sent to new remote destination
            // and is then cached on the RemoteActorRef
            val elements = r.path.elements
            val idx =
              if (priorityMessageDestinations.find(elements).isDefined) {
                log.debug("Using priority message stream for {}", r.path)
                ControlQueueIndex
              } else if (transport.largeMessageChannelEnabled && largeMessageDestinations.find(elements).isDefined) {
                log.debug("Using large message stream for {}", r.path)
                LargeQueueIndex
              } else if (outboundLanes == 1) {
                OrdinaryQueueIndex
              } else {
                // select lane based on destination, to preserve message order
                OrdinaryQueueIndex + (math.abs(r.path.uid % outboundLanes))
              }
            r.cachedSendQueueIndex = idx
            idx
          case idx => idx
        }

      case _ =>
        OrdinaryQueueIndex
    }
  }

  override def isOrdinaryMessageStreamActive(): Boolean =
    isStreamActive(OrdinaryQueueIndex)

  def isStreamActive(queueIndex: Int): Boolean = {
    queues(queueIndex) match {
      case _: LazyQueueWrapper  => false
      case DisabledQueueWrapper => false
      case RemovedQueueWrapper  => false
      case _                    => true
    }
  }

  def sendTerminationHint(replyTo: ActorRef): Int = {
    log.debug("Sending ActorSystemTerminating to all queues")
    sendToAllQueues(ActorSystemTerminating(localAddress), replyTo, excludeControlQueue = false)
  }

  def sendFlush(replyTo: ActorRef, excludeControlQueue: Boolean): Int =
    sendToAllQueues(Flush, replyTo, excludeControlQueue)

  def sendToAllQueues(msg: ControlMessage, replyTo: ActorRef, excludeControlQueue: Boolean): Int = {
    if (!associationState.isQuarantined()) {
      var sent = 0
      val queuesIter = if (excludeControlQueue) queues.iterator.drop(1) else queues.iterator
      queuesIter.filter(q => q.isEnabled && !q.isInstanceOf[LazyQueueWrapper]).foreach { queue =>
        try {
          val envelope = outboundEnvelopePool.acquire().init(OptionVal.None, msg, OptionVal.Some(replyTo))

          queue.offer(envelope)
          sent += 1
        } catch {
          case ShuttingDown => // can be thrown if `offer` triggers new materialization
        }
      }
      sent
    } else 0
  }

  // OutboundContext
  override def quarantine(reason: String): Unit = {
    val uid = associationState.uniqueRemoteAddress().map(_.uid)
    quarantine(reason, uid, harmless = false)
  }

  @tailrec final def quarantine(reason: String, uid: Option[Long], harmless: Boolean): Unit = {
    uid match {
      case Some(u) =>
        val current = associationState
        current.uniqueRemoteAddress() match {
          case Some(peer) if peer.uid == u =>
            if (!current.isQuarantined(u)) {
              val newState = current.newQuarantined(harmless)
              if (swapState(current, newState)) {
                // quarantine state change was performed
                if (harmless) {
                  log.info(
                    "Association to [{}] having UID [{}] has been stopped. Harmless quarantine. " +
                    "All messages to this UID will be delivered to dead letters. Reason: {}",
                    remoteAddress,
                    u,
                    reason)
                  transport.system.eventStream
                    .publish(GracefulShutdownQuarantinedEvent(UniqueAddress(remoteAddress, u), reason))
                } else {
                  log.warning(
                    RemoteLogMarker.quarantine(remoteAddress, Some(u)),
                    "Association to [{}] with UID [{}] is irrecoverably failed. UID is now quarantined and all " +
                    "messages to this UID will be delivered to dead letters. " +
                    "Remote ActorSystem must be restarted to recover from this situation. Reason: {}",
                    remoteAddress,
                    u,
                    reason)
                  transport.system.eventStream.publish(QuarantinedEvent(UniqueAddress(remoteAddress, u)))
                }
                RemotingFlightRecorder.transportQuarantined(remoteAddress, u)
                clearOutboundCompression()
                clearInboundCompression(u)
                // end delivery of system messages to that incarnation after this point
                send(ClearSystemMessageDelivery(current.incarnation), OptionVal.None, OptionVal.None)
                if (!harmless) {
                  // try to tell the other system that we have quarantined it
                  log.info("Sending Quarantined to [{}]", peer)
                  sendControl(Quarantined(localAddress, peer))
                }
                setupStopQuarantinedTimer()
              } else
                quarantine(reason, uid, harmless) // recursive
            }
          case Some(peer) =>
            log.info(
              RemoteLogMarker.quarantine(remoteAddress, Some(u)),
              "Quarantine of [{}] ignored due to non-matching UID, quarantine requested for [{}] but current is [{}]. Reason: {}",
              remoteAddress,
              u,
              peer.uid,
              reason)
            send(ClearSystemMessageDelivery(current.incarnation - 1), OptionVal.None, OptionVal.None)
          case None =>
            log.info(
              RemoteLogMarker.quarantine(remoteAddress, Some(u)),
              "Quarantine of [{}] ignored because handshake not completed, quarantine request was for old incarnation. Reason: {}",
              remoteAddress,
              reason)
        }
      case None =>
        log.warning(
          RemoteLogMarker.quarantine(remoteAddress, None),
          "Quarantine of [{}] ignored because unknown UID. Reason: {}",
          remoteAddress,
          reason)
    }

  }

  /**
   * After calling this no messages can be sent with this Association instance
   */
  def removedAfterQuarantined(): Unit = {
    if (!isRemovedAfterQuarantined()) {
      RemotingFlightRecorder.transportRemoveQuarantined(remoteAddress)
      queues(ControlQueueIndex) = RemovedQueueWrapper

      if (transport.largeMessageChannelEnabled)
        queues(LargeQueueIndex) = RemovedQueueWrapper

      (0 until outboundLanes).foreach { i =>
        queues(OrdinaryQueueIndex + i) = RemovedQueueWrapper
      }
      queuesVisibility = true // volatile write for visibility of the queues array

      // cleanup
      _outboundControlIngress = OptionVal.None
      outboundCompressionAccess = Vector.empty
      cancelAllTimers()
      abortQuarantined()

      log.info("Unused association to [{}] removed after quarantine", remoteAddress)
    }
  }

  def isRemovedAfterQuarantined(): Boolean =
    queues(ControlQueueIndex) == RemovedQueueWrapper

  private def cancelStopQuarantinedTimer(): Unit = {
    val current = stopQuarantinedTimer.get
    current.foreach(_.cancel())
    stopQuarantinedTimer.compareAndSet(current, None)
  }

  private def setupStopQuarantinedTimer(): Unit = {
    cancelStopQuarantinedTimer()
    stopQuarantinedTimer.set(Some(transport.system.scheduler.scheduleOnce(advancedSettings.StopQuarantinedAfterIdle) {
      if (associationState.isQuarantined())
        abortQuarantined()
    }(transport.system.dispatchers.internalDispatcher)))
  }

  private def abortQuarantined(): Unit = {
    cancelIdleTimer()
    streamMatValues.get.foreach {
      case (queueIndex, OutboundStreamMatValues(killSwitch, _, _)) =>
        killSwitch match {
          case OptionVal.Some(k) =>
            setStopReason(queueIndex, OutboundStreamStopQuarantinedSignal)
            clearStreamKillSwitch(queueIndex, k)
            k.abort(OutboundStreamStopQuarantinedSignal)
          case _ => // already aborted
        }
    }
  }

  private def cancelIdleTimer(): Unit = {
    val current = idleTimer.get
    current.foreach(_.cancel())
    idleTimer.compareAndSet(current, None)
  }

  private def setupIdleTimer(): Unit = {
    if (idleTimer.get.isEmpty) {
      val StopIdleOutboundAfter = settings.Advanced.StopIdleOutboundAfter
      val QuarantineIdleOutboundAfter = settings.Advanced.QuarantineIdleOutboundAfter
      val interval = StopIdleOutboundAfter / 2
      val initialDelay = settings.Advanced.Tcp.ConnectionTimeout.max(StopIdleOutboundAfter) + 1.second
      val task =
        transport.system.scheduler.scheduleWithFixedDelay(initialDelay, interval) { () =>
          val lastUsedDurationNanos = System.nanoTime() - associationState.lastUsedTimestamp.get
          if (lastUsedDurationNanos >= QuarantineIdleOutboundAfter.toNanos && !associationState.isQuarantined()) {
            // If idle longer than quarantine-idle-outbound-after and the low frequency HandshakeReq
            // doesn't get through it will be quarantined to cleanup lingering associations to crashed systems.
            quarantine(s"Idle longer than quarantine-idle-outbound-after [${QuarantineIdleOutboundAfter.pretty}]")
            associationState.uniqueRemoteAddressState() match {
              case AssociationState.UidQuarantined => // quarantined as expected
              case AssociationState.UidKnown       => // must be new uid, keep as is
              case AssociationState.UidUnknown =>
                val newLastUsedDurationNanos = System.nanoTime() - associationState.lastUsedTimestamp.get
                // quarantine ignored due to unknown UID, have to stop this task anyway
                if (newLastUsedDurationNanos >= QuarantineIdleOutboundAfter.toNanos)
                  abortQuarantined() // quarantine ignored due to unknown UID, have to stop this task anyway
            }
          } else if (lastUsedDurationNanos >= StopIdleOutboundAfter.toNanos) {
            streamMatValues.get.foreach {
              case (queueIndex, OutboundStreamMatValues(streamKillSwitch, _, stopping)) =>
                if (isStreamActive(queueIndex) && stopping.isEmpty) {
                  if (queueIndex != ControlQueueIndex) {
                    streamKillSwitch match {
                      case OptionVal.Some(k) =>
                        // for non-control streams we can stop the entire stream
                        log.info("Stopping idle outbound stream [{}] to [{}]", queueIndex, remoteAddress)
                        RemotingFlightRecorder.transportStopIdleOutbound(remoteAddress, queueIndex)
                        setStopReason(queueIndex, OutboundStreamStopIdleSignal)
                        clearStreamKillSwitch(queueIndex, k)
                        k.abort(OutboundStreamStopIdleSignal)
                      case _ => // already aborted
                    }

                  } else {
                    // only stop the transport parts of the stream because SystemMessageDelivery stage has
                    // state (seqno) and system messages might be sent at the same time
                    associationState.controlIdleKillSwitch match {
                      case OptionVal.Some(killSwitch) =>
                        log.info("Stopping idle outbound control stream to [{}]", remoteAddress)
                        RemotingFlightRecorder.transportStopIdleOutbound(remoteAddress, queueIndex)
                        setControlIdleKillSwitch(OptionVal.None)
                        killSwitch.abort(OutboundStreamStopIdleSignal)
                      case _ => // already stopped
                    }
                  }
                }
            }
          }
        }(transport.system.dispatcher)

      if (!idleTimer.compareAndSet(None, Some(task))) {
        // another thread did same thing and won
        task.cancel()
      }
    }
  }

  private def cancelAllTimers(): Unit = {
    cancelIdleTimer()
    cancelStopQuarantinedTimer()
  }

  private def sendToDeadLetters[T](pending: Vector[OutboundEnvelope]): Unit = {
    pending.foreach(transport.system.deadLetters ! _)
  }

  /**
   * Called once after construction when the `Association` instance
   * wins the CAS in the `AssociationRegistry`. It will materialize
   * the streams. It is possible to sending (enqueuing) to the association
   * before this method is called.
   *
   * @throws ShuttingDown if called while the transport is shutting down
   */
  def associate(): Unit = {
    if (!controlQueue.isInstanceOf[QueueWrapper])
      throw new IllegalStateException("associate() must only be called once")
    runOutboundStreams()
  }

  private def runOutboundStreams(): Unit = {

    // it's important to materialize the outboundControl stream first,
    // so that outboundControlIngress is ready when stages for all streams start
    runOutboundControlStream()
    runOutboundOrdinaryMessagesStream()

    if (transport.largeMessageChannelEnabled)
      runOutboundLargeMessagesStream()
  }

  private def runOutboundControlStream(): Unit = {
    if (transport.isShutdown) throw ShuttingDown
    log.debug("Starting outbound control stream to [{}]", remoteAddress)

    val wrapper = getOrCreateQueueWrapper(ControlQueueIndex, queueSize)
    queues(ControlQueueIndex) = wrapper // use new underlying queue immediately for restarts
    queuesVisibility = true // volatile write for visibility of the queues array

    val streamKillSwitch = KillSwitches.shared("outboundControlStreamKillSwitch")

    def sendQueuePostStop[T](pending: Vector[OutboundEnvelope]): Unit = {
      sendToDeadLetters(pending)
      val systemMessagesCount = pending.count(env => env.message.isInstanceOf[SystemMessage])
      if (systemMessagesCount > 0)
        quarantine(s"SendQueue stopped with [$systemMessagesCount] pending system messages.")
    }

    val (queueValue, (control, completed)) =
      Source
        .fromGraph(new SendQueue[OutboundEnvelope](sendQueuePostStop))
        .via(streamKillSwitch.flow)
        .toMat(transport.outboundControl(this))(Keep.both)
        .run()(materializer)

    queueValue.inject(wrapper.queue)
    // replace with the materialized value, still same underlying queue
    queues(ControlQueueIndex) = queueValue
    queuesVisibility = true // volatile write for visibility of the queues array
    _outboundControlIngress = OptionVal.Some(control)
    materializing.countDown()

    updateStreamMatValues(ControlQueueIndex, streamKillSwitch, completed)
    setupIdleTimer()
    attachOutboundStreamRestart(
      "Outbound control stream",
      ControlQueueIndex,
      controlQueueSize,
      completed,
      () => runOutboundControlStream())
  }

  private def getOrCreateQueueWrapper(queueIndex: Int, capacity: Int): QueueWrapper = {
    @nowarn("msg=never used")
    val unused = queuesVisibility // volatile read to see latest queues array
    queues(queueIndex) match {
      case existing: QueueWrapper => existing
      case _                      =>
        // use new queue for restarts
        QueueWrapperImpl(createQueue(capacity, queueIndex))
    }
  }

  private def runOutboundOrdinaryMessagesStream(): Unit = {
    if (transport.isShutdown) throw ShuttingDown

    val streamKillSwitch = KillSwitches.shared("outboundMessagesKillSwitch")

    if (outboundLanes == 1) {
      log.debug("Starting outbound message stream to [{}]", remoteAddress)
      val queueIndex = OrdinaryQueueIndex
      val wrapper = getOrCreateQueueWrapper(queueIndex, queueSize)
      queues(queueIndex) = wrapper // use new underlying queue immediately for restarts
      queuesVisibility = true // volatile write for visibility of the queues array

      val (queueValue, _, changeCompression, completed) =
        Source
          .fromGraph(new SendQueue[OutboundEnvelope](sendToDeadLetters))
          .via(streamKillSwitch.flow)
          .viaMat(transport.outboundTestFlow(this))(Keep.both)
          .toMat(transport.outbound(this))({ case ((a, b), (c, d)) => (a, b, c, d) }) // "keep all, exploded"
          .run()(materializer)

      queueValue.inject(wrapper.queue)
      // replace with the materialized value, still same underlying queue
      queues(queueIndex) = queueValue
      queuesVisibility = true // volatile write for visibility of the queues array
      outboundCompressionAccess = Vector(changeCompression)

      updateStreamMatValues(OrdinaryQueueIndex, streamKillSwitch, completed)
      attachOutboundStreamRestart(
        "Outbound message stream",
        OrdinaryQueueIndex,
        queueSize,
        completed,
        () => runOutboundOrdinaryMessagesStream())

    } else {
      log.debug("Starting outbound message stream to [{}] with [{}] lanes", remoteAddress, outboundLanes)
      val wrappers = (0 until outboundLanes).map { i =>
        val wrapper = getOrCreateQueueWrapper(OrdinaryQueueIndex + i, queueSize)
        queues(OrdinaryQueueIndex + i) = wrapper // use new underlying queue immediately for restarts
        queuesVisibility = true // volatile write for visibility of the queues array
        wrapper
      }.toVector

      val lane = Source
        .fromGraph(new SendQueue[OutboundEnvelope](sendToDeadLetters))
        .via(streamKillSwitch.flow)
        .via(transport.outboundTestFlow(this))
        .viaMat(transport.outboundLane(this))(Keep.both)
        .watchTermination()(Keep.both)
        // recover to avoid error logging by MergeHub
        .recoverWithRetries(-1, { case _: Throwable => Source.empty })
        .mapMaterializedValue {
          case ((q, c), w) => (q, c, w)
        }

      val (mergeHub, transportSinkCompleted) = MergeHub
        .source[EnvelopeBuffer]
        .via(streamKillSwitch.flow)
        .toMat(transport.outboundTransportSink(this))(Keep.both)
        .run()(materializer)

      val values: Vector[(SendQueue.QueueValue[OutboundEnvelope], Encoder.OutboundCompressionAccess, Future[Done])] =
        (0 until outboundLanes).iterator
          .map { _ =>
            lane.to(mergeHub).run()(materializer)
          }
          .to(Vector)

      val (queueValues, compressionAccessValues, laneCompletedValues) = values.unzip3

      implicit val ec = transport.system.dispatchers.internalDispatcher

      // tear down all parts if one part fails or completes
      Future.firstCompletedOf(laneCompletedValues).failed.foreach { reason =>
        streamKillSwitch.abort(reason)
      }
      (laneCompletedValues :+ transportSinkCompleted).foreach(_.foreach { _ =>
        streamKillSwitch.shutdown()
      })

      val allCompleted = Future.sequence(laneCompletedValues).flatMap(_ => transportSinkCompleted)

      queueValues.zip(wrappers).zipWithIndex.foreach {
        case ((q, w), i) =>
          q.inject(w.queue)
          queues(OrdinaryQueueIndex + i) = q // replace with the materialized value, still same underlying queue
      }
      queuesVisibility = true // volatile write for visibility of the queues array

      outboundCompressionAccess = compressionAccessValues

      attachOutboundStreamRestart(
        "Outbound message stream",
        OrdinaryQueueIndex,
        queueSize,
        allCompleted,
        () => runOutboundOrdinaryMessagesStream())
    }
  }

  private def runOutboundLargeMessagesStream(): Unit = {
    if (transport.isShutdown) throw ShuttingDown
    log.debug("Starting outbound large message stream to [{}]", remoteAddress)
    val wrapper = getOrCreateQueueWrapper(LargeQueueIndex, largeQueueSize)
    queues(LargeQueueIndex) = wrapper // use new underlying queue immediately for restarts
    queuesVisibility = true // volatile write for visibility of the queues array

    val streamKillSwitch = KillSwitches.shared("outboundLargeMessagesKillSwitch")

    val (queueValue, completed) = Source
      .fromGraph(new SendQueue[OutboundEnvelope](sendToDeadLetters))
      .via(streamKillSwitch.flow)
      .via(transport.outboundTestFlow(this))
      .toMat(transport.outboundLarge(this))(Keep.both)
      .run()(materializer)

    queueValue.inject(wrapper.queue)
    // replace with the materialized value, still same underlying queue
    queues(LargeQueueIndex) = queueValue
    queuesVisibility = true // volatile write for visibility of the queues array

    updateStreamMatValues(LargeQueueIndex, streamKillSwitch, completed)
    attachOutboundStreamRestart(
      "Outbound large message stream",
      LargeQueueIndex,
      largeQueueSize,
      completed,
      () => runOutboundLargeMessagesStream())
  }

  private def attachOutboundStreamRestart(
      streamName: String,
      queueIndex: Int,
      queueCapacity: Int,
      streamCompleted: Future[Done],
      restart: () => Unit): Unit = {

    def lazyRestart(): Unit = {
      RemotingFlightRecorder.transportRestartOutbound(remoteAddress, streamName)
      outboundCompressionAccess = Vector.empty
      if (queueIndex == ControlQueueIndex) {
        materializing = new CountDownLatch(1)
        _outboundControlIngress = OptionVal.None
      }
      // LazyQueueWrapper will invoke the `restart` function when first message is offered
      val wrappedRestartFun: () => Unit = () => {
        restart()
      }

      if (!isRemovedAfterQuarantined())
        queues(queueIndex) = LazyQueueWrapper(createQueue(queueCapacity, queueIndex), wrappedRestartFun)

      queuesVisibility = true // volatile write for visibility of the queues array
    }

    implicit val ec = materializer.executionContext
    streamCompleted.foreach { _ =>
      if (transport.isShutdown || isRemovedAfterQuarantined()) {
        // shutdown as expected
        // countDown the latch in case threads are waiting on the latch in outboundControlIngress method
        materializing.countDown()
      } else {
        log.debug("{} to [{}] was completed. It will be restarted if used again.", streamName, remoteAddress)
        lazyRestart()
      }
    }
    streamCompleted.failed.foreach {
      case ArteryTransport.ShutdownSignal =>
        // shutdown as expected
        cancelAllTimers()
        // countDown the latch in case threads are waiting on the latch in outboundControlIngress method
        materializing.countDown()
      case cause if transport.isShutdown || isRemovedAfterQuarantined() =>
        // don't restart after shutdown, but log some details so we notice
        // for the TCP transport the ShutdownSignal is "converted" to StreamTcpException
        if (!cause.isInstanceOf[StreamTcpException])
          log.warning(
            s"{} to [{}] failed after shutdown. {}: {}",
            streamName,
            remoteAddress,
            cause.getClass.getName,
            cause.getMessage)
        cancelAllTimers()
        // countDown the latch in case threads are waiting on the latch in outboundControlIngress method
        materializing.countDown()
      case _: AeronTerminated =>
        // shutdown already in progress
        cancelAllTimers()
      case _: AbruptTerminationException =>
        // ActorSystem shutdown
        cancelAllTimers()
      case cause =>
        // it might have been stopped as expected due to idle or quarantine
        // for the TCP transport the exception is "converted" to StreamTcpException
        val stoppedIdle = cause == OutboundStreamStopIdleSignal ||
          getStopReason(queueIndex).contains(OutboundStreamStopIdleSignal)
        val stoppedQuarantined = cause == OutboundStreamStopQuarantinedSignal ||
          getStopReason(queueIndex).contains(OutboundStreamStopQuarantinedSignal)

        // for some cases restart unconditionally, without counting restarts
        val bypassRestartCounter = cause match {
          case _: GaveUpMessageException => true
          case _                         => stoppedIdle || stoppedQuarantined
        }

        if (queueIndex == ControlQueueIndex && !stoppedQuarantined) {
          cause match {
            case _: HandshakeTimeoutException => // ok, quarantine not possible without UID
            case _                            =>
              // Must quarantine in case all system messages haven't been delivered.
              // See also comment in the stoppedIdle case below
              quarantine(s"Outbound control stream restarted. $cause")
          }
        }

        def isConnectException: Boolean =
          cause.isInstanceOf[StreamTcpException] && cause.getCause != null && cause.getCause
            .isInstanceOf[ConnectException]

        if (stoppedIdle) {
          log.debug("{} to [{}] was idle and stopped. It will be restarted if used again.", streamName, remoteAddress)
          lazyRestart()
        } else if (stoppedQuarantined) {
          log.debug(
            "{} to [{}] was quarantined and stopped. It will be restarted if used again.",
            streamName,
            remoteAddress)
          lazyRestart()
        } else if (bypassRestartCounter || restartCounter.restart()) {
          // ConnectException may happen repeatedly and are already logged in connectionFlowWithRestart
          if (isConnectException)
            log.debug("{} to [{}] failed. Restarting it. {}", streamName, remoteAddress, cause)
          else
            log.warning("{} to [{}] failed. Restarting it. {}", streamName, remoteAddress, cause)
          lazyRestart()
        } else {
          log.error(
            cause,
            s"{} to [{}] failed and restarted {} times within {} seconds. Terminating system. ${cause.getMessage}",
            streamName,
            remoteAddress,
            advancedSettings.OutboundMaxRestarts,
            advancedSettings.OutboundRestartTimeout.toSeconds)
          cancelAllTimers()
          transport.system.terminate()
        }
    }
  }

  private def updateStreamMatValues(
      streamId: Int,
      streamKillSwitch: SharedKillSwitch,
      completed: Future[Done]): Unit = {
    implicit val ec = materializer.executionContext
    updateStreamMatValues(streamId, OutboundStreamMatValues(OptionVal.Some(streamKillSwitch), completed.recover {
      case _ => Done
    }, stopping = OptionVal.None))
  }

  @tailrec private def updateStreamMatValues(streamId: Int, values: OutboundStreamMatValues): Unit = {
    val prev = streamMatValues.get()
    if (!streamMatValues.compareAndSet(prev, prev + (streamId -> values))) {
      updateStreamMatValues(streamId, values)
    }
  }

  @tailrec private def setStopReason(streamId: Int, stopSignal: StopSignal): Unit = {
    val prev = streamMatValues.get()
    prev.get(streamId) match {
      case Some(v) =>
        if (!streamMatValues.compareAndSet(prev, prev.updated(streamId, v.copy(stopping = OptionVal.Some(stopSignal)))))
          setStopReason(streamId, stopSignal)
      case None => throw new IllegalStateException(s"Expected streamMatValues for [$streamId]")
    }
  }

  private def getStopReason(streamId: Int): OptionVal[StopSignal] = {
    streamMatValues.get().get(streamId) match {
      case Some(OutboundStreamMatValues(_, _, stopping)) => stopping
      case None                                          => OptionVal.None
    }
  }

  // after it has been used we remove the kill switch to cleanup some memory,
  // not a "leak" but a KillSwitch is rather heavy
  @tailrec private def clearStreamKillSwitch(streamId: Int, old: SharedKillSwitch): Unit = {
    val prev = streamMatValues.get()
    prev.get(streamId) match {
      case Some(v) =>
        if (v.streamKillSwitch.isDefined && (v.streamKillSwitch.get eq old)) {
          if (!streamMatValues.compareAndSet(prev, prev.updated(streamId, v.copy(streamKillSwitch = OptionVal.None))))
            clearStreamKillSwitch(streamId, old)
        }
      case None => throw new IllegalStateException(s"Expected streamMatValues for [$streamId]")
    }
  }

  /**
   * Exposed for orderly shutdown purposes, can not be trusted except for during shutdown as streams may restart.
   * Will complete successfully even if one of the stream completion futures failed
   */
  def streamsCompleted: Future[Done] = {
    implicit val ec = materializer.executionContext
    Future
      .sequence(streamMatValues.get().values.map {
        case OutboundStreamMatValues(_, done, _) => done
      })
      .map(_ => Done)
  }

  override def toString: String =
    s"Association($localAddress -> $remoteAddress with $associationState)"

}

/**
 * INTERNAL API
 */
private[remote] class AssociationRegistry(createAssociation: Address => Association) {
  private[this] val associationsByAddress = new AtomicReference[Map[Address, Association]](Map.empty)
  private[this] val associationsByUid = new AtomicReference[ImmutableLongMap[Association]](ImmutableLongMap.empty)

  /**
   * @throws ShuttingDown if called while the transport is shutting down
   */
  @tailrec final def association(remoteAddress: Address): Association = {
    val currentMap = associationsByAddress.get
    currentMap.get(remoteAddress) match {
      case Some(existing) => existing
      case None =>
        val newAssociation = createAssociation(remoteAddress)
        val newMap = currentMap.updated(remoteAddress, newAssociation)
        if (associationsByAddress.compareAndSet(currentMap, newMap)) {
          newAssociation.associate() // start it, only once
          newAssociation
        } else
          association(remoteAddress) // lost CAS, retry
    }
  }

  def association(uid: Long): OptionVal[Association] =
    associationsByUid.get.get(uid)

  /**
   * @throws ShuttingDown if called while the transport is shutting down
   */
  @tailrec final def setUID(peer: UniqueAddress): Association = {
    // Don't create a new association via this method. It's supposed to exist unless it was removed after quarantined.
    val a = association(peer.address)

    val currentMap = associationsByUid.get
    currentMap.get(peer.uid) match {
      case OptionVal.Some(previous) =>
        if (previous eq a)
          // associationsByUid Map already contains the right association
          a
        else
          // make sure we don't overwrite same UID with different association
          throw new UidCollisionException(s"UID collision old [$previous] new [$a]")
      case _ =>
        // update associationsByUid Map with the uid -> association
        val newMap = currentMap.updated(peer.uid, a)
        if (associationsByUid.compareAndSet(currentMap, newMap))
          a
        else
          setUID(peer) // lost CAS, retry
    }
  }

  def allAssociations: Set[Association] =
    associationsByAddress.get.values.toSet

  def removeUnusedQuarantined(after: FiniteDuration): Unit = {
    removeUnusedQuarantinedByAddress(after)
    removeUnusedQuarantinedByUid(after)
  }

  @tailrec private def removeUnusedQuarantinedByAddress(after: FiniteDuration): Unit = {
    val now = System.nanoTime()
    val afterNanos = after.toNanos
    val currentMap = associationsByAddress.get
    val remove = currentMap.foldLeft(Map.empty[Address, Association]) {
      case (acc, (address, association)) =>
        val state = association.associationState
        if ((now - state.lastUsedTimestamp.get) >= afterNanos) {
          state.uniqueRemoteAddressState() match {
            case AssociationState.UidQuarantined | AssociationState.UidUnknown => acc.updated(address, association)
            case AssociationState.UidKnown                                     => acc
          }
        } else {
          acc
        }
    }
    if (remove.nonEmpty) {
      val newMap = currentMap -- remove.keysIterator
      if (associationsByAddress.compareAndSet(currentMap, newMap))
        remove.valuesIterator.foreach(_.removedAfterQuarantined())
      else
        removeUnusedQuarantinedByAddress(after) // CAS fail, recursive
    }
  }

  @tailrec private def removeUnusedQuarantinedByUid(after: FiniteDuration): Unit = {
    val now = System.nanoTime()
    val afterNanos = after.toNanos
    val currentMap = associationsByUid.get
    val remove = currentMap.keysIterator.foldLeft(Map.empty[Long, Association]) {
      case (acc, uid) =>
        val association = currentMap.get(uid).get
        val state = association.associationState
        if ((now - state.lastUsedTimestamp.get) >= afterNanos) {
          state.uniqueRemoteAddressState() match {
            case AssociationState.UidQuarantined | AssociationState.UidUnknown => acc.updated(uid, association)
            case AssociationState.UidKnown                                     => acc
          }
        } else {
          acc
        }
    }
    if (remove.nonEmpty) {
      val newMap = remove.keysIterator.foldLeft(currentMap)((acc, uid) => acc.remove(uid))
      if (associationsByUid.compareAndSet(currentMap, newMap))
        remove.valuesIterator.foreach(_.removedAfterQuarantined())
      else
        removeUnusedQuarantinedByUid(after) // CAS fail, recursive
    }
  }
}
