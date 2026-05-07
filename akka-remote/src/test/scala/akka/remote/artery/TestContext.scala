/*
 * Copyright (C) 2016-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadLocalRandom

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import com.typesafe.config.ConfigFactory

import akka.Done
import akka.actor.ActorRef
import akka.actor.Address
import akka.remote.UniqueAddress
import akka.remote.artery.InboundControlJunction.ControlMessageObserver
import akka.remote.artery.InboundControlJunction.ControlMessageSubject
import akka.util.OptionVal

private[remote] class TestInboundContext(
    override val localAddress: UniqueAddress,
    val controlSubject: TestControlMessageSubject = new TestControlMessageSubject,
    val controlProbe: Option[ActorRef] = None,
    val replyDropRate: Double = 0.0)
    extends InboundContext {

  private val associationsByAddress = new ConcurrentHashMap[Address, OutboundContext]()
  private val associationsByUid = new ConcurrentHashMap[Long, OutboundContext]()

  override def sendControl(to: Address, message: ControlMessage) = {
    if (ThreadLocalRandom.current().nextDouble() >= replyDropRate)
      association(to).sendControl(message)
  }

  override def association(remoteAddress: Address): OutboundContext =
    associationsByAddress.get(remoteAddress) match {
      case null =>
        val a = createAssociation(remoteAddress)
        associationsByAddress.putIfAbsent(remoteAddress, a) match {
          case null     => a
          case existing => existing
        }
      case existing => existing
    }

  override def association(uid: Long): OptionVal[OutboundContext] =
    OptionVal(associationsByUid.get(uid))

  override def completeHandshake(peer: UniqueAddress): Future[Done] = {
    val a = association(peer.address).asInstanceOf[TestOutboundContext]
    val done = a.completeHandshake(peer)
    done.foreach { _ =>
      associationsByUid.put(peer.uid, a)
    }(ExecutionContext.parasitic)
    done
  }

  protected def createAssociation(remoteAddress: Address): TestOutboundContext =
    new TestOutboundContext(localAddress, remoteAddress, controlSubject, controlProbe)

  override lazy val settings: ArterySettings =
    ArterySettings(ConfigFactory.load().getConfig("akka.remote.artery"))

  override def publishDropped(env: InboundEnvelope, reason: String): Unit = ()
}

private[remote] class TestOutboundContext(
    override val localAddress: UniqueAddress,
    override val remoteAddress: Address,
    override val controlSubject: TestControlMessageSubject,
    val controlProbe: Option[ActorRef] = None)
    extends OutboundContext {

  // access to this is synchronized (it's a test utility)
  private var _associationState = AssociationState()

  override def associationState: AssociationState = synchronized {
    _associationState
  }

  def completeHandshake(peer: UniqueAddress): Future[Done] = synchronized {
    _associationState.completeUniqueRemoteAddress(peer)
    _associationState.uniqueRemoteAddress() match {
      case Some(`peer`) => // our value
      case _ =>
        _associationState = _associationState.newIncarnation(peer)
    }
    Future.successful(Done)
  }

  override def quarantine(reason: String): Unit = synchronized {
    _associationState = _associationState.newQuarantined(harmless = false)
  }

  override def isOrdinaryMessageStreamActive(): Boolean = true

  override def sendControl(message: ControlMessage) = {
    controlProbe.foreach(_ ! message)
    controlSubject.sendControl(
      InboundEnvelope(OptionVal.None, message, OptionVal.None, localAddress.uid, OptionVal.None))
  }

  override lazy val settings: ArterySettings =
    ArterySettings(ConfigFactory.load().getConfig("akka.remote.artery"))

}

private[remote] class TestControlMessageSubject extends ControlMessageSubject {

  private val observers = new CopyOnWriteArrayList[ControlMessageObserver]

  override def attach(observer: ControlMessageObserver): Future[Done] = {
    observers.add(observer)
    Future.successful(Done)
  }

  override def detach(observer: ControlMessageObserver): Unit = {
    observers.remove(observer)
  }

  def sendControl(env: InboundEnvelope): Unit = {
    val iter = observers.iterator()
    while (iter.hasNext()) iter.next().notify(env)
  }

}

private[remote] class ManualReplyInboundContext(
    replyProbe: ActorRef,
    localAddress: UniqueAddress,
    controlSubject: TestControlMessageSubject)
    extends TestInboundContext(localAddress, controlSubject) {

  private var lastReply: Option[(Address, ControlMessage)] = None

  override def sendControl(to: Address, message: ControlMessage): Unit = synchronized {
    lastReply = Some((to, message))
    replyProbe ! message
  }

  def deliverLastReply(): Unit = synchronized {
    lastReply.foreach { case (to, message) => super.sendControl(to, message) }
    lastReply = None
  }
}
