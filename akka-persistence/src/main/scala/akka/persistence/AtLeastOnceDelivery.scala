/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.{ ActorPath, ActorSelection, NotInfluenceReceiveTimeout }
import akka.actor.Cancellable
import akka.actor.DeadLetterSuppression
import akka.annotation.InternalApi
import akka.persistence.serialization.Message

object AtLeastOnceDelivery {

  /**
   * Snapshot of current `AtLeastOnceDelivery` state. Can be retrieved with
   * [[AtLeastOnceDeliveryLike#getDeliverySnapshot]] and saved with [[PersistentActor#saveSnapshot]].
   * During recovery the snapshot received in [[SnapshotOffer]] should be set
   * with [[AtLeastOnceDeliveryLike#setDeliverySnapshot]].
   */
  @SerialVersionUID(1L)
  case class AtLeastOnceDeliverySnapshot(
      currentDeliveryId: Long,
      unconfirmedDeliveries: immutable.Seq[UnconfirmedDelivery])
      extends Message {

    /**
     * Java API
     */
    def getUnconfirmedDeliveries: java.util.List[UnconfirmedDelivery] = {
      import scala.jdk.CollectionConverters._
      unconfirmedDeliveries.asJava
    }

  }

  /**
   * @see [[AtLeastOnceDeliveryLike#warnAfterNumberOfUnconfirmedAttempts]]
   */
  @SerialVersionUID(1L)
  case class UnconfirmedWarning(unconfirmedDeliveries: immutable.Seq[UnconfirmedDelivery]) {

    /**
     * Java API
     */
    def getUnconfirmedDeliveries: java.util.List[UnconfirmedDelivery] = {
      import scala.jdk.CollectionConverters._
      unconfirmedDeliveries.asJava
    }
  }

  /**
   * Information about a message that has not been confirmed. Included in [[UnconfirmedWarning]]
   * and [[AtLeastOnceDeliverySnapshot]].
   */
  case class UnconfirmedDelivery(deliveryId: Long, destination: ActorPath, message: Any) {

    /**
     * Java API
     */
    def getMessage(): AnyRef = message.asInstanceOf[AnyRef]
  }

  /**
   * @see [[AtLeastOnceDeliveryLike#maxUnconfirmedMessages]]
   */
  class MaxUnconfirmedMessagesExceededException(message: String) extends RuntimeException(message)

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    case class Delivery(destination: ActorPath, message: Any, timestamp: Long, attempt: Int)
    case object RedeliveryTick extends NotInfluenceReceiveTimeout with DeadLetterSuppression
  }

}

/**
 * Scala API: Mix-in this trait with your `PersistentActor` to send messages with at-least-once
 * delivery semantics to destinations. It takes care of re-sending messages when they
 * have not been confirmed within a configurable timeout. Use the [[AtLeastOnceDeliveryLike#deliver]] method to
 * send a message to a destination. Call the [[AtLeastOnceDeliveryLike#confirmDelivery]] method when the destination
 * has replied with a confirmation message.
 *
 * At-least-once delivery implies that original message send order is not always retained
 * and the destination may receive duplicate messages due to possible resends.
 *
 * The interval between redelivery attempts can be defined by [[AtLeastOnceDeliveryLike#redeliverInterval]].
 * After a number of delivery attempts a [[AtLeastOnceDelivery.UnconfirmedWarning]] message
 * will be sent to `self`. The re-sending will still continue, but you can choose to call
 * [[AtLeastOnceDeliveryLike#confirmDelivery]] to cancel the re-sending.
 *
 * The `AtLeastOnceDelivery` trait has a state consisting of unconfirmed messages and a
 * sequence number. It does not store this state itself. You must persist events corresponding
 * to the `deliver` and `confirmDelivery` invocations from your `PersistentActor` so that the
 * state can be restored by calling the same methods during the recovery phase of the
 * `PersistentActor`. Sometimes these events can be derived from other business level events,
 * and sometimes you must create separate events. During recovery calls to `deliver`
 * will not send out the message, but it will be sent later if no matching `confirmDelivery`
 * was performed.
 *
 * Support for snapshots is provided by [[AtLeastOnceDeliveryLike#getDeliverySnapshot]] and [[AtLeastOnceDeliveryLike#setDeliverySnapshot]].
 * The `AtLeastOnceDeliverySnapshot` contains the full delivery state, including unconfirmed messages.
 * If you need a custom snapshot for other parts of the actor state you must also include the
 * `AtLeastOnceDeliverySnapshot`. It is serialized using protobuf with the ordinary Akka
 * serialization mechanism. It is easiest to include the bytes of the `AtLeastOnceDeliverySnapshot`
 * as a blob in your custom snapshot.
 *
 * @see [[AtLeastOnceDeliveryLike]]
 * @see [[AbstractPersistentActorWithAtLeastOnceDelivery]] for Java API
 */
trait AtLeastOnceDelivery extends PersistentActor with AtLeastOnceDeliveryLike {

  /**
   * Scala API: Send the message created by the `deliveryIdToMessage` function to
   * the `destination` actor. It will retry sending the message until
   * the delivery is confirmed with [[#confirmDelivery]]. Correlation
   * between `deliver` and `confirmDelivery` is performed with the
   * `deliveryId` that is provided as parameter to the `deliveryIdToMessage`
   * function. The `deliveryId` is typically passed in the message to the
   * destination, which replies with a message containing the same `deliveryId`.
   *
   * The `deliveryId` is a strictly monotonically increasing sequence number without
   * gaps. The same sequence is used for all destinations of the actor, i.e. when sending
   * to multiple destinations the destinations will see gaps in the sequence if no
   * translation is performed.
   *
   * During recovery this method will not send out the message, but it will be sent
   * later if no matching `confirmDelivery` was performed.
   *
   * This method will throw [[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException]]
   * if [[#numberOfUnconfirmed]] is greater than or equal to [[#maxUnconfirmedMessages]].
   */
  def deliver(destination: ActorPath)(deliveryIdToMessage: Long => Any): Unit = {
    internalDeliver(destination)(deliveryIdToMessage)
  }

  /**
   * Scala API: Send the message created by the `deliveryIdToMessage` function to
   * the `destination` actor. It will retry sending the message until
   * the delivery is confirmed with [[#confirmDelivery]]. Correlation
   * between `deliver` and `confirmDelivery` is performed with the
   * `deliveryId` that is provided as parameter to the `deliveryIdToMessage`
   * function. The `deliveryId` is typically passed in the message to the
   * destination, which replies with a message containing the same `deliveryId`.
   *
   * The `deliveryId` is a strictly monotonically increasing sequence number without
   * gaps. The same sequence is used for all destinations of the actor, i.e. when sending
   * to multiple destinations the destinations will see gaps in the sequence if no
   * translation is performed.
   *
   * During recovery this method will not send out the message, but it will be sent
   * later if no matching `confirmDelivery` was performed.
   *
   * This method will throw [[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException]]
   * if [[#numberOfUnconfirmed]] is greater than or equal to [[#maxUnconfirmedMessages]].
   */
  def deliver(destination: ActorSelection)(deliveryIdToMessage: Long => Any): Unit = {
    internalDeliver(destination)(deliveryIdToMessage)
  }

}

/**
 * @see [[AtLeastOnceDelivery]]
 */
trait AtLeastOnceDeliveryLike extends Eventsourced {
  import AtLeastOnceDelivery._
  import AtLeastOnceDelivery.Internal._

  /**
   * Interval between redelivery attempts.
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.redeliver-interval`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def redeliverInterval: FiniteDuration = defaultRedeliverInterval

  private val defaultRedeliverInterval: FiniteDuration =
    Persistence(context.system).settings.atLeastOnceDelivery.redeliverInterval

  /**
   * Maximum number of unconfirmed messages that will be sent at each redelivery burst
   * (burst frequency is half of the redelivery interval).
   * If there's a lot of unconfirmed messages (e.g. if the destination is not available for a long time),
   * this helps to prevent an overwhelming amount of messages to be sent at once.
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.redelivery-burst-limit`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def redeliveryBurstLimit: Int = defaultRedeliveryBurstLimit

  private val defaultRedeliveryBurstLimit: Int =
    Persistence(context.system).settings.atLeastOnceDelivery.redeliveryBurstLimit

  /**
   * After this number of delivery attempts an [[AtLeastOnceDelivery.UnconfirmedWarning]] message
   * will be sent to `self`. The count is reset after a restart.
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.warn-after-number-of-unconfirmed-attempts`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def warnAfterNumberOfUnconfirmedAttempts: Int = defaultWarnAfterNumberOfUnconfirmedAttempts

  private val defaultWarnAfterNumberOfUnconfirmedAttempts: Int =
    Persistence(context.system).settings.atLeastOnceDelivery.warnAfterNumberOfUnconfirmedAttempts

  /**
   * Maximum number of unconfirmed messages that this actor is allowed to hold in memory.
   * If this number is exceed [[#deliver]] will not accept more messages and it will throw
   * [[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException]].
   *
   * The default value can be configured with the
   * `akka.persistence.at-least-once-delivery.max-unconfirmed-messages`
   * configuration key. This method can be overridden by implementation classes to return
   * non-default values.
   */
  def maxUnconfirmedMessages: Int = defaultMaxUnconfirmedMessages

  private val defaultMaxUnconfirmedMessages: Int =
    Persistence(context.system).settings.atLeastOnceDelivery.maxUnconfirmedMessages

  // will be started after recovery completed
  private var redeliverTask: Option[Cancellable] = None

  private var deliverySequenceNr = 0L
  private var unconfirmed = immutable.SortedMap.empty[Long, Delivery]

  private def startRedeliverTask(): Unit = {
    if (redeliverTask.isEmpty) {
      val interval = redeliverInterval / 2
      redeliverTask = Some(
        context.system.scheduler.scheduleWithFixedDelay(interval, interval, self, RedeliveryTick)(context.dispatcher))
    }
  }

  private def cancelRedeliveryTask(): Unit = {
    redeliverTask.foreach(_.cancel())
    redeliverTask = None
  }

  private def nextDeliverySequenceNr(): Long = {
    deliverySequenceNr += 1
    deliverySequenceNr
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final def internalDeliver(destination: ActorPath)(deliveryIdToMessage: Long => Any): Unit = {
    if (unconfirmed.size >= maxUnconfirmedMessages)
      throw new MaxUnconfirmedMessagesExceededException(
        s"Too many unconfirmed messages, maximum allowed is [$maxUnconfirmedMessages]")

    val deliveryId = nextDeliverySequenceNr()
    val now = if (recoveryRunning) {
      System.nanoTime() - redeliverInterval.toNanos
    } else System.nanoTime()
    val d = Delivery(destination, deliveryIdToMessage(deliveryId), now, attempt = 0)

    if (recoveryRunning)
      unconfirmed = unconfirmed.updated(deliveryId, d)
    else
      send(deliveryId, d, now)
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] final def internalDeliver(destination: ActorSelection)(deliveryIdToMessage: Long => Any): Unit = {
    val isWildcardSelection = destination.pathString.contains("*")
    require(
      !isWildcardSelection,
      "Delivering to wildcard actor selections is not supported by AtLeastOnceDelivery. " +
      "Introduce an mediator Actor which this AtLeastOnceDelivery Actor will deliver the messages to," +
      "and will handle the logic of fan-out and collecting individual confirmations, until it can signal confirmation back to this Actor.")
    internalDeliver(ActorPath.fromString(destination.toSerializationFormat))(deliveryIdToMessage)
  }

  /**
   * Call this method when a message has been confirmed by the destination,
   * or to abort re-sending.
   * @see [[#deliver]]
   * @return `true` the first time the `deliveryId` is confirmed, i.e. `false` for duplicate confirm
   */
  def confirmDelivery(deliveryId: Long): Boolean = {
    if (unconfirmed.contains(deliveryId)) {
      unconfirmed -= deliveryId
      if (unconfirmed.isEmpty)
        cancelRedeliveryTask()
      true
    } else false
  }

  /**
   * Number of messages that have not been confirmed yet.
   */
  def numberOfUnconfirmed: Int = unconfirmed.size

  private def redeliverOverdue(): Unit = {
    val now = System.nanoTime()
    val deadline = now - redeliverInterval.toNanos
    var warnings = Vector.empty[UnconfirmedDelivery]

    unconfirmed.iterator
      .filter { case (_, delivery) => delivery.timestamp <= deadline }
      .take(redeliveryBurstLimit)
      .foreach {
        case (deliveryId, delivery) =>
          send(deliveryId, delivery, now)

          if (delivery.attempt == warnAfterNumberOfUnconfirmedAttempts)
            warnings :+= UnconfirmedDelivery(deliveryId, delivery.destination, delivery.message)
      }

    if (warnings.nonEmpty)
      self ! UnconfirmedWarning(warnings)
  }

  private def send(deliveryId: Long, d: Delivery, timestamp: Long): Unit = {
    context.actorSelection(d.destination) ! d.message
    unconfirmed = unconfirmed.updated(deliveryId, d.copy(timestamp = timestamp, attempt = d.attempt + 1))
    startRedeliverTask()
  }

  /**
   * Full state of the `AtLeastOnceDelivery`. It can be saved with [[PersistentActor#saveSnapshot]].
   * During recovery the snapshot received in [[SnapshotOffer]] should be set
   * with [[#setDeliverySnapshot]].
   *
   * The `AtLeastOnceDeliverySnapshot` contains the full delivery state, including unconfirmed messages.
   * If you need a custom snapshot for other parts of the actor state you must also include the
   * `AtLeastOnceDeliverySnapshot`. It is serialized using protobuf with the ordinary Akka
   * serialization mechanism. It is easiest to include the bytes of the `AtLeastOnceDeliverySnapshot`
   * as a blob in your custom snapshot.
   */
  def getDeliverySnapshot: AtLeastOnceDeliverySnapshot =
    AtLeastOnceDeliverySnapshot(
      deliverySequenceNr,
      unconfirmed.iterator
        .map { case (deliveryId, d) => UnconfirmedDelivery(deliveryId, d.destination, d.message) }
        .to(immutable.IndexedSeq))

  /**
   * If snapshot from [[#getDeliverySnapshot]] was saved it will be received during recovery
   * in a [[SnapshotOffer]] message and should be set with this method.
   */
  def setDeliverySnapshot(snapshot: AtLeastOnceDeliverySnapshot): Unit = {
    deliverySequenceNr = snapshot.currentDeliveryId
    // deliver on next tick
    val now = System.nanoTime() - redeliverInterval.toNanos
    unconfirmed = scala.collection.immutable.SortedMap.from(snapshot.unconfirmedDeliveries.iterator.map(d =>
      d.deliveryId -> Delivery(d.destination, d.message, now, 0)))
  }

  /**
   * INTERNAL API
   */
  override protected[akka] def aroundPreRestart(reason: Throwable, message: Option[Any]): Unit = {
    cancelRedeliveryTask()
    super.aroundPreRestart(reason, message)
  }

  /**
   * INTERNAL API
   */
  override protected[akka] def aroundPostStop(): Unit = {
    cancelRedeliveryTask()
    super.aroundPostStop()
  }

  override private[akka] def onReplaySuccess(): Unit = {
    if (unconfirmed.nonEmpty) {
      redeliverOverdue()
      startRedeliverTask()
    }
    super.onReplaySuccess()
  }

  /**
   * INTERNAL API
   */
  override protected[akka] def aroundReceive(receive: Receive, message: Any): Unit =
    message match {
      case RedeliveryTick =>
        redeliverOverdue()

      case _ =>
        super.aroundReceive(receive, message)
    }
}

/**
 * Java API: compatible with lambda expressions
 *
 * Use this class instead of `AbstractPersistentActor` to send messages
 * with at-least-once delivery semantics to destinations.
 * Full documentation in [[AtLeastOnceDelivery]].
 *
 * @see [[AtLeastOnceDelivery]]
 * @see [[AtLeastOnceDeliveryLike]]
 */
abstract class AbstractPersistentActorWithAtLeastOnceDelivery
    extends AbstractPersistentActor
    with AtLeastOnceDeliveryLike {

  /**
   * Java API: Send the message created by the `deliveryIdToMessage` function to
   * the `destination` actor. It will retry sending the message until
   * the delivery is confirmed with [[#confirmDelivery]]. Correlation
   * between `deliver` and `confirmDelivery` is performed with the
   * `deliveryId` that is provided as parameter to the `deliveryIdToMessage`
   * function. The `deliveryId` is typically passed in the message to the
   * destination, which replies with a message containing the same `deliveryId`.
   *
   * The `deliveryId` is a strictly monotonically increasing sequence number without
   * gaps. The same sequence is used for all destinations, i.e. when sending to
   * multiple destinations the destinations will see gaps in the sequence if no
   * translation is performed.
   *
   * During recovery this method will not send out the message, but it will be sent
   * later if no matching `confirmDelivery` was performed.
   *
   * This method will throw [[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException]]
   * if [[#numberOfUnconfirmed]] is greater than or equal to [[#maxUnconfirmedMessages]].
   */
  def deliver(destination: ActorPath, deliveryIdToMessage: akka.japi.Function[java.lang.Long, Object]): Unit =
    internalDeliver(destination)(id => deliveryIdToMessage.apply(id))

  /**
   * Java API: Send the message created by the `deliveryIdToMessage` function to
   * the `destination` actor. It will retry sending the message until
   * the delivery is confirmed with [[#confirmDelivery]]. Correlation
   * between `deliver` and `confirmDelivery` is performed with the
   * `deliveryId` that is provided as parameter to the `deliveryIdToMessage`
   * function. The `deliveryId` is typically passed in the message to the
   * destination, which replies with a message containing the same `deliveryId`.
   *
   * The `deliveryId` is a strictly monotonically increasing sequence number without
   * gaps. The same sequence is used for all destinations, i.e. when sending to
   * multiple destinations the destinations will see gaps in the sequence if no
   * translation is performed.
   *
   * During recovery this method will not send out the message, but it will be sent
   * later if no matching `confirmDelivery` was performed.
   *
   * This method will throw [[AtLeastOnceDelivery.MaxUnconfirmedMessagesExceededException]]
   * if [[#numberOfUnconfirmed]] is greater than or equal to [[#maxUnconfirmedMessages]].
   */
  def deliver(destination: ActorSelection, deliveryIdToMessage: akka.japi.Function[java.lang.Long, Object]): Unit =
    internalDeliver(destination)(id => deliveryIdToMessage.apply(id))
}
