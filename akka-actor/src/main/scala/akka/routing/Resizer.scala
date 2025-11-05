/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.routing

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable

import com.typesafe.config.Config

import akka.AkkaException
import akka.actor.Actor
import akka.actor.ActorCell
import akka.actor.ActorInitializationException
import akka.actor.ActorRefWithCell
import akka.actor.ActorSystemImpl
import akka.actor.InternalActorRef
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.dispatch.Envelope
import akka.dispatch.MessageDispatcher

/**
 * [[Pool]] routers with dynamically resizable number of routees are implemented by providing a Resizer
 * implementation in the [[akka.routing.Pool]] configuration.
 */
trait Resizer {

  /**
   * Is it time for resizing. Typically implemented with modulo of nth message, but
   * could be based on elapsed time or something else. The messageCounter starts with 0
   * for the initial resize and continues with 1 for the first message. Make sure to perform
   * initial resize before first message (messageCounter == 0), because there is no guarantee
   * that resize will be done when concurrent messages are in play.
   *
   * CAUTION: this method is invoked from the thread which tries to send a
   * message to the pool, i.e. the ActorRef.!() method, hence it may be called
   * concurrently.
   */
  def isTimeForResize(messageCounter: Long): Boolean

  /**
   * Decide if the capacity of the router need to be changed. Will be invoked when `isTimeForResize`
   * returns true and no other resize is in progress.
   *
   * Return the number of routees to add or remove. Negative value will remove that number of routees.
   * Positive value will add that number of routees. 0 will not change the routees.
   *
   * This method is invoked only in the context of the Router actor.
   */
  def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int

}

object Resizer {
  def fromConfig(parentConfig: Config): Option[Resizer] = {
    val defaultResizerConfig = parentConfig.getConfig("resizer")
    val metricsBasedResizerConfig = parentConfig.getConfig("optimal-size-exploring-resizer")
    (defaultResizerConfig.getBoolean("enabled"), metricsBasedResizerConfig.getBoolean("enabled")) match {
      case (true, false)  => Some(DefaultResizer(defaultResizerConfig))
      case (false, true)  => Some(OptimalSizeExploringResizer(metricsBasedResizerConfig))
      case (false, false) => None
      case (true, true) =>
        throw new ResizerInitializationException(s"cannot enable both resizer and optimal-size-exploring-resizer", null)
    }
  }
}

@SerialVersionUID(1L)
class ResizerInitializationException(message: String, cause: Throwable) extends AkkaException(message, cause)

case object DefaultResizer {

  /**
   * Creates a new DefaultResizer from the given configuration
   */
  def apply(resizerConfig: Config): DefaultResizer =
    DefaultResizer(
      lowerBound = resizerConfig.getInt("lower-bound"),
      upperBound = resizerConfig.getInt("upper-bound"),
      pressureThreshold = resizerConfig.getInt("pressure-threshold"),
      rampupRate = resizerConfig.getDouble("rampup-rate"),
      backoffThreshold = resizerConfig.getDouble("backoff-threshold"),
      backoffRate = resizerConfig.getDouble("backoff-rate"),
      messagesPerResize = resizerConfig.getInt("messages-per-resize"))

  def fromConfig(resizerConfig: Config): Option[DefaultResizer] =
    if (resizerConfig.getBoolean("resizer.enabled"))
      Some(DefaultResizer(resizerConfig.getConfig("resizer")))
    else
      None
}

/**
 * Implementation of [[Resizer]] that adjust the [[Pool]] based on specified
 * thresholds.
 * @param lowerBound The fewest number of routees the router should ever have.
 * @param upperBound The most number of routees the router should ever have. Must be greater than or equal to `lowerBound`.
 * @param pressureThreshold Threshold to evaluate if routee is considered to be busy (under pressure).
 * Implementation depends on this value (default is 1).
 * <ul>
 * <li> 0:   number of routees currently processing a message.</li>
 * <li> 1:   number of routees currently processing a message has
 *           some messages in mailbox.</li>
 * <li> &gt; 1: number of routees with at least the configured `pressureThreshold`
 *           messages in their mailbox. Note that estimating mailbox size of
 *           default UnboundedMailbox is O(N) operation.</li>
 * </ul>
 * @param rampupRate  Percentage to increase capacity whenever all routees are busy.
 * For example, 0.2 would increase 20% (rounded up), i.e. if current
 * capacity is 6 it will request an increase of 2 more routees.
 * @param backoffThreshold Minimum fraction of busy routees before backing off.
 * For example, if this is 0.3, then we'll remove some routees only when
 * less than 30% of routees are busy, i.e. if current capacity is 10 and
 * 3 are busy then the capacity is unchanged, but if 2 or less are busy
 * the capacity is decreased.
 * Use 0.0 or negative to avoid removal of routees.
 * @param backoffRate  Fraction of routees to be removed when the resizer reaches the
 * backoffThreshold.
 * For example, 0.1 would decrease 10% (rounded up), i.e. if current
 * capacity is 9 it will request an decrease of 1 routee.
 * @param messagesPerResize Number of messages between resize operation.
 * Use 1 to resize before each message.
 */
@SerialVersionUID(1L)
case class DefaultResizer(
    lowerBound: Int = 1,
    upperBound: Int = 10,
    pressureThreshold: Int = 1,
    rampupRate: Double = 0.2,
    backoffThreshold: Double = 0.3,
    backoffRate: Double = 0.1,
    messagesPerResize: Int = 10)
    extends Resizer {

  /**
   * Java API constructor for default values except bounds.
   */
  def this(lower: Int, upper: Int) = this(lowerBound = lower, upperBound = upper)

  if (lowerBound < 0) throw new IllegalArgumentException("lowerBound must be >= 0, was: [%s]".format(lowerBound))
  if (upperBound < 0) throw new IllegalArgumentException("upperBound must be >= 0, was: [%s]".format(upperBound))
  if (upperBound < lowerBound)
    throw new IllegalArgumentException(
      "upperBound must be >= lowerBound, was: [%s] < [%s]".format(upperBound, lowerBound))
  if (rampupRate < 0.0) throw new IllegalArgumentException("rampupRate must be >= 0.0, was [%s]".format(rampupRate))
  if (backoffThreshold > 1.0)
    throw new IllegalArgumentException("backoffThreshold must be <= 1.0, was [%s]".format(backoffThreshold))
  if (backoffRate < 0.0) throw new IllegalArgumentException("backoffRate must be >= 0.0, was [%s]".format(backoffRate))
  if (messagesPerResize <= 0)
    throw new IllegalArgumentException("messagesPerResize must be > 0, was [%s]".format(messagesPerResize))

  def isTimeForResize(messageCounter: Long): Boolean = (messageCounter % messagesPerResize == 0)

  override def resize(currentRoutees: immutable.IndexedSeq[Routee]): Int =
    capacity(currentRoutees)

  /**
   * Returns the overall desired change in resizer capacity. Positive value will
   * add routees to the resizer. Negative value will remove routees from the
   * resizer.
   * @param routees The current actor in the resizer
   * @return the number of routees by which the resizer should be adjusted (positive, negative or zero)
   */
  def capacity(routees: immutable.IndexedSeq[Routee]): Int = {
    val currentSize = routees.size
    val press = pressure(routees)
    val delta = filter(press, currentSize)
    val proposed = currentSize + delta

    if (proposed < lowerBound) delta + (lowerBound - proposed)
    else if (proposed > upperBound) delta - (proposed - upperBound)
    else delta
  }

  /**
   * Number of routees considered busy, or above 'pressure level'.
   *
   * Implementation depends on the value of `pressureThreshold`
   * (default is 1).
   * <ul>
   * <li> 0:   number of routees currently processing a message.</li>
   * <li> 1:   number of routees currently processing a message has
   *           some messages in mailbox.</li>
   * <li> &gt; 1: number of routees with at least the configured `pressureThreshold`
   *           messages in their mailbox. Note that estimating mailbox size of
   *           default UnboundedMailbox is O(N) operation.</li>
   * </ul>
   *
   * @param routees the current resizer of routees
   * @return number of busy routees, between 0 and routees.size
   */
  def pressure(routees: immutable.IndexedSeq[Routee]): Int = {
    routees.count {
      case ActorRefRoutee(a: ActorRefWithCell) =>
        a.underlying match {
          case cell: ActorCell =>
            pressureThreshold match {
              case 1          => cell.mailbox.isScheduled && cell.mailbox.hasMessages
              case i if i < 1 => cell.mailbox.isScheduled && cell.currentMessage != null
              case threshold  => cell.mailbox.numberOfMessages >= threshold
            }
          case cell =>
            pressureThreshold match {
              case 1          => cell.hasMessages
              case i if i < 1 => true // unstarted cells are always busy, for example
              case threshold  => cell.numberOfMessages >= threshold
            }
        }
      case _ =>
        false
    }
  }

  /**
   * This method can be used to smooth the capacity delta by considering
   * the current pressure and current capacity.
   *
   * @param pressure current number of busy routees
   * @param capacity current number of routees
   * @return proposed change in the capacity
   */
  def filter(pressure: Int, capacity: Int): Int = rampup(pressure, capacity) + backoff(pressure, capacity)

  /**
   * Computes a proposed positive (or zero) capacity delta using
   * the configured `rampupRate`.
   * @param pressure the current number of busy routees
   * @param capacity the current number of total routees
   * @return proposed increase in capacity
   */
  def rampup(pressure: Int, capacity: Int): Int =
    if (pressure < capacity) 0 else math.ceil(rampupRate * capacity).toInt

  /**
   * Computes a proposed negative (or zero) capacity delta using
   * the configured `backoffThreshold` and `backoffRate`
   * @param pressure the current number of busy routees
   * @param capacity the current number of total routees
   * @return proposed decrease in capacity (as a negative number)
   */
  def backoff(pressure: Int, capacity: Int): Int =
    if (backoffThreshold > 0.0 && backoffRate > 0.0 && capacity > 0 && pressure.toDouble / capacity < backoffThreshold)
      math.floor(-1.0 * backoffRate * capacity).toInt
    else 0
}

/**
 * INTERNAL API
 */
private[akka] final class ResizablePoolCell(
    _system: ActorSystemImpl,
    _ref: InternalActorRef,
    _routerProps: Props,
    _routerDispatcher: MessageDispatcher,
    _routeeProps: Props,
    _supervisor: InternalActorRef,
    val pool: Pool)
    extends RoutedActorCell(_system, _ref, _routerProps, _routerDispatcher, _routeeProps, _supervisor) {

  require(pool.resizer.isDefined, "RouterConfig must be a Pool with defined resizer")
  val resizer = pool.resizer.get
  private val resizeInProgress = new AtomicBoolean
  private val resizeCounter = new AtomicLong

  override protected def preSuperStart(): Unit = {
    // initial resize, before message send
    if (resizer.isTimeForResize(resizeCounter.getAndIncrement())) {
      resize(initial = true)
    }
  }

  override def sendMessage(envelope: Envelope): Unit = {
    if (!routerConfig.isManagementMessage(envelope.message) &&
        resizer.isTimeForResize(resizeCounter.getAndIncrement()) && resizeInProgress.compareAndSet(false, true)) {
      super.sendMessage(Envelope(ResizablePoolActor.Resize, self, system))
    }

    super.sendMessage(envelope)
  }

  private[akka] def resize(initial: Boolean): Unit = {
    if (resizeInProgress.get || initial) try {
      tryReportMessageCount()
      val requestedCapacity = resizer.resize(router.routees)
      if (requestedCapacity > 0) {
        val newRoutees = Vector.fill(requestedCapacity)(pool.newRoutee(routeeProps, this))
        addRoutees(newRoutees)
      } else if (requestedCapacity < 0) {
        val currentRoutees = router.routees
        val abandon = currentRoutees.drop(currentRoutees.length + requestedCapacity)
        removeRoutees(abandon, stopChild = true)
      }
    } finally resizeInProgress.set(false)
  }

  /**
   * This approach is chosen for binary compatibility
   */
  private def tryReportMessageCount(): Unit = {
    resizer match {
      case r: OptimalSizeExploringResizer => r.reportMessageCount(router.routees, resizeCounter.get())
      case _                              => //ignore
    }
  }

}

/**
 * INTERNAL API
 */
private[akka] object ResizablePoolActor {
  case object Resize extends RouterManagementMesssage
}

/**
 * INTERNAL API
 */
private[akka] class ResizablePoolActor(supervisorStrategy: SupervisorStrategy)
    extends RouterPoolActor(supervisorStrategy) {
  import ResizablePoolActor._

  val resizerCell = context match {
    case x: ResizablePoolCell => x
    case _ =>
      throw ActorInitializationException(
        "Resizable router actor can only be used when resizer is defined, not in " + context.getClass)
  }

  override def receive =
    ({
      case Resize =>
        resizerCell.resize(initial = false)
    }: Actor.Receive).orElse(super.receive)

}
