/*
 * Copyright (C) 2014-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed

import java.util.concurrent.TimeoutException

import scala.jdk.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.util.{ Failure, Success, Try }

import akka.actor.typed.{ ActorRef, Behavior, Props, RecipientRef }
import akka.annotation.{ DoNotInherit, InternalApi }

/**
 * All tracked effects for the [[akka.actor.testkit.typed.scaladsl.BehaviorTestKit]] and
 * [[akka.actor.testkit.typed.javadsl.BehaviorTestKit]] must extend this type.
 *
 * Factories/types for effects are available through [[akka.actor.testkit.typed.javadsl.Effects]]
 * and [[akka.actor.testkit.typed.javadsl.Effects]]
 *
 * Not for user extension
 */
@DoNotInherit
abstract class Effect private[akka] ()

object Effect {

  /**
   * The behavior initiated an ask via its context.  A response or timeout may be sent via this
   * effect to the asking behavior: this effect enforces that at most one response or timeout is
   * sent.  Alternatively, one may, after obtaining the effect, test the response adaptation function
   * (without sending a message to the asking behavior) arbitrarily many times via the 'adaptResponse`
   * and `adaptTimeout` methods.
   *
   * The 'replyToRef' is exposed so that the target inbox can expect the actual message sent to
   * initiate the ask.
   *
   * Note that this requires the ask to be initiated via the [[ActorContext]].  The [[Future]] returning
   * ask is not testable in the [[BehaviorTestKit]].
   */
  final case class AskInitiated[Req, Res, T](
      target: RecipientRef[Req],
      responseTimeout: FiniteDuration,
      responseClass: Class[Res])(val askMessage: Req, forwardResponse: Try[Res] => Unit, mapResponse: Try[Res] => T)
      extends Effect {
    def respondWith(response: Res): Unit = sendResponse(Success(response))

    def timeout(): Unit = sendResponse(timeoutTry(timeoutMsg))

    def adaptResponse(response: Res): T = mapResponse(Success(response))
    def adaptTimeout(msg: String): T = mapResponse(timeoutTry(msg))
    def adaptTimeout: T = adaptTimeout(timeoutMsg)

    /**
     * Java API
     */
    def getResponseTimeout: java.time.Duration = responseTimeout.toJava

    private var sentResponse: Boolean = false

    private def timeoutTry(msg: String): Try[Res] = Failure(new TimeoutException(msg))

    private def timeoutMsg: String =
      s"Ask timed out on [$target] after [${responseTimeout.toMillis} ms]. " +
      s"Message of type [${askMessage.getClass.getName}]." +
      " A typical reason for `AskTimeoutException` is that the recipient actor didn't send a reply."

    private def sendResponse(t: Try[Res]): Unit = synchronized {
      if (sentResponse) {
        throw new IllegalStateException("Can only complete the ask once")
      }

      sentResponse = true

      if (forwardResponse != null) {
        forwardResponse(t)
      } else throw new IllegalStateException("Can only complete and ask from a BehaviorTestKit-emitted effect")
    }
  }

  /**
   * The behavior spawned a named child with the given behavior (and optionally specific props)
   */
  final class Spawned[T](val behavior: Behavior[T], val childName: String, val props: Props, val ref: ActorRef[T])
      extends Effect
      with Product3[Behavior[T], String, Props]
      with Serializable {

    override def equals(other: Any) = other match {
      case o: Spawned[_] =>
        this.behavior == o.behavior &&
        this.childName == o.childName &&
        this.props == o.props
      case _ => false
    }
    override def hashCode: Int = (behavior.## * 31 + childName.##) * 31 + props.##
    override def toString: String = s"Spawned($behavior, $childName, $props)"

    override def productPrefix = "Spawned"
    override def _1: Behavior[T] = behavior
    override def _2: String = childName
    override def _3: Props = props
    override def canEqual(o: Any) = o.isInstanceOf[Spawned[_]]
  }

  object Spawned {
    def apply[T](behavior: Behavior[T], childName: String, props: Props = Props.empty): Spawned[T] =
      new Spawned(behavior, childName, props, null)
    def unapply[T](s: Spawned[T]): Option[(Behavior[T], String, Props)] = Some((s.behavior, s.childName, s.props))
  }

  /**
   * The behavior spawned an anonymous child with the given behavior (and optionally specific props)
   */
  final class SpawnedAnonymous[T](val behavior: Behavior[T], val props: Props, val ref: ActorRef[T])
      extends Effect
      with Product2[Behavior[T], Props]
      with Serializable {

    override def equals(other: Any) = other match {
      case o: SpawnedAnonymous[_] => this.behavior == o.behavior && this.props == o.props
      case _                      => false
    }
    override def hashCode: Int = behavior.## * 31 + props.##
    override def toString: String = s"SpawnedAnonymous($behavior, $props)"

    override def productPrefix = "SpawnedAnonymous"
    override def _1: Behavior[T] = behavior
    override def _2: Props = props
    override def canEqual(o: Any) = o.isInstanceOf[SpawnedAnonymous[_]]
  }

  object SpawnedAnonymous {
    def apply[T](behavior: Behavior[T], props: Props = Props.empty): SpawnedAnonymous[T] =
      new SpawnedAnonymous(behavior, props, null)
    def unapply[T](s: SpawnedAnonymous[T]): Option[(Behavior[T], Props)] = Some((s.behavior, s.props))
  }

  /**
   * INTERNAL API
   * Spawning adapters is private[akka]
   */
  @InternalApi
  private[akka] final class SpawnedAdapter[T](val name: String, val ref: ActorRef[T])
      extends Effect
      with Product1[String]
      with Serializable {

    override def equals(other: Any) = other match {
      case o: SpawnedAdapter[_] => this.name == o.name
      case _                    => false
    }
    override def hashCode: Int = name.##
    override def toString: String = s"SpawnedAdapter($name)"

    override def productPrefix = "SpawnedAdapter"
    override def _1: String = name
    override def canEqual(o: Any) = o.isInstanceOf[SpawnedAdapter[_]]
  }

  /**
   * INTERNAL API
   * Spawning adapters is private[akka]
   */
  @InternalApi
  private[akka] object SpawnedAdapter {
    def apply[T](name: String): SpawnedAdapter[T] = new SpawnedAdapter(name, null)
    def unapply[T](s: SpawnedAdapter[T]): Option[Tuple1[String]] = Some(Tuple1(s.name))
  }

  /**
   * INTERNAL API
   * The behavior spawned an anonymous adapter, through `context.spawnMessageAdapter`
   */
  @InternalApi
  private[akka] final class SpawnedAnonymousAdapter[T](val ref: ActorRef[T])
      extends Effect
      with Product
      with Serializable {

    override def equals(other: Any): Boolean = other match {
      case _: SpawnedAnonymousAdapter[_] => true
      case _                             => false
    }
    override def hashCode: Int = Nil.##
    override def toString: String = "SpawnedAnonymousAdapter"

    override def productPrefix: String = "SpawnedAnonymousAdapter"
    override def productIterator: Iterator[_] = Iterator.empty
    override def productArity: Int = 0
    override def productElement(n: Int) = throw new NoSuchElementException
    override def canEqual(o: Any): Boolean = o.isInstanceOf[SpawnedAnonymousAdapter[_]]
  }

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] object SpawnedAnonymousAdapter {
    def apply[T]() = new SpawnedAnonymousAdapter[T](null)
    def unapply[T](s: SpawnedAnonymousAdapter[T]): Boolean = true
  }

  /**
   * The behavior create a message adapter for the messages of type clazz
   */
  final case class MessageAdapter[A, T](messageClass: Class[A], adapt: A => T) extends Effect {

    /**
     * JAVA API
     */
    def adaptFunction: java.util.function.Function[A, T] = adapt.asJava
  }

  /**
   * The behavior stopped `childName`
   */
  final case class Stopped(childName: String) extends Effect

  /**
   * The behavior started watching `other`, through `context.watch(other)`
   */
  final case class Watched[T](other: ActorRef[T]) extends Effect

  /**
   * The behavior started watching `other`, through `context.watchWith(other, message)`
   */
  final case class WatchedWith[U, T](other: ActorRef[U], message: T) extends Effect

  /**
   * The behavior stopped watching `other`, through `context.unwatch(other)`
   */
  final case class Unwatched[T](other: ActorRef[T]) extends Effect

  /**
   * The behavior set a new receive timeout, with `message` as timeout notification
   */
  final case class ReceiveTimeoutSet[T](d: FiniteDuration, message: T) extends Effect {

    /**
     * Java API
     */
    def duration(): java.time.Duration = d.toJava
  }

  case object ReceiveTimeoutCancelled extends ReceiveTimeoutCancelled

  sealed abstract class ReceiveTimeoutCancelled extends Effect

  /**
   * The behavior used `context.scheduleOnce` to schedule `message` to be sent to `target` after `delay`
   * FIXME what about events scheduled through the scheduler?
   */
  final case class Scheduled[U](delay: FiniteDuration, target: ActorRef[U], message: U) extends Effect {
    def duration(): java.time.Duration = delay.toJava
  }

  final case class TimerScheduled[U](
      key: Any,
      msg: U,
      delay: FiniteDuration,
      mode: TimerScheduled.TimerMode,
      overriding: Boolean)(val send: () => Unit)
      extends Effect {
    def duration(): java.time.Duration = delay.toJava
  }

  object TimerScheduled {
    import scala.jdk.DurationConverters._

    sealed trait TimerMode
    case object FixedRateMode extends TimerMode
    case class FixedRateModeWithInitialDelay(initialDelay: FiniteDuration) extends TimerMode
    case object FixedDelayMode extends TimerMode
    case class FixedDelayModeWithInitialDelay(initialDelay: FiniteDuration) extends TimerMode
    case object SingleMode extends TimerMode

    /*Java API*/
    def fixedRateMode = FixedRateMode
    def fixedRateMode(initialDelay: java.time.Duration) = FixedRateModeWithInitialDelay(initialDelay.toScala)
    def fixedDelayMode = FixedDelayMode
    def fixedDelayMode(initialDelay: java.time.Duration) = FixedDelayModeWithInitialDelay(initialDelay.toScala)
    def singleMode = SingleMode
  }

  /*Java API*/
  def timerScheduled = TimerScheduled

  final case class TimerCancelled(key: Any) extends Effect

  /**
   * Used to represent an empty list of effects - in other words, the behavior didn't do anything observable
   */
  case object NoEffects extends NoEffects

  /**
   * Used for NoEffects expectations by type
   */
  sealed abstract class NoEffects extends Effect
}
