/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor

import java.lang.{ Iterable => JIterable }
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters._
import scala.language.implicitConversions
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.Logging.{ Error, LogEvent, LogLevel }
import akka.japi.Util.immutableSeq

/**
 * INTERNAL API
 */
@InternalApi
private[akka] sealed trait ChildStats

/**
 * INTERNAL API
 */
@InternalApi
private[akka] case object ChildNameReserved extends ChildStats

/**
 * ChildRestartStats is the statistics kept by every parent Actor for every child Actor
 * and is used for SupervisorStrategies to know how to deal with problems that occur for the children.
 */
final case class ChildRestartStats(
    child: ActorRef,
    var maxNrOfRetriesCount: Int = 0,
    var restartTimeWindowStartNanos: Long = 0L)
    extends ChildStats {

  def uid: Int = child.path.uid

  //FIXME How about making ChildRestartStats immutable and then move these methods into the actual supervisor strategies?
  def requestRestartPermission(retriesWindow: (Option[Int], Option[Int])): Boolean =
    retriesWindow match {
      case (Some(retries), _) if retries < 1 => false
      case (Some(retries), None)             => maxNrOfRetriesCount += 1; maxNrOfRetriesCount <= retries
      case (x, Some(window))                 => retriesInWindowOkay(if (x.isDefined) x.get else 1, window)
      case (None, _)                         => true
    }

  private def retriesInWindowOkay(retries: Int, window: Int): Boolean = {
    /*
     * Simple window algorithm: window is kept open for a certain time
     * after a restart and if enough restarts happen during this time, it
     * denies. Otherwise window closes and the scheme starts over.
     */
    val retriesDone = maxNrOfRetriesCount + 1
    val now = System.nanoTime
    val windowStart =
      if (restartTimeWindowStartNanos == 0) {
        restartTimeWindowStartNanos = now
        now
      } else restartTimeWindowStartNanos
    val insideWindow = (now - windowStart) <= TimeUnit.MILLISECONDS.toNanos(window)
    if (insideWindow) {
      maxNrOfRetriesCount = retriesDone
      retriesDone <= retries
    } else {
      maxNrOfRetriesCount = 1
      restartTimeWindowStartNanos = now
      true
    }
  }
}

/**
 * Implement this interface in order to configure the supervisorStrategy for
 * the top-level guardian actor (`/user`). An instance of this class must be
 * instantiable using a no-arg constructor.
 */
trait SupervisorStrategyConfigurator {
  def create(): SupervisorStrategy
}

final class DefaultSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = SupervisorStrategy.defaultStrategy
}

final class StoppingSupervisorStrategy extends SupervisorStrategyConfigurator {
  override def create(): SupervisorStrategy = SupervisorStrategy.stoppingStrategy
}

trait SupervisorStrategyLowPriorityImplicits { this: SupervisorStrategy.type =>

  /**
   * Implicit conversion from `Seq` of Cause-Directive pairs to a `Decider`. See makeDecider(causeDirective).
   */
  implicit def seqCauseDirective2Decider(trapExit: Iterable[CauseDirective]): Decider = makeDecider(trapExit)
  // the above would clash with seqThrowable2Decider for empty lists
}

object SupervisorStrategy extends SupervisorStrategyLowPriorityImplicits {
  sealed trait Directive {

    /** INTERNAL API */
    @InternalApi
    private[akka] def logLevel: LogLevel
  }

  /**
   * Resumes message processing for the failed Actor
   */
  case object Resume extends Resume(Logging.WarningLevel)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] sealed class Resume(private[akka] val logLevel: LogLevel) extends Directive

  /**
   * Discards the old Actor instance and replaces it with a new,
   * then resumes message processing.
   */
  case object Restart extends Restart(Logging.ErrorLevel)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] sealed class Restart(private[akka] val logLevel: LogLevel) extends Directive

  /**
   * Stops the Actor
   */
  case object Stop extends Stop(Logging.ErrorLevel)

  @InternalApi
  private[akka] sealed class Stop(private[akka] val logLevel: LogLevel) extends Directive

  /**
   * Escalates the failure to the supervisor of the supervisor,
   * by rethrowing the cause of the failure, i.e. the supervisor fails with
   * the same exception as the child.
   */
  case object Escalate extends Directive {
    override private[akka] def logLevel = throw new IllegalStateException("Escalate is not logged")
  }

  /**
   * Java API: Returning this directive resumes message processing for the failed Actor
   */
  def resume = Resume // switch to return type `Directive` on next binary incompatible release

  /**
   * Returning this directive resumes message processing for the failed Actor.
   *
   * @param logLevel Log level which will be used to log the failure
   */
  def resume(logLevel: LogLevel): Directive = new Resume(logLevel)

  /**
   * Java API: Returning this directive discards the old Actor instance and replaces it with a new,
   * then resumes message processing.
   */
  def restart = Restart // switch to return type `Directive` on next binary incompatible release

  /**
   * Returning this directive discards the old Actor instance and replaces it with a new,
   * then resumes message processing.
   *
   * @param logLevel Log level which will be used to log the failure
   */
  def restart(logLevel: LogLevel): Directive = new Restart(logLevel)

  /**
   * Java API: Returning this directive stops the Actor
   */
  def stop = Stop // switch to return type `Directive` on next binary incompatible release

  /**
   * Returning this directive stops the Actor
   *
   * @param logLevel Log level which will be used to log the failure
   */
  def stop(logLevel: LogLevel): Directive = new Stop(logLevel)

  /**
   * Java API: Returning this directive escalates the failure to the supervisor of the supervisor,
   * by rethrowing the cause of the failure, i.e. the supervisor fails with
   * the same exception as the child.
   */
  def escalate = Escalate // switch to return type `Directive` on next binary incompatible release

  /**
   * When supervisorStrategy is not specified for an actor this
   * `Decider` is used by default in the supervisor strategy.
   * The child will be stopped when [[akka.actor.ActorInitializationException]],
   * [[akka.actor.ActorKilledException]], or [[akka.actor.DeathPactException]] is
   * thrown. It will be restarted for other `Exception` types.
   * The error is escalated if it's a `Throwable`, i.e. `Error`.
   */
  final val defaultDecider: Decider = {
    case _: ActorInitializationException => Stop
    case _: ActorKilledException         => Stop
    case _: DeathPactException           => Stop
    case _: Exception                    => Restart
  }

  /**
   * When supervisorStrategy is not specified for an actor this
   * is used by default. OneForOneStrategy with decider defined in
   * [[#defaultDecider]].
   */
  final val defaultStrategy: SupervisorStrategy = {
    OneForOneStrategy()(defaultDecider)
  }

  /**
   * This strategy resembles Erlang in that failing children are always
   * terminated (one-for-one).
   */
  final val stoppingStrategy: SupervisorStrategy = {
    def stoppingDecider: Decider = {
      case _: Exception => Stop
    }
    OneForOneStrategy()(stoppingDecider)
  }

  /**
   * Implicit conversion from `Seq` of Throwables to a `Decider`.
   * This maps the given Throwables to restarts, otherwise escalates.
   */
  implicit def seqThrowable2Decider(trapExit: immutable.Seq[Class[_ <: Throwable]]): Decider = makeDecider(trapExit)

  type Decider = PartialFunction[Throwable, Directive]
  type JDecider = akka.japi.Function[Throwable, Directive]
  type CauseDirective = (Class[_ <: Throwable], Directive)

  /**
   * Decider builder which just checks whether one of
   * the given Throwables matches the cause and restarts, otherwise escalates.
   */
  def makeDecider(trapExit: immutable.Seq[Class[_ <: Throwable]]): Decider = {
    case x => if (trapExit.exists(_.isInstance(x))) Restart else Escalate
  }

  /**
   * Decider builder which just checks whether one of
   * the given Throwables matches the cause and restarts, otherwise escalates.
   */
  def makeDecider(trapExit: JIterable[Class[_ <: Throwable]]): Decider = makeDecider(immutableSeq(trapExit))

  /**
   * Decider builder for Iterables of cause-directive pairs, e.g. a map obtained
   * from configuration; will sort the pairs so that the most specific type is
   * checked before all its subtypes, allowing carving out subtrees of the
   * Throwable hierarchy.
   */
  def makeDecider(flat: Iterable[CauseDirective]): Decider = {
    val directives = sort(flat)

    { case x => directives.collectFirst { case (c, d) if c.isInstance(x) => d }.getOrElse(Escalate) }
  }

  /**
   * Converts a Java Decider into a Scala Decider
   */
  def makeDecider(func: JDecider): Decider = { case x => func(x) }

  /**
   * Sort so that subtypes always precede their supertypes, but without
   * obeying any order between unrelated subtypes (insert sort).
   *
   * INTERNAL API
   */
  private[akka] def sort(in: Iterable[CauseDirective]): immutable.Seq[CauseDirective] =
    in.foldLeft(new ArrayBuffer[CauseDirective](in.size)) { (buf, ca) =>
        buf.indexWhere(_._1.isAssignableFrom(ca._1)) match {
          case -1 => buf.append(ca)
          case x  => buf.insert(x, ca)
        }
        buf
      }
      .to(immutable.IndexedSeq)

  private[akka] def withinTimeRangeOption(withinTimeRange: Duration): Option[Duration] =
    if (withinTimeRange.isFinite && withinTimeRange >= Duration.Zero) Some(withinTimeRange) else None

  private[akka] def maxNrOfRetriesOption(maxNrOfRetries: Int): Option[Int] =
    if (maxNrOfRetries < 0) None else Some(maxNrOfRetries)

  private[akka] val escalateDefault = (_: Any) => Escalate
}

/**
 * An Akka SupervisorStrategy is the policy to apply for crashing children.
 *
 * <b>IMPORTANT:</b>
 *
 * You should not normally need to create new subclasses, instead use the
 * existing [[akka.actor.OneForOneStrategy]] or [[akka.actor.AllForOneStrategy]],
 * but if you do, please read the docs of the methods below carefully, as
 * incorrect implementations may lead to “blocked” actor systems (i.e.
 * permanently suspended actors).
 */
abstract class SupervisorStrategy {

  import SupervisorStrategy._

  /**
   * Returns the Decider that is associated with this SupervisorStrategy.
   * The Decider is invoked by the default implementation of `handleFailure`
   * to obtain the Directive to be applied.
   */
  def decider: Decider

  /**
   * This method is called after the child has been removed from the set of children.
   * It does not need to do anything special. Exceptions thrown from this method
   * do NOT make the actor fail if this happens during termination.
   */
  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit

  /**
   * This method is called to act on the failure of a child: restart if the flag is true, stop otherwise.
   */
  def processFailure(
      context: ActorContext,
      restart: Boolean,
      child: ActorRef,
      cause: Throwable,
      stats: ChildRestartStats,
      children: Iterable[ChildRestartStats]): Unit

  /**
   * This is the main entry point: in case of a child’s failure, this method
   * must try to handle the failure by resuming, restarting or stopping the
   * child (and returning `true`), or it returns `false` to escalate the
   * failure, which will lead to this actor re-throwing the exception which
   * caused the failure. The exception will not be wrapped.
   *
   * This method calls [[akka.actor.SupervisorStrategy#logFailure]], which will
   * log the failure unless it is escalated. You can customize the logging by
   * setting [[akka.actor.SupervisorStrategy#loggingEnabled]] to `false` and
   * do the logging inside the `decider` or override the `logFailure` method.
   *
   * @param children is a lazy collection (a view)
   */
  def handleFailure(
      context: ActorContext,
      child: ActorRef,
      cause: Throwable,
      stats: ChildRestartStats,
      children: Iterable[ChildRestartStats]): Boolean = {
    val directive = decider.applyOrElse(cause, escalateDefault)
    directive match {
      case _: Resume =>
        logFailure(context, child, cause, directive)
        resumeChild(child, cause)
        true
      case _: Restart =>
        logFailure(context, child, cause, directive)
        processFailure(context, restart = true, child, cause, stats, children)
        true
      case _: Stop =>
        logFailure(context, child, cause, directive)
        processFailure(context, restart = false, child, cause, stats, children)
        true
      case Escalate =>
        logFailure(context, child, cause, directive)
        false
    }
  }

  /**
   * Logging of actor failures is done when this is `true`.
   */
  protected def loggingEnabled: Boolean = true

  /**
   * Default logging of actor failures when
   * [[akka.actor.SupervisorStrategy#loggingEnabled]] is `true`.
   * `Escalate` failures are not logged here, since they are supposed
   * to be handled at a level higher up in the hierarchy.
   * `Resume` failures are logged at `Warning` level.
   * `Stop` and `Restart` failures are logged at `Error` level.
   */
  def logFailure(context: ActorContext, child: ActorRef, cause: Throwable, decision: Directive): Unit =
    if (loggingEnabled) {
      val logMessage = cause match {
        case e: ActorInitializationException if e.getCause ne null =>
          e.getCause match {
            case ex: InvocationTargetException if ex.getCause ne null => ex.getCause.getMessage
            case ex                                                   => ex.getMessage
          }
        case e => e.getMessage
      }
      decision match {
        case Escalate => // don't log here
        case d =>
          if (d.logLevel == Logging.ErrorLevel)
            publish(context, Error(cause, child.path.toString, getClass, logMessage))
          else
            publish(context, LogEvent(d.logLevel, child.path.toString, getClass, logMessage))
      }
    }

  // logging is not the main purpose, and if it fails there’s nothing we can do
  private def publish(context: ActorContext, logEvent: LogEvent): Unit =
    try context.system.eventStream.publish(logEvent)
    catch { case NonFatal(_) => }

  /**
   * Resume the previously failed child: <b>do never apply this to a child which
   * is not the currently failing child</b>. Suspend/resume needs to be done in
   * matching pairs, otherwise actors will wake up too soon or never at all.
   */
  final def resumeChild(child: ActorRef, cause: Throwable): Unit =
    child.asInstanceOf[InternalActorRef].resume(causedByFailure = cause)

  /**
   * Restart the given child, possibly suspending it first.
   *
   * <b>IMPORTANT:</b>
   *
   * If the child is the currently failing one, it will already have been
   * suspended, hence `suspendFirst` must be false. If the child is not the
   * currently failing one, then it did not request this treatment and is
   * therefore not prepared to be resumed without prior suspend.
   */
  final def restartChild(child: ActorRef, cause: Throwable, suspendFirst: Boolean): Unit = {
    val c = child.asInstanceOf[InternalActorRef]
    if (suspendFirst) c.suspend()
    c.restart(cause)
  }

}

/**
 * Applies the fault handling `Directive` (Resume, Restart, Stop) specified in the `Decider`
 * to all children when one fails, as opposed to [[akka.actor.OneForOneStrategy]] that applies
 * it only to the child actor that failed.
 *
 * @param maxNrOfRetries the number of times a child actor is allowed to be restarted, negative value means no limit,
 *   if the limit is exceeded the child actor is stopped
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window
 * @param decider mapping from Throwable to [[akka.actor.SupervisorStrategy.Directive]], you can also use a
 *   [[scala.collection.immutable.Seq]] of Throwables which maps the given Throwables to restarts, otherwise escalates.
 * @param loggingEnabled the strategy logs the failure if this is enabled (true), by default it is enabled
 */
case class AllForOneStrategy(
    maxNrOfRetries: Int = -1,
    withinTimeRange: Duration = Duration.Inf,
    override val loggingEnabled: Boolean = true)(val decider: SupervisorStrategy.Decider)
    extends SupervisorStrategy {

  import SupervisorStrategy._

  /**
   * Java API
   */
  def this(
      maxNrOfRetries: Int,
      withinTimeRange: Duration,
      decider: SupervisorStrategy.JDecider,
      loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(
      maxNrOfRetries: Int,
      withinTimeRange: java.time.Duration,
      decider: SupervisorStrategy.JDecider,
      loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange.toScala, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange.toScala)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange.toScala)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API: compatible with lambda expressions
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange)(decider)

  /**
   * Java API: compatible with lambda expressions
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange.toScala)(decider)

  /**
   * Java API: compatible with lambda expressions
   */
  def this(loggingEnabled: Boolean, decider: SupervisorStrategy.Decider) =
    this(loggingEnabled = loggingEnabled)(decider)

  /**
   * Java API: compatible with lambda expressions
   */
  def this(decider: SupervisorStrategy.Decider) =
    this()(decider)

  /*
   *  this is a performance optimization to avoid re-allocating the pairs upon
   *  every call to requestRestartPermission, assuming that strategies are shared
   *  across actors and thus this field does not take up much space
   */
  private val retriesWindow =
    (maxNrOfRetriesOption(maxNrOfRetries), withinTimeRangeOption(withinTimeRange).map(_.toMillis.toInt))

  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()

  def processFailure(
      context: ActorContext,
      restart: Boolean,
      child: ActorRef,
      cause: Throwable,
      stats: ChildRestartStats,
      children: Iterable[ChildRestartStats]): Unit = {
    if (children.nonEmpty) {
      if (restart && children.forall(_.requestRestartPermission(retriesWindow)))
        children.foreach(crs => restartChild(crs.child, cause, suspendFirst = (crs.child != child)))
      else
        for (c <- children) context.stop(c.child)
    }
  }
}

/**
 * Applies the fault handling `Directive` (Resume, Restart, Stop) specified in the `Decider`
 * to the child actor that failed, as opposed to [[akka.actor.AllForOneStrategy]] that applies
 * it to all children.
 *
 * @param maxNrOfRetries the number of times a child actor is allowed to be restarted, negative value means no limit
 *  if the duration is infinite. If the limit is exceeded the child actor is stopped
 * @param withinTimeRange duration of the time window for maxNrOfRetries, Duration.Inf means no window
 * @param decider mapping from Throwable to [[akka.actor.SupervisorStrategy.Directive]], you can also use a
 *   [[scala.collection.immutable.Seq]] of Throwables which maps the given Throwables to restarts, otherwise escalates.
 * @param loggingEnabled the strategy logs the failure if this is enabled (true), by default it is enabled
 */
case class OneForOneStrategy(
    maxNrOfRetries: Int = -1,
    withinTimeRange: Duration = Duration.Inf,
    override val loggingEnabled: Boolean = true)(val decider: SupervisorStrategy.Decider)
    extends SupervisorStrategy {

  /**
   * Java API
   */
  def this(
      maxNrOfRetries: Int,
      withinTimeRange: Duration,
      decider: SupervisorStrategy.JDecider,
      loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   *  Java API
   */
  def this(
      maxNrOfRetries: Int,
      withinTimeRange: java.time.Duration,
      decider: SupervisorStrategy.JDecider,
      loggingEnabled: Boolean) =
    this(maxNrOfRetries, withinTimeRange.toScala, loggingEnabled)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, decider: SupervisorStrategy.JDecider) =
    this(maxNrOfRetries, withinTimeRange.toScala)(SupervisorStrategy.makeDecider(decider))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, trapExit: JIterable[Class[_ <: Throwable]]) =
    this(maxNrOfRetries, withinTimeRange.toScala)(SupervisorStrategy.makeDecider(trapExit))

  /**
   * Java API: compatible with lambda expressions
   */
  def this(maxNrOfRetries: Int, withinTimeRange: Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange)(decider)

  /**
   * Java API: compatible with lambda expressions
   */
  def this(maxNrOfRetries: Int, withinTimeRange: java.time.Duration, decider: SupervisorStrategy.Decider) =
    this(maxNrOfRetries = maxNrOfRetries, withinTimeRange = withinTimeRange.toScala)(decider)

  def this(loggingEnabled: Boolean, decider: SupervisorStrategy.Decider) =
    this(loggingEnabled = loggingEnabled)(decider)

  /**
   * Java API: Restart an infinite number of times. Compatible with lambda expressions.
   */
  def this(decider: SupervisorStrategy.Decider) =
    this()(decider)

  def withMaxNrOfRetries(maxNrOfRetries: Int): OneForOneStrategy = copy(maxNrOfRetries = maxNrOfRetries)(decider)

  /*
   *  this is a performance optimization to avoid re-allocating the pairs upon
   *  every call to requestRestartPermission, assuming that strategies are shared
   *  across actors and thus this field does not take up much space
   */
  private val retriesWindow = (
    SupervisorStrategy.maxNrOfRetriesOption(maxNrOfRetries),
    SupervisorStrategy.withinTimeRangeOption(withinTimeRange).map(_.toMillis.toInt))

  def handleChildTerminated(context: ActorContext, child: ActorRef, children: Iterable[ActorRef]): Unit = ()

  def processFailure(
      context: ActorContext,
      restart: Boolean,
      child: ActorRef,
      cause: Throwable,
      stats: ChildRestartStats,
      children: Iterable[ChildRestartStats]): Unit = {
    if (restart && stats.requestRestartPermission(retriesWindow))
      restartChild(child, cause, suspendFirst = false)
    else
      context.stop(child) //TODO optimization to drop child here already?
  }
}
